import FluentSQL
import DuckDBKit
@preconcurrency import DuckDBNIO
import NIOCore
import SQLKit
import FluentKit

struct _FluentDuckDBDatabase {
    let database: DuckDBDatabase
    let context: DatabaseContext
    let inTransaction: Bool
}

extension _FluentDuckDBDatabase: Database {
    func execute(
        query: DatabaseQuery,
        onOutput: @escaping (DatabaseOutput) -> ()
    ) -> EventLoopFuture<Void> {
        var sql = SQLQueryConverter(delegate: DuckDBConverterDelegate()).convert(query)
        
        if case .create = query.action, query.customIDKey != .some(.string("")) {
            sql = DuckDBReturningID(base: sql, idKey: query.customIDKey ?? .id)
        }
        
        let (string, binds) = self.serialize(sql)
        let data: [DuckDBData]
        do {
            data = try binds.map { encodable in
                try DuckDBDataEncoder().encode(encodable)
            }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
        return self.database.withConnection { connection in
            connection.logging(to: self.logger)
                .query(string, data) { row in
                    onOutput(row)
                }
                .flatMap { self.eventLoop.makeSucceededFuture(()) }
        }
    }

    func transaction<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        guard !self.inTransaction else {
            return closure(self)
        }
        return self.database.withConnection { conn in
            conn.query("BEGIN TRANSACTION").flatMap { _ in
                let db = _FluentDuckDBDatabase(
                    database: conn,
                    context: self.context,
                    inTransaction: true
                )
                return closure(db).flatMap { result in
                    conn.query("COMMIT TRANSACTION").map { _ in
                        result
                    }
                }.flatMapError { error in
                    conn.query("ROLLBACK TRANSACTION").flatMapThrowing { _ in
                        throw error
                    }
                }
            }
        }
    }
    
    func execute(schema: DatabaseSchema) -> EventLoopFuture<Void> {
        var schema = schema
        switch schema.action {
        case .update:
            // Remove enum updates as they are unnecessary.
            schema.updateFields = schema.updateFields.filter({
                switch $0 {
                case .custom:
                    return true
                case .dataType(_, let dataType):
                    switch dataType {
                    case .enum:
                        return false
                    default:
                        return true
                    }
                }
            })
            guard 
                schema.createConstraints.isEmpty,
                schema.updateFields.isEmpty, 
                schema.deleteFields.isEmpty,
                schema.deleteConstraints.isEmpty
            else {
                return self.eventLoop.makeFailedFuture(FluentDuckDBError.unsupportedAlter)
            }

            // If only enum updates, then skip.
            if schema.createFields.isEmpty {
                return self.eventLoop.makeSucceededFuture(())
            }
        default:
            break
        }
        let sql = SQLSchemaConverter(delegate: DuckDBConverterDelegate()).convert(schema)
        let (string, binds) = self.serialize(sql)
        let data: [DuckDBData]
        do {
            data = try binds.map { encodable in
                try DuckDBDataEncoder().encode(encodable)
            }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
        return self.database.logging(to: self.logger).query(string, data) {
            fatalError("Unexpected output: \($0)")
        }
    }

    func execute(enum: DatabaseEnum) -> EventLoopFuture<Void> {
        return self.eventLoop.makeSucceededFuture(())
    }
    
    func withConnection<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        self.database.withConnection {
            closure(_FluentDuckDBDatabase(database: $0, context: self.context, inTransaction: self.inTransaction))
        }
    }
}

private enum FluentDuckDBError: Error, CustomStringConvertible {
    case unsupportedAlter

    var description: String {
        switch self {
        case .unsupportedAlter:
            return "DuckDB only supports adding columns in ALTER TABLE statements."
        }
    }
}

extension _FluentDuckDBDatabase: SQLDatabase {
    var dialect: SQLDialect {
        DuckDBDialect()
    }
    
    func execute(
        sql query: SQLExpression,
        _ onRow: @escaping (SQLRow) -> ()
    ) -> EventLoopFuture<Void> {
        self.logging(to: self.logger).sql().execute(sql: query, onRow)
    }
}

extension _FluentDuckDBDatabase: DuckDBDatabase {
    func withConnection<T>(_ closure: @escaping (DuckDBConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        self.database.withConnection(closure)
    }
    
    func query(
        _ query: String,
        _ binds: [DuckDBData],
        logger: Logger,
        _ onRow: @escaping (DuckDBRow) -> Void
    ) -> EventLoopFuture<Void> {
        self.database.query(query, binds, logger: logger, onRow)
    }
}

protocol AutoincrementIDInitializable {
    init(autoincrementID: Int)
}

extension AutoincrementIDInitializable where Self: FixedWidthInteger {
    init(autoincrementID: Int) {
        self = numericCast(autoincrementID)
    }
}

extension Int: AutoincrementIDInitializable { }
extension UInt: AutoincrementIDInitializable { }
extension Int64: AutoincrementIDInitializable { }
extension UInt64: AutoincrementIDInitializable { }

fileprivate struct DuckDBReturningID: SQLExpression {
    let base: any SQLExpression
    let idKey: FieldKey

    func serialize(to serializer: inout SQLSerializer) {
        serializer.statement {
            $0.append(self.base)
            $0.append("RETURNING", SQLIdentifier(self.idKey.description))
        }
    }
}

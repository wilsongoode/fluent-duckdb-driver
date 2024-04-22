import SQLKit
@preconcurrency import DuckDBNIO
import Logging

extension DuckDBDatabase {
    public func sql() -> any SQLDatabase {
        _DuckDBSQLDatabase(database: self)
    }
}

private struct _DuckDBSQLDatabase: SQLDatabase {
    let database: any DuckDBDatabase
    
    var eventLoop: any EventLoop {
        self.database.eventLoop
    }
    
    var logger: Logger {
        self.database.logger
    }
    
    var dialect: any SQLDialect {
        DuckDBDialect()
    }
    
    func execute(
        sql query: any SQLExpression,
        _ onRow: @escaping (any SQLRow) -> ()
    ) -> EventLoopFuture<Void> {
        var serializer = SQLSerializer(database: self)
        query.serialize(to: &serializer)
        let binds: [DuckDBData]
        do {
            binds = try serializer.binds.map { encodable in
                try DuckDBDataEncoder().encode(encodable)
            }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
        
        // This temporary silliness silences a Sendable capture warning whose correct resolution
        // requires updating SQLKit itself to be fully Sendable-compliant.
        @Sendable func onRowWorkaround(_ row: any SQLRow) {
            onRow(row)
        }
        return self.database.query(serializer.sql, binds, logger: self.logger, onRowWorkaround)
    }
}


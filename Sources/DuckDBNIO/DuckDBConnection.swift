import Foundation
import NIOCore
import NIOPosix
import Logging
@preconcurrency import DuckDB

public protocol DuckDBDatabase {
    var logger: Logger { get }
    var eventLoop: any EventLoop { get }
    
    @preconcurrency func query(
        _ query: String,
        _ binds: [DuckDBData],
        logger: Logger,
        _ onRow: @escaping @Sendable (DuckDBRow) -> Void
    ) -> EventLoopFuture<Void>
    
    @preconcurrency func withConnection<T>(
        _: @escaping @Sendable (DuckDBConnection) -> EventLoopFuture<T>
    ) -> EventLoopFuture<T>
}

extension DuckDBDatabase {
    @preconcurrency
    public func query(
        _ query: String,
        _ binds: [DuckDBData] = [],
        _ onRow: @escaping @Sendable (DuckDBRow) -> Void
    ) -> EventLoopFuture<Void> {
        self.query(query, binds, logger: self.logger, onRow)
    }
    
    public func query(
        _ query: String,
        _ binds: [DuckDBData] = []
    ) -> EventLoopFuture<[DuckDBRow]> {
        let rows: UnsafeMutableTransferBox<[DuckDBRow]> = .init([])
        return self.query(query, binds, logger: self.logger) { row in
            rows.wrappedValue.append(row)
        }.map { rows.wrappedValue }
    }
  }

extension DuckDBDatabase {
    public func logging(to logger: Logger) -> any DuckDBDatabase {
        _DuckDBDatabaseCustomLogger(database: self, logger: logger)
    }
}

private struct _DuckDBDatabaseCustomLogger: DuckDBDatabase {
    let database: any DuckDBDatabase
    var eventLoop: any EventLoop {
        self.database.eventLoop
    }
    let logger: Logger
    
    @preconcurrency func withConnection<T>(
        _ closure: @escaping @Sendable (DuckDBConnection) -> EventLoopFuture<T>
    ) -> EventLoopFuture<T> {
        self.database.withConnection(closure)
    }
    
    @preconcurrency func query(
        _ query: String,
        _ binds: [DuckDBData],
        logger: Logger,
        _ onRow: @escaping @Sendable (DuckDBRow) -> Void
    ) -> EventLoopFuture<Void> {
        self.database.query(query, binds, logger: logger, onRow)
    }
}

internal final class DuckDBConnectionHandle: @unchecked Sendable {
    var raw: OpaquePointer?
    
    init(_ raw: OpaquePointer?) {
        self.raw = raw
    }
}

public final class DuckDBConnection: DuckDBDatabase {
    public enum DuckDBConnectionError: Error {
        case invalidUUID(String)
    }
    public typealias Store = DuckDB.Database.Store
    
    public let eventLoop: any EventLoop
    
    internal let handle: DuckDB.Connection
    internal let threadPool: NIOThreadPool
    public let logger: Logger
    
    public var isClosed: Bool { false }
    
    public static func open(
        store: Store = .inMemory,
        configuration: DuckDB.Database.Configuration? = nil,
        logger: Logger = .init(label: "codes.vapor.DuckDB")
    ) -> EventLoopFuture<DuckDBConnection> {
        Self.open(
            store: store,
            configuration: configuration,
            threadPool: NIOThreadPool.singleton,
            logger: logger,
            on: MultiThreadedEventLoopGroup.singleton.any()
        )
    }
    
    public static func open(
        store: Store = .inMemory,
        configuration: DuckDB.Database.Configuration? = nil,
        threadPool: NIOThreadPool,
        logger: Logger = .init(label: "codes.vapor.DuckDB"),
        on eventLoop: any EventLoop
    ) -> EventLoopFuture<DuckDBConnection> {
        threadPool.runIfActive(eventLoop: eventLoop) {
            let connection = DuckDBConnection(
                handle: try DuckDB.Database(store: store, configuration: configuration).connect(),
                threadPool: threadPool,
                logger: logger,
                on: eventLoop
            )
            logger.debug("Connected to DuckDB db: \(store)")
            return connection
        }
    }
    
    init(
        handle: DuckDB.Connection,
        threadPool: NIOThreadPool,
        logger: Logger,
        on eventLoop: any EventLoop
    ) {
        self.handle = handle
        self.threadPool = threadPool
        self.logger = logger
        self.eventLoop = eventLoop
    }
    
//    public func lastAutoincrementID() -> EventLoopFuture<Int> {
//        self.threadPool.runIfActive(eventLoop: self.eventLoop) {
//            let rowid = sqlite_nio_sqlite3_last_insert_rowid(self.handle.raw)
//            return numericCast(rowid)
//        }
//    }

    @preconcurrency public func withConnection<T>(
        _ closure: @escaping @Sendable (DuckDBConnection) -> EventLoopFuture<T>
    ) -> EventLoopFuture<T> {
        closure(self)
    }
    
    @preconcurrency public func query(
        _ query: String,
        _ binds: [DuckDBData],
        logger: Logger,
        _ onRow: @escaping @Sendable (DuckDBRow) -> Void
    ) -> EventLoopFuture<Void> {
        logger.debug("\(query) \(binds)")
        let promise = self.eventLoop.makePromise(of: Void.self)
        self.threadPool.submit {
            guard case $0 = NIOThreadPool.WorkItemState.active else {
                // Note: We should be throwing NIOThreadPoolError.ThreadPoolInactive here, but we can't
                // 'cause its initializer isn't public so we let `DuckDB_MISUSE` get the point across.
                return promise.fail(DuckDBNIOError.threadPullIsInactive)
            }
            var futures: [EventLoopFuture<Void>] = []
            do {
                let statement = try DuckDB.PreparedStatement(connection: self.handle, query: query)
                
                for (index, value) in binds.enumerated() {
                    try value.bind(to: statement, at: index + 1)
                }
                
                let resultSet = try statement.execute()
                
                let columnOffsets = resultSet.enumerated().map {($1.name, $0)}
                
                var position: DuckDB.DBInt = 0
                var end = false
                while !end {
                    var row: [DuckDBData] = []
                    row.reserveCapacity(Int(resultSet.columnCount))
                    
                    for i in resultSet.indices {
                        guard position < resultSet.column(at: DBInt(i)).endIndex else {
                            end = true
                            break
                        }
                        let duckDBData: DuckDBData = switch resultSet.column(at: DBInt(i)).underlyingDatabaseType {
                        case .boolean:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Bool.self)[position] { .bool(value) } else { .null }
                        case .tinyint:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Int8.self)[position] { .int8(value) } else { .null }
                        case .smallint:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Int16.self)[position] { .int16(value) } else { .null }
                        case .integer:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Int32.self)[position] { .int32(value) } else { .null }
                        case .bigint:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Int64.self)[position] { .int64(value) } else { .null }
                        case .hugeint:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: DuckDB.IntHuge.self)[position] { .intHuge(value) } else { .null }
                        case .uhugeint:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: DuckDB.UIntHuge.self)[position] { .uintHuge(value) } else { .null }
                        case .utinyint:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: UInt8.self)[position] { .uint8(value) } else { .null }
                        case .usmallint:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: UInt16.self)[position] { .uint16(value) } else { .null }
                        case .uinteger:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: UInt32.self)[position] { .uint32(value) } else { .null }
                        case .ubigint:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: UInt64.self)[position] { .uint64(value) } else { .null }
                        case .float:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Float.self)[position] { .float(value) } else { .null }
                        case .double:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Double.self)[position] { .double(value) } else { .null }
                        case .timestamp:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Timestamp.self)[position] { .timestamp(value) } else { .null }
                        case .timestampTz:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Timestamp.self)[position] { .timestamp(value) } else { .null }
                        case .date:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Date.self)[position] { .date(value) } else { .null }
                        case .time:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Time.self)[position] { .time(value) } else { .null }
                        case .timeTz:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Time.self)[position] { .time(value) } else { .null }
                        case .interval:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Interval.self)[position] { .interval(value) } else { .null }
                        case .varchar:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: String.self)[position] { .string(value) } else { .null }
                        case .blob:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Data.self)[position] { .data(value) } else { .null }
                        case .decimal:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Decimal.self)[position] { .decimal(value) } else { .null }
                        case .timestampS:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Timestamp.self)[position] { .timestamp(value) } else { .null }
                        case .timestampMS:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Timestamp.self)[position] { .timestamp(value) } else { .null }
                        case .timestampNS:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: Timestamp.self)[position] { .timestamp(value) } else { .null }
                        case .`enum`:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: String.self)[position] { .string(value) } else { .null }
                        case .list:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: String.self)[position] { .string(value) } else { .null }
                        case .`struct`:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: String.self)[position] { .string(value) } else { .null }
                        case .map:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: String.self)[position] { .string(value) } else { .null }
                        case .union:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: String.self)[position] { .string(value) } else { .null }
                        case .uuid:
                            if let value = resultSet.column(at: DBInt(i)).cast(to: UUID.self)[position] { .uuid(value) } else { .null }
                        default:
                            .null
                        }

                        row.append(duckDBData)
                    }
                    
                    position += 1
                    
                    guard !end else {
                        break
                    }
                    
                    let copy = row
                    futures.append(promise.futureResult.eventLoop.submit { onRow(DuckDBRow(columnOffsets: .init(offsets: columnOffsets), data: copy)) })
                }
            } catch {
                return promise.fail(error) // EventLoopPromise.fail(_:), conveniently, returns Void
            }
            EventLoopFuture.andAllSucceed(futures, promise: promise)
        }
        return promise.futureResult
    }

    public func close() -> EventLoopFuture<Void> {
        self.threadPool.runIfActive(eventLoop: self.eventLoop) {
//            self.handle.raw = nil
        }
    }
}

fileprivate final class UnsafeMutableTransferBox<Wrapped: Sendable>: @unchecked Sendable {
    var wrappedValue: Wrapped
    init(_ wrappedValue: Wrapped) { self.wrappedValue = wrappedValue }
}

extension DuckDBConnection: Sendable {}

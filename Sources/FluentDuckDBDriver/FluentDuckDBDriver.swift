import NIOCore
import FluentKit
@preconcurrency import AsyncKit
import Logging
import DuckDBKit
@preconcurrency import DuckDBNIO

struct _FluentDuckDBDriver: DatabaseDriver {
    let pool: EventLoopGroupConnectionPool<DuckDBConnectionSource>

    var eventLoopGroup: EventLoopGroup {
        self.pool.eventLoopGroup
    }

    func makeDatabase(with context: DatabaseContext) -> Database {
        _FluentDuckDBDatabase(
            database: _ConnectionPoolDuckDBDatabase(pool: self.pool.pool(for: context.eventLoop), logger: context.logger),
            context: context,
            inTransaction: false
        )
    }

    func shutdown() {
        self.pool.shutdown()
    }
}

struct _ConnectionPoolDuckDBDatabase {
    let pool: EventLoopConnectionPool<DuckDBConnectionSource>
    let logger: Logger
}

extension _ConnectionPoolDuckDBDatabase: DuckDBDatabase {
    var eventLoop: EventLoop {
        self.pool.eventLoop
    }

    func withConnection<T>(_ closure: @escaping (DuckDBConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        self.pool.withConnection {
            closure($0)
        }
    }

    func query(_ query: String, _ binds: [DuckDBData], logger: Logger, _ onRow: @escaping (DuckDBRow) -> Void) -> EventLoopFuture<Void> {
        self.withConnection {
            $0.query(query, binds, logger: logger, onRow)
        }
    }
}

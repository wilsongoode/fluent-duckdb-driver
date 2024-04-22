import Foundation
import NIO
import DuckDBKit
import FluentKit
import AsyncKit
@preconcurrency import DuckDB

public typealias DuckDBConfiguration = DuckDBKit.DuckDBConfiguration

extension DatabaseConfigurationFactory {
    public static func duckdb(
        configuration: DuckDBConfiguration = .init(store: .inMemory),
        maxConnectionsPerEventLoop: Int = 1,
        connectionPoolTimeout: NIO.TimeAmount = .seconds(10)
    ) -> Self {
        return .init {
            FluentDuckDBConfiguration(
                configuration: configuration,
                maxConnectionsPerEventLoop: maxConnectionsPerEventLoop,
                middleware: [],
                connectionPoolTimeout: connectionPoolTimeout
            )
        }
    }
}

struct FluentDuckDBConfiguration: DatabaseConfiguration {
    let configuration: DuckDBConfiguration
    let maxConnectionsPerEventLoop: Int
    var middleware: [AnyModelMiddleware]
    let connectionPoolTimeout: NIO.TimeAmount

    func makeDriver(for databases: Databases) -> DatabaseDriver {
        let db = DuckDBConnectionSource(
            configuration: configuration,
            threadPool: databases.threadPool
        )
        let pool = EventLoopGroupConnectionPool(
            source: db,
            maxConnectionsPerEventLoop: maxConnectionsPerEventLoop,
            requestTimeout: connectionPoolTimeout,
            on: databases.eventLoopGroup
        )
        return _FluentDuckDBDriver(pool: pool)
    }
}

extension DuckDBConfiguration {
    public static func file(at url: URL) -> Self {
        .init(store: .file(at: url))
    }

    public static var inMemory: Self {
        .init(store: .inMemory)
    }
}

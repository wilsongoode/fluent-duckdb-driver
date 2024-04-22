import Foundation
import Logging
import AsyncKit
import NIOPosix
import NIOCore
@preconcurrency import DuckDBNIO
@preconcurrency import DuckDB

public struct DuckDBConnectionSource: ConnectionPoolSource, Sendable {
    public typealias Connection = DuckDBConnection
    
    private let configuration: DuckDBConfiguration
    private let threadPool: NIOThreadPool

    public init(
        configuration: DuckDBConfiguration,
        threadPool: NIOThreadPool
    ) {
        self.configuration = configuration
        self.threadPool = threadPool
    }

    public func makeConnection(
        logger: Logger,
        on eventLoop: any EventLoop
    ) -> EventLoopFuture<DuckDBConnection> {
        return DuckDBConnection.open(
            store: self.configuration.store,
            threadPool: self.threadPool,
            logger: logger,
            on: eventLoop
        ).flatMap { conn in
            eventLoop.makeSucceededFuture(conn)
        }
    }
}

extension DuckDBConnection: ConnectionPoolItem { }

fileprivate extension String {
    var asSafeFilename: String {
#if os(Windows)
        self.replacingOccurrences(of: ":", with: "_").replacingOccurrences(of: "\\", with: "-")
#else
        self.replacingOccurrences(of: "/", with: "-")
#endif
    }
}

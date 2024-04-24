import struct Foundation.UUID
@preconcurrency import DuckDB
import DuckDBNIO

public struct DuckDBConfiguration: Sendable {
    public typealias Configuration = DuckDBNIO.SQLDuckDBConfiguration
    
    public let store: DuckDB.Database.Store
    public let configuration: Configuration?

    public init(store: DuckDB.Database.Store = .inMemory, configuration: Configuration? = nil) {
        self.store = store
        self.configuration = configuration
    }
}

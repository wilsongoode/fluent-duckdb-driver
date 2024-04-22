import struct Foundation.UUID
@preconcurrency import DuckDB

public struct DuckDBConfiguration: Sendable {
    public let store: DuckDB.Database.Store
    public let configuration: DuckDB.Database.Configuration?

    public init(store: DuckDB.Database.Store = .inMemory, configuration: DuckDB.Database.Configuration? = nil) {
        self.store = store
        self.configuration = configuration
    }
    
}

extension DuckDB.Database.Configuration: ExpressibleByDictionaryLiteral {
    public convenience init(dictionaryLiteral elements: (String, String)...) {
        self.init()
        for (name, value) in elements {
            do {
                try self.setValue(value, forKey: name)
            } catch {
                print("WARNING: Attempt to set DuckDB configuration parameter \"\(name)\" to value \"\(value)\" caused error: \(error)")
            }
        }
    }
}

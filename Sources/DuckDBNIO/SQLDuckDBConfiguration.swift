import DuckDB

public struct SQLDuckDBConfiguration: ExpressibleByDictionaryLiteral {
    internal let configuration: DuckDB.Database.Configuration
    public let options: [String: String]
    
    public init(dictionaryLiteral elements: (String, String)...) {
        let configuration = DuckDB.Database.Configuration()
        self.configuration = configuration
        self.options = .init(elements.compactMap { args in
            let (key, value) = args
            
            do {
                try configuration.setValue(value, forKey: key)
            } catch {
                print("WARNING: Attempt to set DuckDB configuration parameter \"\(key)\" to value \"\(value)\" caused error: \(error).")
                return nil
            }

            return (key, value)
        }) { $1 }
    }
}

extension SQLDuckDBConfiguration: Equatable {
    public static func == (lhs: SQLDuckDBConfiguration, rhs: SQLDuckDBConfiguration) -> Bool {
        lhs.options == rhs.options
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

@preconcurrency import DuckDB

public enum DuckDBNIOError: Error {
    case threadPullIsInactive
}

extension DuckDBNIOError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .threadPullIsInactive: "Thread pull is inactive"
        }
    }
}

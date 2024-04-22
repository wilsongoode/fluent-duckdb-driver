import FluentKit
@preconcurrency import DuckDB

extension DuckDB.DatabaseError: FluentKit.DatabaseError {
    public var isSyntaxError: Bool {
        switch self {
        case .connectionQueryError, .preparedStatementFailedToInitialize:
            return true
        default:
            return false
        }
    }

    public var isConnectionClosed: Bool {
        switch self {
        case .connectionFailedToInitialize:
            return true
        default:
            return false
        }
    }

    public var isConstraintFailure: Bool {
        switch self {
        default:
            return false
        }
    }
}

import FluentKit
@preconcurrency import DuckDB

extension DuckDB.DatabaseError: FluentKit.DatabaseError {
    public var isSyntaxError: Bool {
        switch self {
        case .connectionQueryError, .preparedStatementFailedToInitialize:
            true
        default:
            false
        }
    }

    public var isConnectionClosed: Bool {
        switch self {
        case .connectionFailedToInitialize:
            true
        default:
            false
        }
    }

    public var isConstraintFailure: Bool {
        switch self {
        case .preparedStatementQueryError(let reason):
            reason?.hasPrefix("Constraint Error:") == true
        default:
            false
        }
    }
}

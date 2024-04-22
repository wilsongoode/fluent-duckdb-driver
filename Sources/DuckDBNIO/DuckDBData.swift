import struct Foundation.UUID
import struct Foundation.Data
import struct Foundation.Decimal
import NIOCore
@preconcurrency import DuckDB

/// Supported DuckDB data types.
public enum DuckDBData: Equatable {
    case bool(Bool)
    case int(Int)
    case int8(Int8)
    case int16(Int16)
    case int32(Int32)
    case int64(Int64)
    case intHuge(DuckDB.IntHuge)
    case uintHuge(DuckDB.UIntHuge)
    case uint(UInt)
    case uint8(UInt8)
    case uint16(UInt16)
    case uint32(UInt32)
    case uint64(UInt64)
    case float(Float)
    case double(Double)
    case string(String)
    case uuid(UUID)
    case time(DuckDB.Time)
    case date(DuckDB.Date)
    case timestamp(DuckDB.Timestamp)
    case interval(DuckDB.Interval)
    case data(Data)
    case decimal(Decimal)
    case null
}

extension DuckDBData {
    public var integer: Int? {
        switch self {
        case .bool(let value):
            Int(value ? 1 : 0)
        case .int(let value):
            Int(value)
        case .int8(let value):
            Int(value)
        case .int16(let value):
            Int(value)
        case .int32(let value):
            Int(value)
        case .int64(let value):
            Int(value)
        case .intHuge(let value):
            Int(value)
        case .uintHuge(let value):
            Int(value)
        case .uint(let value):
            Int(value)
        case .uint8(let value):
            Int(value)
        case .uint16(let value):
            Int(value)
        case .uint32(let value):
            Int(value)
        case .uint64(let value):
            Int(value)
        case .float(let value):
            Int(value)
        case .double(let value):
            Int(value)
        case .string(let value):
            Int(value)
        case .decimal(let value):
            Int("\(value)")
        case .uuid, .time, .date, .timestamp, .interval, .data, .null:
            nil
        }
    }
    
    public var double: Double? {
        switch self {
        case .bool(let value):
            Double(value ? 1 : 0)
        case .int(let value):
            Double(value)
        case .int8(let value):
            Double(value)
        case .int16(let value):
            Double(value)
        case .int32(let value):
            Double(value)
        case .int64(let value):
            Double(value)
        case .intHuge(let value):
            Double(value)
        case .uintHuge(let value):
            Double(value)
        case .uint(let value):
            Double(value)
        case .uint8(let value):
            Double(value)
        case .uint16(let value):
            Double(value)
        case .uint32(let value):
            Double(value)
        case .uint64(let value):
            Double(value)
        case .float(let value):
            Double(value)
        case .double(let value):
            Double(value)
        case .string(let value):
            Double(value)
        case .decimal(let value):
            Double("\(value)")
        case .uuid, .time, .date, .timestamp, .interval, .data, .null:
            nil
        }
    }
    
    public var string: String? {
        switch self {
        case .bool(let value):
            String(value)
        case .int(let value):
            String(value)
        case .int8(let value):
            String(value)
        case .int16(let value):
            String(value)
        case .int32(let value):
            String(value)
        case .int64(let value):
            String(value)
        case .intHuge(let value):
            String(value)
        case .uintHuge(let value):
            String(value)
        case .uint(let value):
            String(value)
        case .uint8(let value):
            String(value)
        case .uint16(let value):
            String(value)
        case .uint32(let value):
            String(value)
        case .uint64(let value):
            String(value)
        case .float(let value):
            String(value)
        case .double(let value):
            String(value)
        case .string(let value):
            String(value)
        case .uuid(let value):
            value.uuidString
        case .time(let value):
            "\(value)"
        case .date(let value):
            "\(value)"
        case .timestamp(let value):
            "\(value)"
        case .interval(let value):
            "\(value)"
        case .data(let value):
            String(data: value, encoding: .utf8)
        case .decimal(let value):
            "\(value)"
        case .null:
            nil
        }
    }
    
    public var bool: Bool? {
        switch self {
        case .bool(let value):
            value
        case .int(let value):
            value != 0
        case .int8(let value):
            value != 0
        case .int16(let value):
            value != 0
        case .int32(let value):
            value != 0
        case .int64(let value):
            value != 0
        case .intHuge(let value):
            value != 0
        case .uintHuge(let value):
            value != 0
        case .uint(let value):
            value != 0
        case .uint8(let value):
            value != 0
        case .uint16(let value):
            value != 0
        case .uint32(let value):
            value != 0
        case .uint64(let value):
            value != 0
        case .float(let value):
            value != 0
        case .double(let value):
            value != 0
        case .string(let value):
            !value.isEmpty
        case .decimal(let value):
            !value.isZero
        case .uuid, .time, .date, .timestamp, .interval, .data, .null:
            nil
        }
    }
    
    public var blob: ByteBuffer? {
        switch self {
        case .data(let data):
            ByteBuffer(bytes: data)
        default:
            nil
        }
    }
    
    public var isNull: Bool {
        switch self {
        case .null:
            true
        default:
            false
        }
    }
    
    public func bind(to statement: DuckDB.PreparedStatement, at index: Int) throws {
        switch self {
        case .bool(let value):
            try statement.bind(value, at: index)
        case .int(let value):
            try statement.bind(Int64(value), at: index)
        case .int8(let value):
            try statement.bind(value, at: index)
        case .int16(let value):
            try statement.bind(value, at: index)
        case .int32(let value):
            try statement.bind(value, at: index)
        case .int64(let value):
            try statement.bind(value, at: index)
        case .intHuge(let value):
            try statement.bind(value, at: index)
        case .uintHuge(let value):
            try statement.bind(value, at: index)
        case .uint(let value):
            try statement.bind(UInt64(value), at: index)
        case .uint8(let value):
            try statement.bind(value, at: index)
        case .uint16(let value):
            try statement.bind(value, at: index)
        case .uint32(let value):
            try statement.bind(value, at: index)
        case .uint64(let value):
            try statement.bind(value, at: index)
        case .float(let value):
            try statement.bind(value, at: index)
        case .double(let value):
            try statement.bind(value, at: index)
        case .string(let value):
            try statement.bind(value, at: index)
        case .uuid(let value):
            try statement.bind(value.uuidString, at: index)
        case .time(let value):
            try statement.bind(value, at: index)
        case .date(let value):
            try statement.bind(value, at: index)
        case .timestamp(let value):
            try statement.bind(value, at: index)
        case .interval(let value):
            try statement.bind(value, at: index)
        case .data(let value):
            try statement.bind(value, at: index)
        case .decimal(let value):
            try statement.bind(value, at: index)
        case .null:
            try statement.bind(String?.none, at: index)
        }
    }
}

extension DuckDBData: CustomStringConvertible {
    public var description: String {
        switch self {
        case .bool(let value):
            value.description
        case .int(let value):
            value.description
        case .int8(let value):
            value.description
        case .int16(let value):
            value.description
        case .int32(let value):
            value.description
        case .int64(let value):
            value.description
        case .intHuge(let value):
            value.description
        case .uintHuge(let value):
            value.description
        case .uint(let value):
            value.description
        case .uint8(let value):
            value.description
        case .uint16(let value):
            value.description
        case .uint32(let value):
            value.description
        case .uint64(let value):
            value.description
        case .float(let value):
            value.description
        case .double(let value):
            value.description
        case .string(let value):
            value.description
        case .uuid(let value):
            value.description
        case .time(let value):
            "\(value)"
        case .date(let value):
            "\(value)"
        case .timestamp(let value):
            "\(value)"
        case .interval(let value):
            "\(value)"
        case .data(let value):
            "<\(value.count) bytes>"
        case .decimal(let value):
            value.description
        case .null:
            "null"
        }
    }
}

extension DuckDBData: Encodable {
    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .bool(let value):
            try container.encode(value)
        case .int(let value):
            try container.encode(value)
        case .int8(let value):
            try container.encode(value)
        case .int16(let value):
            try container.encode(value)
        case .int32(let value):
            try container.encode(value)
        case .int64(let value):
            try container.encode(value)
        case .intHuge(let value):
            try container.encode(Int64(value))
        case .uintHuge(let value):
            try container.encode(UInt64(value))
        case .uint(let value):
            try container.encode(value)
        case .uint8(let value):
            try container.encode(value)
        case .uint16(let value):
            try container.encode(value)
        case .uint32(let value):
            try container.encode(value)
        case .uint64(let value):
            try container.encode(value)
        case .float(let value):
            try container.encode(value)
        case .double(let value):
            try container.encode(value)
        case .string(let value):
            try container.encode(value)
        case .uuid(let value):
            try container.encode(value)
        case .time(let value):
            try container.encode("\(value)")
        case .date(let value):
            try container.encode(value)
        case .timestamp(let value):
            try container.encode(value)
        case .interval(let value):
            try container.encode("\(value)")
        case .data(let value):
            try container.encode(value)
        case .decimal(let value):
            try container.encode(value)
        case .null:
            try container.encodeNil()
        }
    }
}

extension DuckDBData: Sendable {}

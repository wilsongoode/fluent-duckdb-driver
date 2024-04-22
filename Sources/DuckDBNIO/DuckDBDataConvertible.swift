import NIOCore
import Foundation
@preconcurrency import struct DuckDB.Timestamp

public protocol DuckDBDataConvertible {
    init?(duckdbData: DuckDBData)
    var duckdbData: DuckDBData? { get }
}

extension String: DuckDBDataConvertible {
    public init?(duckdbData: DuckDBData) {
        guard let value = duckdbData.string else {
            return nil
        }
        self = value
    }

    public var duckdbData: DuckDBData? {
        return .string(self)
    }
}

extension FixedWidthInteger {
    public init?(duckdbData: DuckDBData) {
        guard let value = duckdbData.integer else {
            return nil
        }
        self = numericCast(value)
    }

    public var duckdbData: DuckDBData? {
        return .int(numericCast(self))
    }
}

extension Int: DuckDBDataConvertible { }
extension Int8: DuckDBDataConvertible { }
extension Int16: DuckDBDataConvertible { }
extension Int32: DuckDBDataConvertible { }
extension Int64: DuckDBDataConvertible { }
extension UInt: DuckDBDataConvertible { }
extension UInt8: DuckDBDataConvertible { }
extension UInt16: DuckDBDataConvertible { }
extension UInt32: DuckDBDataConvertible { }
extension UInt64: DuckDBDataConvertible { }

extension Double: DuckDBDataConvertible {
    public init?(duckdbData: DuckDBData) {
        guard let value = duckdbData.double else {
            return nil
        }
        self = value
    }

    public var duckdbData: DuckDBData? {
        return .double(self)
    }
}

extension Float: DuckDBDataConvertible {
    public init?(duckdbData: DuckDBData) {
        guard let value = duckdbData.double else {
            return nil
        }
        self = Float(value)
    }

    public var duckdbData: DuckDBData? {
        return .double(Double(self))
    }
}

extension ByteBuffer: DuckDBDataConvertible {
    public init?(duckdbData: DuckDBData) {
        guard case .data(let value) = duckdbData else {
            return nil
        }
        self = .init(bytes: value)
    }

    public var duckdbData: DuckDBData? {
        var buffer = self
        
        guard let data = buffer.readBytes(length: buffer.readableBytes) else {
            return nil
        }
        
        return .data(Data(data))
    }
}

extension Data: DuckDBDataConvertible {
    public init?(duckdbData: DuckDBData) {
        guard case .data(let value) = duckdbData else {
            return nil
        }
        self = value
    }

    public var duckdbData: DuckDBData? {
        .data(self)
    }
}

extension Bool: DuckDBDataConvertible {
    public init?(duckdbData: DuckDBData) {
        guard let bool = duckdbData.bool else {
            return nil
        }
        self = bool
    }

    public var duckdbData: DuckDBData? {
        return .bool(self)
    }
}

extension Date: DuckDBDataConvertible {
    public init?(duckdbData: DuckDBData) {
        let value: Double
        
        switch duckdbData {
        case .timestamp(let timestamp):
            value = Double(timestamp.microseconds) / 1e6
        default:
            guard let v = duckdbData.double ?? duckdbData.integer.map({ Double($0) }) else {
                return nil
            }
            value = v
        }
        let valueSinceReferenceDate = value - Date.timeIntervalBetween1970AndReferenceDate
        let secondsSinceReference = round(valueSinceReferenceDate * 1e6) / 1e6
        self.init(timeIntervalSinceReferenceDate: secondsSinceReference)
    }

    public var duckdbData: DuckDBData? {
        return .timestamp(Timestamp(self))
    }
}

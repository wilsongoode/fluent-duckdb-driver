import NIOCore
import Foundation
@preconcurrency import DuckDBNIO

public struct DuckDBDataEncoder {
    public init() {}

    public func encode(_ value: any Encodable) throws -> DuckDBData {
        if let data = (value as? any DuckDBDataConvertible)?.duckdbData {
            return data
        } else {
            let encoder = EncoderImpl()
            
            try value.encode(to: encoder)
            switch encoder.result {
            case .data(let data):
                return data
            case .unkeyed, .keyed:
                return .string(.init(decoding: try JSONEncoder().encode(value), as: UTF8.self))
            }
        }
    }

    private enum Result {
        case keyed
        case unkeyed
        case data(DuckDBData)
    }

    private final class EncoderImpl: Encoder, SingleValueEncodingContainer {
        private struct KeyedEncoderImpl<K: CodingKey>: KeyedEncodingContainerProtocol {
            var codingPath: [any CodingKey] { [] }
            mutating func encodeNil(forKey: K) throws {}
            mutating func encode(_: some Encodable, forKey: K) throws {}
            mutating func nestedContainer<N: CodingKey>(keyedBy: N.Type, forKey: K) -> KeyedEncodingContainer<N> { .init(KeyedEncoderImpl<N>()) }
            mutating func nestedUnkeyedContainer(forKey: K) -> any UnkeyedEncodingContainer { UnkeyedEncoderImpl() }
            mutating func superEncoder() -> any Encoder { EncoderImpl() }
            mutating func superEncoder(forKey: K) -> any Encoder { EncoderImpl() }
        }

        private struct UnkeyedEncoderImpl: UnkeyedEncodingContainer {
            var codingPath: [any CodingKey] { [] }
            var count: Int = 0
            mutating func encodeNil() throws {}
            mutating func encode(_: some Encodable) throws {}
            mutating func nestedContainer<N: CodingKey>(keyedBy: N.Type) -> KeyedEncodingContainer<N> { .init(KeyedEncoderImpl<N>()) }
            mutating func nestedUnkeyedContainer() -> any UnkeyedEncodingContainer { UnkeyedEncoderImpl() }
            mutating func superEncoder() -> any Encoder { EncoderImpl() }
        }
    
        var codingPath: [any CodingKey] { [] }
        var userInfo: [CodingUserInfoKey: Any] { [:] }
        var result: Result
        
        init() { self.result = .data(.null) }

        func container<K: CodingKey>(keyedBy: K.Type) -> KeyedEncodingContainer<K> {
            self.result = .keyed
            return .init(KeyedEncoderImpl())
        }

        func unkeyedContainer() -> any UnkeyedEncodingContainer {
            self.result = .unkeyed
            return UnkeyedEncoderImpl()
        }

        func singleValueContainer() -> any SingleValueEncodingContainer { self }

        func encodeNil() throws { self.result = .data(.null) }
        
        func encode(_ value: some Encodable) throws {
            self.result = .data(try DuckDBDataEncoder().encode(value))
        }
    }
}

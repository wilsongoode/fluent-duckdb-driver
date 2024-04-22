import Foundation
@preconcurrency import DuckDBNIO
import NIOFoundationCompat

public struct DuckDBDataDecoder {
    let json = JSONDecoder()
    
    public init() {}

    public func decode<T: Decodable>(_ type: T.Type, from data: DuckDBData) throws -> T {
        // If `T` can be converted directly, just do so.
        if let type = type as? any DuckDBDataConvertible.Type {
            guard let value = type.init(duckdbData: data) else {
                throw DecodingError.typeMismatch(T.self, .init(
                    codingPath: [],
                    debugDescription: "Could not initialize \(T.self) from \(data)."
                ))
            }
            return value as! T
        } else {
            do {
                return try T.init(from: GiftBoxUnwrapDecoder(decoder: self, data: data))
            } catch is TryJSONSentinel {
                // Couldn't unwrap it either. Fall back to attempting a JSON decode.
                let buf: Data = switch data {
                case .string(let str):  .init(str.utf8)
                case .data(let data): data
                default: .init()
                }
                return try self.json.decode(T.self, from: buf)
            }
        }
    }
    
    private struct TryJSONSentinel: Swift.Error {}

    private struct GiftBoxUnwrapDecoder: Decoder, SingleValueDecodingContainer {
        let decoder: DuckDBDataDecoder
        let data: DuckDBData
        
        var codingPath: [any CodingKey] { [] }
        var userInfo: [CodingUserInfoKey: Any] { [:] }

        func container<K: CodingKey>(keyedBy: K.Type) throws -> KeyedDecodingContainer<K> { throw TryJSONSentinel() }
        func unkeyedContainer() throws -> any UnkeyedDecodingContainer { throw TryJSONSentinel() }
        func singleValueContainer() throws -> any SingleValueDecodingContainer { self }
        func decodeNil() -> Bool { self.data.isNull }
        func decode<T: Decodable>(_: T.Type) throws -> T { try self.decoder.decode(T.self, from: self.data) }
    }
}

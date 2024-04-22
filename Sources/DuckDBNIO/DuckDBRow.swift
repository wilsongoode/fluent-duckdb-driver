public struct DuckDBColumn: CustomStringConvertible {
    public let name: String
    public let data: DuckDBData

    public var description: String {
        "\(self.name): \(self.data)"
    }
}

public struct DuckDBRow {
    let columnOffsets: DuckDBColumnOffsets
    let data: [DuckDBData]

    public var columns: [DuckDBColumn] {
        self.columnOffsets.offsets.map { (name, offset) in
            DuckDBColumn(name: name, data: self.data[offset])
        }
    }

    public func column(_ name: String) -> DuckDBData? {
        guard let offset = self.columnOffsets.lookupTable[name] else {
            return nil
        }
        return self.data[offset]
    }
}

extension DuckDBRow: CustomStringConvertible {
    public var description: String {
        self.columns.description
    }
}

final class DuckDBColumnOffsets {
    let offsets: [(String, Int)]
    let lookupTable: [String: Int]

    init(offsets: [(String, Int)]) {
        self.offsets = offsets
        self.lookupTable = .init(offsets, uniquingKeysWith: { a, b in a })
    }
}

extension DuckDBRow: Sendable {}
extension DuckDBColumnOffsets: Sendable {}

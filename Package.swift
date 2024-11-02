// swift-tools-version: 5.10

import PackageDescription

let package = Package(
    name: "fluent-duckdb-driver",
    platforms: [.macOS(.v10_15)],
    products: [
        .library(
            name: "FluentDuckDBDriver",
            targets: ["FluentDuckDBDriver"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.59.0"),
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.2.0"),
        .package(url: "https://github.com/vapor/async-kit.git", from: "1.17.0"),
        .package(url: "https://github.com/vapor/fluent-kit.git", from: "1.43.0"),
        .package(url: "https://github.com/vapor/fluent.git", from: "4.12.0"),
        .package(url: "https://github.com/duckdb/duckdb-swift", from: "1.1.0"),
    ],
    targets: [
        .target(
            name: "FluentDuckDBDriver",
            dependencies: [
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "AsyncKit", package: "async-kit"),
                .product(name: "FluentKit", package: "fluent-kit"),
                .product(name: "FluentSQL", package: "fluent-kit"),
                .product(name: "DuckDB", package: "duckdb-swift"),
                "DuckDBKit",
                "DuckDBNIO",
            ]
        ),
        .target(
            name: "DuckDBKit",
            dependencies: [
                .product(name: "AsyncKit", package: "async-kit"),
                .product(name: "DuckDB", package: "duckdb-swift"),
                "DuckDBNIO",
            ]
        ),
        .target(
            name: "DuckDBNIO",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "DuckDB", package: "duckdb-swift"),
            ]
        ),
        .testTarget(
            name: "FluentDuckDBDriverTests",
            dependencies: [
                .product(name: "Fluent", package: "fluent"),
                "FluentDuckDBDriver",
            ]
        ),
    ]
)

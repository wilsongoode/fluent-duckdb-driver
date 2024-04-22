[DuckDB](https://duckdb.org) driver for [Vaport Fluent]([https://](https://docs.vapor.codes/fluent/overview/))
# Package.swift
## Package dependeicnes:
```swift
.package(url: "https://github.com/vsevolod-volkov/fluent-duckdb-driver.git", from: "0.1.0"),
```
## Target dependencies:
```swift
.product(name: "FluendDuckDBDriver", package: "fluent-duckdb-driver"),
```

# Usage
```swift
import Fluent
import FluendDuckDBDriver

// in-memory
app.databases.use(.duckdb(), as: .duckdb)

// stored
app.databases.use(.duckdb(configuration: DuckDBConfiguration(
      store: .file(at: URL(fileURLWithPath: "./my_database.db"))
   )),
   as: .duckdb
)

// custom configuration
app.databases.use(.duckdb(configuration: DuckDBConfiguration(
      store: .file(at: URL(fileURLWithPath: "./my_database.db")),
      configuration: [
         // for full parameter list refer to
         //   https://duckdb.org/docs/configuration/overview#global-configuration-options
         "access_mode": "READ_WRITE",
      ]
   )),
   as: .duckdb
)
```

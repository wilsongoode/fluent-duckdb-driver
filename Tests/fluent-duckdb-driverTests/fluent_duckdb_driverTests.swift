import Foundation
import XCTest
import Fluent
import FluentSQL
import NIOPosix

@testable import FluentDuckDBDriver

final class FluentDuckDBDriverTests: XCTestCase {
    static let logger = Logger(label: "TEST")
    
    func testConnection() async throws {
        let configuration = DatabaseConfigurationFactory.duckdb().make()

        let threadPool = NIOThreadPool(numberOfThreads: ProcessInfo.processInfo.processorCount)
        threadPool.start()
        
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: ProcessInfo.processInfo.processorCount)
        
        defer {
            try! eventLoopGroup.syncShutdownGracefully()
        }
        
        let driver = configuration.makeDriver(for: Databases(
            threadPool: threadPool,
            on: eventLoopGroup
        ))
        
        defer {
            driver.shutdown()
        }
        
        let context = DatabaseContext(
            configuration: configuration,
            logger: FluentDuckDBDriverTests.logger,
            eventLoop: eventLoopGroup.next(),
            history: nil,
            pageSizeLimit: 100
        )

        let database = driver.makeDatabase(with: context)
        

        final class Test: Model {
            static var schema = "test_table"
            @ID(key: .id)
            var id
            
            @Field(key: "name")
            var name: String
            
            @Field(key: "created_at")
            var creationDate: Date?
            
            @Field(key: "i")
            var i: Int

            @Field(key: "f")
            var f: Double
            
            @Field(key: "s")
            var s: TestStruct
            
            @Field(key: "s_array")
            var sArray: [TestStruct]
            
            struct TestStruct: Codable {
                var key: String
                var value: String
            }

            public init() {
            }
            
            public init(
                id: IDValue? = nil,
                name: String,
                creationDate: Date? = nil,
                i: Int,
                f: Double,
                s: TestStruct,
                sArray: [TestStruct]
            ) {
                if let id { self.id = id }
                self.name = name
                if let creationDate { self.creationDate = creationDate }
                self.i = i
                self.f = f
                self.s = s
                self.sArray = sArray
            }
        }
        
        try await database.schema("test_table")
            .id()
            .field("name", .string, .required)
            .field("created_at", .datetime, .required, .sql(.default(SQLFunction("now"))))
            .field("i", .int, .required)
            .field("f", .double, .required)
            .field("s", .dictionary, .required)
            .field("s_array", .array, .required)
            .create()

        for i in 1...10 {
            try await Test(
                name: "This is test #\(i)",
                i: i,
                f: Double(i) / 10,
                s: Test.TestStruct(key: "SomeKey", value: "SomeValue"),
                sArray: [
                    Test.TestStruct(key: "SomeKey1", value: "SomeValue1"),
                    Test.TestStruct(key: "SomeKey2", value: "SomeValue2")
                ]
            ).create(on: database)
        }
        
        let rows = try await Test.query(on: database).all()
        
        
        for row in rows {
            print(row)
        }
        
        
    }
}

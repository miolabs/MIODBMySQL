import XCTest
@testable import MIODBMySQL

final class MIODBMySQLTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(MIODBMySQL().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}

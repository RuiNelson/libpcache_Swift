//
//  ReadmeExampleTests.swift
//
//  MIT 2-Claude License.
//

@testable import libpcache_Swift
import Foundation
import Testing

struct ReadmeExampleTests {
    @Test func `cookbook example`() throws {
        let uuid = UUID().uuidString
        
        let dbURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(uuid)
            .appendingPathExtension("db")
        let datURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(uuid)
            .appendingPathExtension("dat")
        defer {
            try? FileManager.default.removeItem(at: dbURL)
            try? FileManager.default.removeItem(at: datURL)
        }

        let files = FilePair(databaseURL: dbURL, dataURL: datURL)!

        let config = Configuration(
            pageSize: 4096,
            maxPages: 1000,
            idWidth: 16,
            capacityPolicy: .fixed,
        )!

        try PersistentCache.create(files: files, configuration: config)
        let cache = try PersistentCache(files: files)

        var id = "Hello, World!!!".data(using: .ascii)!
        id.append(contentsOf: repeatElement(0, count: 16 - id.count))

        let page = Data(repeatElement(0x42, count: 4096))

        try cache.putPage(id: id, data: page)

        let retrieved: Data = try cache.getPage(id: id)
        #expect(retrieved == page)

        try cache.close()
    }

    @Test func `cookbook counter example`() throws {
        let uuid = UUID().uuidString

        let dbURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(uuid)
            .appendingPathExtension("db")
        let datURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(uuid)
            .appendingPathExtension("dat")
        defer {
            try? FileManager.default.removeItem(at: dbURL)
            try? FileManager.default.removeItem(at: datURL)
        }

        let files = FilePair(databaseURL: dbURL, dataURL: datURL)!

        let config = Configuration(
            pageSize: 4096,
            maxPages: 1000,
            idWidth: 16,
            capacityPolicy: .fixed,
        )!

        try PersistentCache.create(files: files, configuration: config)
        let cache = try PersistentCache(files: files)
        defer { try? cache.close() }

        var counter = Counter(
            template: Data(repeatElement(0xAB, count: 14)),
            zeroPad: 2,
            position: 0,
            initialValue: 0,
            endianess: .bigEndian,
        )

        let batchData = Data(repeatElement(0xFE, count: 4096 * 100))
        try cache.putPages(counter: counter, data: batchData)

        counter.advance(100)
        let nextBatch = Data(repeatElement(0xED, count: 4096 * 100))
        try cache.putPages(counter: counter, data: nextBatch)

        let secondBatch: Data = try cache.getPages(counter: counter, count: 100)

        counter.backwards(100)
        let firstBatch: Data = try cache.getPages(counter: counter, count: 100)

        #expect(firstBatch == batchData)
        #expect(secondBatch == nextBatch)
    }
}

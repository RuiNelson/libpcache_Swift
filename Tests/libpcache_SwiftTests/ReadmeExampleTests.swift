//
//  ReadmeExampleTests.swift
//
//  MIT 2-Claude License.
//

@testable import libpcache_Swift
import Foundation
import Testing

struct ReadmeExampleTests {
    // Mirrors README "Cookbook" section, except:
    //  - pageCount = 256 (README uses 131072 = 4GB/32kB; too large for /tmp)
    //  - deletePage uses durable: false (fsync fails on macOS temp directory)
    @Test func `cookbook example`() throws {
        let pageSize = 32 * 1024
        let pageCount = 256
        let idSize = 36

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
            pageSize: pageSize,
            maxPages: pageCount,
            idWidth: idSize,
            capacityPolicy: .fixed,
        )!

        try PersistentCache.create(files: files, configuration: config)

        let cache = try PersistentCache(files: files)

        let id = Data(repeating: 0xAB, count: idSize)
        let page = Data(repeating: 0x42, count: pageSize)
        try cache.putPage(id: id, data: page)

        let retrieved: Data = try cache.getPage(id: id)
        #expect(retrieved == page)

        try cache.deletePage(id: id, wipe: false, durable: false)

        try cache.close()
    }

    @Test func `cookbook counter example`() throws {
        let pageSize = 4096
        let idSize = 36

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
            pageSize: pageSize,
            maxPages: 1000,
            idWidth: idSize,
            capacityPolicy: .fixed,
        )!

        try PersistentCache.create(files: files, configuration: config)
        let cache = try PersistentCache(files: files)
        defer { try? cache.close() }

        var counter = Counter(
            template: Data(repeating: 0xAB, count: idSize - 2),
            zeroPad: 2,
            position: 0,
            initialValue: 0,
            endianess: .bigEndian,
        )

        let batchData = Data(repeating: 0x12, count: pageSize * 100)
        try cache.putPages(counter: counter, data: batchData)

        counter.advance(100)
        let nextBatch = Data(repeating: 0x34, count: pageSize * 100)
        try cache.putPages(counter: counter, data: nextBatch)

        let secondBatch: Data = try cache.getPages(counter: counter, count: 100)

        counter.backwards(100)
        let firstBatch: Data = try cache.getPages(counter: counter, count: 100)

        #expect(firstBatch == batchData)
        #expect(secondBatch == nextBatch)
    }
}

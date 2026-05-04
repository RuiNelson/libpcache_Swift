//
//  libpcache_SwiftTests.swift
//
//  MIT 2-Claude License.
//

@testable import libpcache_Swift
import Foundation
import Testing

// MARK: - Helpers

private func makeTempFiles() -> (db: URL, dat: URL, pair: FilePair) {
    let tmp = FileManager.default.temporaryDirectory
    let uuid = UUID().uuidString
    let db = tmp.appendingPathComponent("\(uuid).db")
    let dat = tmp.appendingPathComponent("\(uuid).dat")
    let pair = FilePair(databaseURL: db, dataURL: dat)!
    return (db, dat, pair)
}

private func cleanup(_ urls: URL...) {
    for url in urls {
        try? FileManager.default.removeItem(at: url)
    }
}

private func withCache(
    pageSize: Int = 4096,
    maxPages: Int = 10,
    idWidth: Int = 16,
    policy: CapacityPolicy = .fixed,
    _ body: (PersistentCache) throws -> Void,
) throws {
    let (db, dat, pair) = makeTempFiles()
    defer { cleanup(db, dat) }
    let config = try #require(
        Configuration(pageSize: pageSize, maxPages: maxPages, idWidth: idWidth, capacityPolicy: policy),
    )
    try PersistentCache.create(files: pair, configuration: config)
    let cache = try PersistentCache(files: pair)
    try body(cache)
    try cache.close()
}

private func makeID(_ value: UInt8, width: Int) -> Data {
    Data(repeating: value, count: width)
}

private func makePage(_ value: UInt8, size: Int) -> Data {
    Data(repeating: value, count: size)
}

// MARK: - Pure Types

struct FilePairTests {
    @Test func `valid file URL`() {
        let db = URL(fileURLWithPath: "/tmp/test.db")
        let dat = URL(fileURLWithPath: "/tmp/test.dat")
        let pair = FilePair(databaseURL: db, dataURL: dat)
        #expect(pair != nil)
    }

    @Test func `invalid scheme returns nil`() {
        let db = URL(string: "https://example.com/test.db")!
        let dat = URL(fileURLWithPath: "/tmp/test.dat")
        let pair = FilePair(databaseURL: db, dataURL: dat)
        #expect(pair == nil)
    }
}

struct ConfigurationTests {
    @Test func `valid parameters`() {
        let cfg = Configuration(pageSize: 4096, maxPages: 100, idWidth: 16, capacityPolicy: .fixed)
        #expect(cfg != nil)
        #expect(cfg?.pageSize == 4096)
        #expect(cfg?.maxPages == 100)
        #expect(cfg?.idWidth == 16)
        #expect(cfg?.pageSizeInt == 4096)
        #expect(cfg?.maxPagesInt == 100)
        #expect(cfg?.idWidthInt == 16)
        #expect(cfg?.capacityPolicy == .fixed)
    }

    @Test func `zero page size returns nil`() {
        #expect(Configuration(pageSize: 0, maxPages: 100, idWidth: 16, capacityPolicy: .fixed) == nil)
    }

    @Test func `negative page size returns nil`() {
        #expect(Configuration(pageSize: -1, maxPages: 100, idWidth: 16, capacityPolicy: .fixed) == nil)
    }

    @Test func `exceeds int max returns nil`() {
        #expect(
            Configuration(
                pageSize: Int(UInt32.max) + 1,
                maxPages: 100,
                idWidth: 16,
                capacityPolicy: .fixed,
            ) == nil,
        )
    }

    @Test func `init from capacity`() {
        let cfg = Configuration(
            capacity: 4096 * 10,
            pageSize: 4096,
            idWidth: 16,
            capacityPolicy: .fifo,
        )
        #expect(cfg != nil)
        #expect(cfg?.maxPages == 10)
        #expect(cfg?.capacityPolicy == .fifo)
    }

    @Test func `init from capacity invalid multiple`() {
        let cfg = Configuration(
            capacity: 4096 + 1,
            pageSize: 4096,
            idWidth: 16,
            capacityPolicy: .fixed,
        )
        #expect(cfg == nil)
    }

    @Test func `init from capacity too small`() {
        let cfg = Configuration(
            capacity: 512,
            pageSize: 4096,
            idWidth: 16,
            capacityPolicy: .fixed,
        )
        #expect(cfg == nil)
    }
}

struct CounterTests {
    @Test func `init and advance`() {
        var counter = Counter(
            template: Data([0xAB, 0xCD]),
            zeroPad: 2,
            position: 0,
            initialValue: 5,
            endianness: .bigEndian,
        )
        #expect(counter.templateWidth == 4)
        #expect(counter.initialValue == 5)
        counter.advance(3)
        #expect(counter.initialValue == 8)
        counter.backwards(2)
        #expect(counter.initialValue == 6)
    }

    @Test func `zero pad zero`() {
        let counter = Counter(
            template: Data([0x01]),
            zeroPad: 0,
            position: 0,
            initialValue: 0,
            endianness: .littleEndian,
        )
        #expect(counter.templateWidth == 1)
    }
}

struct SquashedTests {
    @Test func `squashed empty`() {
        let arr: [Data] = []
        #expect(arr.squashed().isEmpty)
    }

    @Test func `squashed concatenates`() {
        let arr = [Data([0x01, 0x02]), Data([0x03]), Data([0x04, 0x05, 0x06])]
        #expect(arr.squashed() == Data([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]))
    }
}

// MARK: - Lifecycle & Bridge Errors

struct LifecycleTests {
    @Test func `create and open`() throws {
        let (db, dat, pair) = makeTempFiles()
        defer { cleanup(db, dat) }

        let config = try #require(
            Configuration(pageSize: 4096, maxPages: 10, idWidth: 16, capacityPolicy: .fixed),
        )
        try PersistentCache.create(files: pair, configuration: config)
        #expect(FileManager.default.fileExists(atPath: db.path))
        #expect(FileManager.default.fileExists(atPath: dat.path))

        let cache = try PersistentCache(files: pair)
        let cfg = try cache.configuration
        #expect(cfg.pageSize == 4096)
        #expect(cfg.maxPages == 10)
        #expect(cfg.idWidth == 16)
        #expect(cfg.capacityPolicy == .fixed)

        try cache.close()
    }

    @Test func `create fails when file exists`() throws {
        let (db, dat, pair) = makeTempFiles()
        defer { cleanup(db, dat) }

        let config = try #require(
            Configuration(pageSize: 4096, maxPages: 10, idWidth: 16, capacityPolicy: .fixed),
        )
        try PersistentCache.create(files: pair, configuration: config)

        #expect(throws: CreateVolumeError.fileExists) {
            try PersistentCache.create(files: pair, configuration: config)
        }
    }

    @Test func `open fails when not found`() {
        let db = URL(fileURLWithPath: "/tmp/nonexistent_\(UUID().uuidString).db")
        let dat = URL(fileURLWithPath: "/tmp/nonexistent_\(UUID().uuidString).dat")
        let pair = FilePair(databaseURL: db, dataURL: dat)!
        defer { cleanup(db, dat) }

        #expect(throws: OpenVolumeError.notFound) {
            _ = try PersistentCache(files: pair)
        }
    }
}

// MARK: - Validation (wrapper-only)

struct ValidationTests {
    @Test func `put page wrong ID size`() throws {
        try withCache { cache in
            let badID = Data([0x00])
            let pageData = makePage(0xAA, size: 4096)
            #expect(throws: InvalidCall.idBufferIsNotTheExpectedSize) {
                try cache.putPage(id: badID, data: pageData)
            }
        }
    }

    @Test func `put page wrong data size`() throws {
        try withCache { cache in
            let idData = makeID(0x01, width: 16)
            let badPage = Data([0x00])
            #expect(throws: InvalidCall.dataBufferIsNotTheExpectedSize) {
                try cache.putPage(id: idData, data: badPage)
            }
        }
    }

    @Test func `put pages mismatched counts`() throws {
        try withCache { cache in
            let ids = Data([0x01]) + Data(repeating: 0, count: 15) // 1 id
            let pages = Data(repeating: 0xAA, count: 4096 * 2) // 2 pages
            #expect(throws: InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer) {
                try cache.putPages(ids: ids, data: pages)
            }
        }
    }

    @Test func `get pages wrong ID buffer size`() throws {
        try withCache { cache in
            let badIDs = Data([0x01, 0x02]) // not multiple of idWidth
            #expect(throws: InvalidCall.idBufferIsNotTheExpectedSize) {
                _ = try cache.getPages(ids: badIDs)
            }
        }
    }

    @Test func `get pages array with wrong ID size`() throws {
        try withCache { cache in
            let ids = [Data([0x01]), Data([0x02])]
            #expect(throws: InvalidCall.idBufferIsNotTheExpectedSize) {
                let _: Data = try cache.getPages(ids: ids)
            }
        }
    }

    @Test func `put pages array mismatched counts`() throws {
        try withCache { cache in
            let ids = [makeID(0x01, width: 16), makeID(0x02, width: 16)]
            let pages = [makePage(0xAA, size: 4096)]
            #expect(throws: InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer) {
                try cache.putPages(ids: ids, data: pages)
            }
        }
    }

    @Test func `put pages array wrong page size`() throws {
        try withCache { cache in
            let ids = [makeID(0x01, width: 16)]
            let pages = [Data([0x00])]
            #expect(throws: InvalidCall.dataBufferIsNotTheExpectedSize) {
                try cache.putPages(ids: ids, data: pages)
            }
        }
    }

    @Test func `put pages tuple wrong ID size`() throws {
        try withCache { cache in
            let pages = [(id: Data([0x01]), data: makePage(0xAA, size: 4096))]
            #expect(throws: InvalidCall.idBufferIsNotTheExpectedSize) {
                try cache.putPages(pages: pages)
            }
        }
    }

    @Test func `set new max pages zero`() throws {
        try withCache { cache in
            #expect(throws: InvalidCall.invalidArguments) {
                try cache.setNewMaxPages(0)
            }
        }
    }

    @Test func `preallocate no flags`() throws {
        try withCache { cache in
            #expect(throws: InvalidCall.invalidArguments) {
                try cache.preallocate(database: false, datafile: false)
            }
        }
    }

    @Test func `check page wrong ID size`() throws {
        try withCache { cache in
            let badID = Data([0x01])
            #expect(throws: InvalidCall.idBufferIsNotTheExpectedSize) {
                _ = try cache.checkPage(id: badID)
            }
        }
    }

    @Test func `delete page wrong ID size`() throws {
        try withCache { cache in
            let badID = Data([0x01])
            #expect(throws: InvalidCall.idBufferIsNotTheExpectedSize) {
                try cache.deletePage(id: badID)
            }
        }
    }

    @Test func `delete pages array wrong ID size`() throws {
        try withCache { cache in
            let ids = [Data([0x01]), Data([0x02])]
            #expect(throws: InvalidCall.idBufferIsNotTheExpectedSize) {
                try cache.deletePages(ids: ids)
            }
        }
    }

    @Test func `put pages counter wrong template width`() throws {
        try withCache { cache in
            let counter = Counter(
                template: Data([0x01]),
                zeroPad: 0,
                position: 0,
                initialValue: 0,
                endianness: .bigEndian,
            )
            let data = makePage(0xAA, size: 4096)
            #expect(throws: InvalidCall.idBufferIsNotTheExpectedSize) {
                try cache.putPages(counter: counter, data: data)
            }
        }
    }
}

// MARK: - Page Operations (wrapper integration)

struct PageOperationTests {
    @Test func `put and get single page`() throws {
        try withCache { cache in
            let idData = makeID(0xAB, width: 16)
            let pageData = makePage(0xCD, size: 4096)
            try cache.putPage(id: idData, data: pageData)
            let retrieved = try cache.getPage(id: idData)
            #expect(retrieved == pageData)
        }
    }

    @Test func `put and get multiple pages`() throws {
        try withCache(maxPages: 4) { cache in
            let ids = [makeID(0x01, width: 16), makeID(0x02, width: 16)]
            let pages = [makePage(0xAA, size: 4096), makePage(0xBB, size: 4096)]
            try cache.putPages(ids: ids, data: pages)

            let retrieved: Data = try cache.getPages(ids: ids)
            #expect(retrieved.count == 8192)
            #expect(retrieved[0 ..< 4096] == pages[0])
            #expect(retrieved[4096 ..< 8192] == pages[1])
        }
    }

    @Test func `put and get with counter`() throws {
        try withCache { cache in
            let counter = Counter(
                template: Data(repeating: 0x00, count: 16),
                zeroPad: 0,
                position: 0,
                initialValue: 0,
                endianness: .bigEndian,
            )
            let pages = Data(repeating: 0x42, count: 4096 * 3)
            try cache.putPages(counter: counter, data: pages)

            let retrieved = try cache.getPages(counter: counter, count: 3)
            #expect(retrieved == pages)
        }
    }

    @Test func `check page before and after put`() throws {
        try withCache { cache in
            let idData = makeID(0x01, width: 16)
            #expect(try cache.checkPage(id: idData) == false)

            try cache.putPage(id: idData, data: makePage(0xAA, size: 4096))
            #expect(try cache.checkPage(id: idData) == true)
        }
    }

    @Test func `check pages array`() throws {
        try withCache { cache in
            let ids = [makeID(0x01, width: 16), makeID(0x02, width: 16), makeID(0x03, width: 16)]
            try cache.putPage(id: ids[1], data: makePage(0xBB, size: 4096))

            let results = try cache.checkPages(ids: ids)
            #expect(results == [false, true, false])
        }
    }

    @Test func `delete page removes it`() throws {
        try withCache { cache in
            let idData = makeID(0x01, width: 16)
            try cache.putPage(id: idData, data: makePage(0xAA, size: 4096))
            #expect(try cache.checkPage(id: idData) == true)

            try cache.deletePage(id: idData, durable: false)
            #expect(try cache.checkPage(id: idData) == false)
        }
    }

    @Test func `delete pages array`() throws {
        try withCache { cache in
            let ids = [makeID(0x01, width: 16), makeID(0x02, width: 16)]
            try cache.putPages(ids: ids, data: [makePage(0xAA, size: 4096), makePage(0xBB, size: 4096)])
            try cache.deletePages(ids: ids, durable: false)
            #expect(try cache.checkPages(ids: ids) == [false, false])
        }
    }

    @Test func `fail if exists prevents duplicate`() throws {
        try withCache { cache in
            let idData = makeID(0x01, width: 16)
            try cache.putPage(id: idData, data: makePage(0xAA, size: 4096))
            #expect(throws: PutPagesError.duplicateID) {
                try cache.putPage(id: idData, data: makePage(0xBB, size: 4096), failIfExists: true)
            }
        }
    }

    @Test func `get pages range returns tuples`() throws {
        try withCache { cache in
            let first = makeID(0x01, width: 16)
            let last = makeID(0x03, width: 16)
            let middle = makeID(0x02, width: 16)

            try cache.putPage(id: first, data: makePage(0xAA, size: 4096))
            try cache.putPage(id: middle, data: makePage(0xBB, size: 4096))
            try cache.putPage(id: last, data: makePage(0xCC, size: 4096))

            let result: [(id: Data, page: Data)] = try cache.getPagesRange(first: first, last: last)
            #expect(result.count == 3)
            #expect(result[0].id == first)
            #expect(result[0].page == makePage(0xAA, size: 4096))
            #expect(result[1].id == middle)
            #expect(result[1].page == makePage(0xBB, size: 4096))
            #expect(result[2].id == last)
            #expect(result[2].page == makePage(0xCC, size: 4096))
        }
    }

    @Test func `delete pages range`() throws {
        try withCache { cache in
            let first = makeID(0x01, width: 16)
            let last = makeID(0x03, width: 16)
            let middle = makeID(0x02, width: 16)

            try cache.putPage(id: first, data: makePage(0xAA, size: 4096), durable: false)
            try cache.putPage(id: middle, data: makePage(0xBB, size: 4096), durable: false)
            try cache.putPage(id: last, data: makePage(0xCC, size: 4096), durable: false)

            try cache.deletePagesRange(first: first, last: last, durable: false)
            #expect(try cache.checkPages(ids: [first, middle, last]) == [false, false, false])
        }
    }

    @Test func `check pages range count`() throws {
        try withCache { cache in
            let first = makeID(0x01, width: 16)
            let last = makeID(0x03, width: 16)
            let middle = makeID(0x02, width: 16)

            try cache.putPage(id: first, data: makePage(0xAA, size: 4096))
            try cache.putPage(id: middle, data: makePage(0xBB, size: 4096))

            let count = try cache.checkPagesRange(first: first, last: last)
            #expect(count == 2)
        }
    }
}

// MARK: - Maintenance & Introspection

struct MaintenanceTests {
    @Test func `page counts reflect usage`() throws {
        try withCache(maxPages: 5) { cache in
            var counts = try cache.pageCounts()
            #expect(counts.used == 0)
            #expect(counts.free == 5)

            try cache.putPage(id: makeID(0x01, width: 16), data: makePage(0xAA, size: 4096))
            counts = try cache.pageCounts()
            #expect(counts.used == 1)
            #expect(counts.free == 4)
        }
    }

    @Test func `set new max pages`() throws {
        try withCache(maxPages: 5) { cache in
            try cache.putPage(id: makeID(0x01, width: 16), data: makePage(0xAA, size: 4096))
            try cache.setNewMaxPages(3)
            let cfg = try cache.configuration
            #expect(cfg.maxPages == 3)
        }
    }

    @Test func `set new max pages would discard`() throws {
        try withCache(maxPages: 5) { cache in
            try cache.putPage(id: makeID(0x01, width: 16), data: makePage(0xAA, size: 4096))
            try cache.putPage(id: makeID(0x02, width: 16), data: makePage(0xBB, size: 4096))
            #expect(throws: VolumeSetMaxPagesError.wouldDiscardPages) {
                try cache.setNewMaxPages(1)
            }
        }
    }

    @Test func `defragment runs`() throws {
        try withCache(maxPages: 5) { cache in
            try cache.putPage(id: makeID(0x01, width: 16), data: makePage(0xAA, size: 4096), durable: false)
            try cache.putPage(id: makeID(0x02, width: 16), data: makePage(0xBB, size: 4096), durable: false)
            try cache.deletePage(id: makeID(0x01, width: 16), durable: false)
            try cache.defragment(shrinkFile: false, durable: false) { _ in true }
            let counts = try cache.pageCounts()
            #expect(counts.used == 1)
        }
    }

    @Test func `defragment can cancel`() throws {
        try withCache(maxPages: 5) { cache in
            try cache.putPage(id: makeID(0x01, width: 16), data: makePage(0xAA, size: 4096), durable: false)
            #expect(throws: DefragmentVolumeError.cancelled) {
                try cache.defragment(shrinkFile: false, durable: false) { _ in false }
            }
        }
    }

    @Test func `preallocate runs`() throws {
        try withCache(maxPages: 5) { cache in
            try cache.preallocate(database: true, datafile: true)
        }
    }
}

// MARK: - FIFO Policy

struct FIFOPolicyTests {
    @Test func `fifo configuration round trips`() throws {
        let (db, dat, pair) = makeTempFiles()
        defer { cleanup(db, dat) }

        let config = try #require(
            Configuration(pageSize: 4096, maxPages: 10, idWidth: 16, capacityPolicy: .fifo),
        )
        try PersistentCache.create(files: pair, configuration: config)
        let cache = try PersistentCache(files: pair)
        #expect(try cache.configuration.capacityPolicy == .fifo)
        try cache.close()
    }
}

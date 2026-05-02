//
//  PC-Check.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

// MARK: - C

public extension PersistentCache {
    /// Tests whether a page identified by `id` exists in the volume.
    ///
    /// The check is serviced entirely against the index database; the data file is not read.
    ///
    /// - Parameter id: Page identifier; must be exactly ``Configuration/idWidthInt`` bytes.
    ///
    /// - Returns: `true` if the page exists, `false` otherwise.
    /// - Throws: Error if the check fails.
    func checkPage(id: CBuffer) throws -> Bool {
        try validateIDBuffer(id)
        return try b_checkPage(handle: handle, id: id.pointer)
    }

    /// Tests whether multiple pages exist in the volume.
    ///
    /// - Parameters:
    ///   - ids: Page identifiers; must be `count * idWidth` bytes.
    ///   - results: Caller-supplied array of at least `count` booleans; `true` for each
    ///     page that exists, `false` otherwise.
    ///
    /// - Throws: Error if the check fails.
    func checkPages(ids: CBuffer, results: UnsafeMutablePointer<Bool>) throws {
        let count = try itemCount(fromIDs: ids)
        try b_checkPages(handle: handle, count: count, ids: ids.pointer, results: results)
    }

    /// Tests whether multiple pages exist, with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - count: Number of pages to check.
    ///   - results: Caller-supplied array of at least `count` booleans.
    ///
    /// - Throws: ``CheckPagesError`` if `position` is out of bounds, the counter overflows,
    ///   or `endianess` is invalid.
    func checkPages(counter: Counter, count: Int, results: UnsafeMutablePointer<Bool>) throws {
        try validateCounter(counter)
        try counter.template.withUnsafeBytes { counterBuf in
            try b_checkPagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.baseAddress!,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianess,
                results: results,
            )
        }
    }

    /// Counts pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// Uses byte-by-byte comparison (SQLite BLOB ordering). An empty match is not an error.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive); exactly ``Configuration/idWidthInt`` bytes.
    ///   - last: Upper bound of the identifier range (inclusive); exactly ``Configuration/idWidthInt`` bytes.
    ///
    /// - Returns: The number of pages in the range.
    ///
    /// - Throws: ``CheckPagesError`` if `first > last`.
    func checkPagesRange(first: CBuffer, last: CBuffer) throws -> Int {
        try validateIDBuffer(first)
        try validateIDBuffer(last)
        return try b_checkPagesRange(handle: handle, first: first.pointer, last: last.pointer)
    }
}

// MARK: - Swift

public extension PersistentCache {
    /// Tests whether a page identified by `id` exists in the volume.
    ///
    /// - Parameter id: Page identifier.
    ///
    /// - Returns: `true` if the page exists, `false` otherwise.
    /// - Throws: Error if the check fails.
    func checkPage(id: RawSpan) throws -> Bool {
        try id.withUnsafeBytes { try checkPage(id: ($0.baseAddress!, $0.count)) }
    }

    /// Tests whether multiple pages exist in the volume.
    ///
    /// - Parameter ids: Contiguous memory region containing `count` identifiers.
    ///
    /// - Returns: Array of booleans, `true` for each page that exists.
    /// - Throws: Error if the check fails.
    func checkPages(ids: RawSpan) throws -> [Bool] {
        try ids.withUnsafeBytes { idsBuf in
            let count = idsBuf.count / configuration.idWidthInt
            var results = [Bool](repeating: false, count: count)
            try checkPages(ids: (idsBuf.baseAddress!, idsBuf.count), results: &results)
            return results
        }
    }

    /// Tests whether multiple pages exist, with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - count: Number of pages to check.
    ///
    /// - Returns: Array of booleans, `true` for each page that exists.
    /// - Throws: Error if the check fails.
    func checkPages(counter: Counter, count: Int) throws -> [Bool] {
        var results = [Bool](repeating: false, count: count)
        try checkPages(counter: counter, count: count, results: &results)
        return results
    }

    /// Counts pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive).
    ///   - last: Upper bound of the identifier range (inclusive).
    ///
    /// - Returns: The number of pages in the range.
    /// - Throws: Error if the check fails.
    internal func checkPagesRange(first: RawSpan, last: RawSpan) throws -> Int {
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try checkPagesRange(
                    first: (firstBuf.baseAddress!, firstBuf.count),
                    last: (lastBuf.baseAddress!, lastBuf.count),
                )
            }
        }
    }
}

// MARK: - Foundation

import Foundation

public extension PersistentCache {
    /// Tests whether a page identified by `id` exists in the volume.
    ///
    /// - Parameter id: Page identifier.
    ///
    /// - Returns: `true` if the page exists, `false` otherwise.
    /// - Throws: Error if the check fails.
    func checkPage(id: some ContiguousBytes) throws -> Bool {
        try id.withUnsafeBytes { try checkPage(id: ($0.baseAddress!, $0.count)) }
    }

    /// Tests whether multiple pages exist in the volume.
    ///
    /// - Parameter ids: Memory region containing `count` identifiers.
    ///
    /// - Returns: Array of booleans, `true` for each page that exists.
    /// - Throws: Error if the check fails.
    func checkPages(ids: some ContiguousBytes) throws -> [Bool] {
        try ids.withUnsafeBytes { idsBuf in
            let count = idsBuf.count / configuration.idWidthInt
            var results = [Bool](repeating: false, count: count)
            try checkPages(ids: (idsBuf.baseAddress!, idsBuf.count), results: &results)
            return results
        }
    }

    /// Counts pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive).
    ///   - last: Upper bound of the identifier range (inclusive).
    ///
    /// - Returns: The number of pages in the range.
    /// - Throws: Error if the check fails.
    internal func checkPagesRange(first: some ContiguousBytes, last: some ContiguousBytes) throws -> Int {
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try checkPagesRange(
                    first: (firstBuf.baseAddress!, firstBuf.count),
                    last: (lastBuf.baseAddress!, lastBuf.count),
                )
            }
        }
    }
}

// MARK: - Foundation (Arrays)

public extension PersistentCache {
    /// Tests whether multiple pages exist in the volume.
    ///
    /// - Parameter ids: Array of page identifiers.
    ///
    /// - Returns: Array of booleans, `true` for each page that exists.
    /// - Throws: Error if the check fails.
    func checkPages(ids: [Data]) throws -> [Bool] {
        for id in ids {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
        }

        let idsSquashed = ids.squashed()

        return try idsSquashed.withUnsafeBytes { idsBuf in
            var results = [Bool](repeating: false, count: ids.count)
            try checkPages(ids: (idsBuf.baseAddress!, idsBuf.count), results: &results)
            return results
        }
    }
}

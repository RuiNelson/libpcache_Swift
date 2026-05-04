//
//  PC-Check.swift
//
//  MIT 2-Claude License.
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
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if `id` has the wrong length;
    ///   ``CheckPagesError`` on check failure;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if `ids` is not a multiple of `idWidth`;
    ///   ``CheckPagesError`` on check failure;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func checkPages(ids: CBuffer, results: UnsafeMutablePointer<Bool>) throws {
        let count = try itemCount(fromIDs: ids)
        try b_checkPages(handle: handle, count: count, ids: ids.pointer, results: results)
    }

    /// Tests whether multiple pages exist, with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - count: Number of pages to check. Must be non-negative.
    ///   - results: Caller-supplied array of at least `count` booleans.
    ///
    /// - Throws: ``InvalidCall/invalidArguments`` if `count` is negative;
    ///   ``InvalidCall/idBufferIsNotTheExpectedSize`` if the counter template width is wrong;
    ///   ``CheckPagesError/invalidArgument`` if `position` is out of bounds, the counter overflows,
    ///   or `endianness` is invalid;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func checkPages(counter: Counter, count: Int, results: UnsafeMutablePointer<Bool>) throws {
        guard count >= 0 else { throw InvalidCall.invalidArguments }
        try validateCounter(counter)
        try counter.template.withUnsafeBytes { counterBuf in
            guard let base = counterBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try b_checkPagesWithCounter(
                handle: handle,
                count: count,
                idBase: base,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianness,
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
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if either buffer has the wrong length;
    ///   ``CheckPagesError/rangeInvalidRange`` if `first > last`;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``CheckPagesError`` on check failure;
    ///   ``CommonErrors``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func checkPage(id: RawSpan) throws -> Bool {
        try id.withUnsafeBytes { idBuf in
            guard let base = idBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            return try checkPage(id: (base, idBuf.count))
        }
    }

    /// Tests whether multiple pages exist in the volume.
    ///
    /// - Parameters:
    ///   - ids: Contiguous memory region containing `count` identifiers.
    ///   - results: Destination buffer; must hold at least `count` elements,
    ///     where `count = ids.byteCount / idWidth`.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if `ids.byteCount` is not a multiple of `idWidth`;
    ///   ``InvalidCall/dataBufferIsNotTheExpectedSize`` if `results` has fewer elements than required;
    ///   ``CheckPagesError`` on check failure;
    ///   ``CommonErrors``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func checkPages(ids: RawSpan, into results: consuming MutableSpan<Bool>) throws {
        let configuration = try self.configuration
        guard ids.byteCount % configuration.idWidthInt == 0 else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        let count = ids.byteCount / configuration.idWidthInt
        guard results.count >= count else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        try ids.withUnsafeBytes { idsBuf in
            guard let idsBase = idsBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try results.withUnsafeMutableBufferPointer { resultsBuf in
                guard let resultsBase = resultsBuf.baseAddress else {
                    throw InvalidCall.dataBufferIsNotTheExpectedSize
                }
                try checkPages(ids: (idsBase, idsBuf.count), results: resultsBase)
            }
        }
    }

    /// Tests whether multiple pages exist, with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - count: Number of pages to check.
    ///
    /// - Returns: Array of booleans, `true` for each page that exists.
    /// - Throws: ``InvalidCall/invalidArguments`` if `count` is negative;
    ///   ``InvalidCall`` on invalid buffer size; ``CheckPagesError`` on check failure;
    ///   ``CommonErrors``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
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
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``CheckPagesError`` on check failure;
    ///   ``CommonErrors``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func checkPagesRange(first: RawSpan, last: RawSpan) throws -> Int {
        try first.withUnsafeBytes { firstBuf in
            guard let firstBase = firstBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            return try last.withUnsafeBytes { lastBuf in
                guard let lastBase = lastBuf.baseAddress else {
                    throw InvalidCall.idBufferIsNotTheExpectedSize
                }
                return try checkPagesRange(
                    first: (firstBase, firstBuf.count),
                    last: (lastBase, lastBuf.count),
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
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``CheckPagesError`` on check failure;
    ///   ``CommonErrors``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func checkPage(id: some ContiguousBytes) throws -> Bool {
        try id.withUnsafeBytes { idBuf in
            guard let base = idBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            return try checkPage(id: (base, idBuf.count))
        }
    }

    /// Tests whether multiple pages exist in the volume.
    ///
    /// - Parameter ids: Memory region containing `count` identifiers.
    ///
    /// - Returns: Array of booleans, `true` for each page that exists.
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``CheckPagesError`` on check failure;
    ///   ``CommonErrors``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func checkPages(ids: some ContiguousBytes) throws -> [Bool] {
        let configuration = try self.configuration
        return try ids.withUnsafeBytes { idsBuf in
            guard let idsBase = idsBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            let count = idsBuf.count / configuration.idWidthInt
            var results = [Bool](repeating: false, count: count)
            try checkPages(ids: (idsBase, idsBuf.count), results: &results)
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
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``CheckPagesError`` on check failure;
    ///   ``CommonErrors``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func checkPagesRange(first: some ContiguousBytes, last: some ContiguousBytes) throws -> Int {
        try first.withUnsafeBytes { firstBuf in
            guard let firstBase = firstBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            return try last.withUnsafeBytes { lastBuf in
                guard let lastBase = lastBuf.baseAddress else {
                    throw InvalidCall.idBufferIsNotTheExpectedSize
                }
                return try checkPagesRange(
                    first: (firstBase, firstBuf.count),
                    last: (lastBase, lastBuf.count),
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
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``CheckPagesError`` on check failure;
    ///   ``CommonErrors``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func checkPages(ids: [Data]) throws -> [Bool] {
        guard !ids.isEmpty else { return [] }
        try validateIDArray(ids)
        let idsSquashed = ids.squashed()
        return try idsSquashed.withUnsafeBytes { idsBuf in
            guard let idsBase = idsBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            var results = [Bool](repeating: false, count: ids.count)
            try checkPages(ids: (idsBase, idsBuf.count), results: &results)
            return results
        }
    }
}

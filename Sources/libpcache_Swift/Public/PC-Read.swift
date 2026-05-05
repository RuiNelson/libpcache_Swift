//
//  PC-Read.swift
//
//  MIT 2-Claude License.
//

import Foundation

// MARK: - C

public extension PersistentCache {
    /// Retrieves the page identified by `id`.
    ///
    /// - Parameters:
    ///   - id: Page identifier; must be exactly ``Configuration/idWidthInt`` bytes.
    ///   - data: Destination buffer; must be exactly ``Configuration/pageSizeInt`` bytes.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` or ``InvalidCall/dataBufferIsNotTheExpectedSize``
    ///   if either buffer has the wrong length; ``GetPagesError`` on read failure;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func getPage(id: CBuffer, data: CMutableBuffer) throws {
        try validateIDBuffer(id)
        try validateDataBuffer(data)
        try b_getPage(handle: handle, id: id.pointer, pageData: data.pointer)
    }

    /// Retrieves multiple pages in a single atomic operation.
    ///
    /// The operation is fail-fast: if any identifier is not found, the operation fails
    /// and the buffer contents are unspecified.
    ///
    /// - Parameters:
    ///   - ids: Page identifiers; must be `count * idWidth` bytes.
    ///   - data: Destination buffer; must be `count * pageSize` bytes.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` or
    ///   ``InvalidCall/numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer``
    ///   if the buffers are mismatched; ``GetPagesError`` on read failure;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func getPages(ids: CBuffer, data: CMutableBuffer) throws {
        let count = try validateMatchingCounts(ids: ids, pages: data)
        guard count > 0 else { return }
        try b_getPages(handle: handle, count: count, ids: ids.pointer, pageData: data.pointer)
    }

    /// Retrieves multiple pages with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - data: Destination buffer; must be `count * pageSize` bytes.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if the counter template width is wrong;
    ///   ``GetPagesError`` on read failure;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func getPages(counter: Counter, data: CMutableBuffer) throws {
        try validateCounter(counter)
        let count = try itemCount(fromPages: data)
        guard count > 0 else { return }

        try counter.template.withUnsafeBytes { counterBuf in
            try b_getPagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.cBuffer.pointer,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianness,
                pageData: data.pointer,
            )
        }
    }

    /// Retrieves all pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// Uses byte-by-byte comparison (SQLite BLOB ordering). Pages are returned in ascending
    /// identifier order.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive); exactly ``Configuration/idWidthInt`` bytes.
    ///   - last: Upper bound of the identifier range (inclusive); exactly ``Configuration/idWidthInt`` bytes.
    ///   - idsOut: Destination for retrieved identifiers; at least `bufferCapacity * idWidth` bytes.
    ///   - pagesOut: Destination for retrieved page data; at least `bufferCapacity * pageSize` bytes.
    ///
    /// - Returns: The number of pages retrieved.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if any buffer has the wrong length;
    ///   ``InvalidCall/numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer``
    ///   if `idsOut` and `pagesOut` have different capacities;
    ///   ``GetPagesError/rangeInvalidRange`` if `first > last`;
    ///   ``GetPagesError/rangeBufferTooSmall`` if the output buffers are too small;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func getPagesRange(
        first: CBuffer,
        last: CBuffer,
        idsOut: CMutableBuffer,
        pagesOut: CMutableBuffer,
    ) throws -> Int {
        try validateIDBuffer(first)
        try validateIDBuffer(last)
        let cfg = try configuration
        guard idsOut.count.isMultiple(of: cfg.idWidthInt) else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        guard pagesOut.count.isMultiple(of: cfg.pageSizeInt) else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        let idCapacity = idsOut.count / cfg.idWidthInt
        let pageCapacity = pagesOut.count / cfg.pageSizeInt
        guard idCapacity == pageCapacity else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }
        return try b_getPagesRange(
            handle: handle,
            first: first.pointer,
            last: last.pointer,
            idsOut: idsOut.pointer,
            pagesOut: pagesOut.pointer,
            bufferCapacity: UInt32(idCapacity),
        )
    }
}

// MARK: - Swift

public extension PersistentCache {
    /// Retrieves the page identified by `id`.
    ///
    /// - Parameters:
    ///   - id: Page identifier.
    ///   - data: Destination buffer; must be at least ``Configuration/pageSizeInt`` bytes.
    ///
    /// - Throws: ``InvalidCall/dataBufferIsNotTheExpectedSize`` if `data` is smaller than ``Configuration/pageSizeInt``
    /// bytes;
    ///   ``InvalidCall/idBufferIsNotTheExpectedSize`` on invalid id; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPage(id: RawSpan, into data: consuming MutableRawSpan) throws {
        let pageSize = try configuration.pageSizeInt
        guard data.byteCount >= pageSize else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeMutableBytes { dataBuf in
                try getPage(
                    id: idBuf.cBuffer,
                    data: (dataBuf.cMutableBuffer.pointer, pageSize),
                )
            }
        }
    }

    /// Retrieves multiple pages from contiguous memory.
    ///
    /// - Parameters:
    ///   - ids: Contiguous memory region containing `count` identifiers.
    ///   - data: Destination buffer; must be at least `count * pageSize` bytes,
    ///     where `count = ids.byteCount / idWidth`.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if `ids.byteCount` is not a multiple of `idWidth`;
    ///   ``InvalidCall/dataBufferIsNotTheExpectedSize`` if `data` is smaller than required;
    ///   ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPages(ids: RawSpan, into data: consuming MutableRawSpan) throws {
        let cfg = try configuration
        guard ids.byteCount.isMultiple(of: cfg.idWidthInt) else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        let count = ids.byteCount / cfg.idWidthInt
        let requiredBytes = count * cfg.pageSizeInt
        guard data.byteCount >= requiredBytes else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        guard count > 0 else { return }
        try ids.withUnsafeBytes { idsBuf in
            try data.withUnsafeMutableBytes { dataBuf in
                try getPages(
                    ids: idsBuf.cBuffer,
                    data: (dataBuf.cMutableBuffer.pointer, requiredBytes),
                )
            }
        }
    }

    /// Retrieves all pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// Call ``checkPagesRange(first:last:)-swift.method`` first to determine the required buffer capacity.
    /// Pages are returned in ascending identifier order.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive).
    ///   - last: Upper bound of the identifier range (inclusive).
    ///   - idsOut: Destination for retrieved identifiers; must be a multiple of `idWidth` bytes.
    ///   - pagesOut: Destination for retrieved page data; must be a multiple of `pageSize` bytes.
    ///
    /// - Returns: The number of pages retrieved.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if `idsOut` is not a multiple of `idWidth`;
    ///   ``InvalidCall/dataBufferIsNotTheExpectedSize`` if `pagesOut` is not a multiple of `pageSize`;
    ///   ``InvalidCall/numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer`` if capacities differ;
    ///   ``GetPagesError/rangeInvalidRange`` if `first > last`;
    ///   ``GetPagesError/rangeBufferTooSmall`` if the buffers are too small;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPagesRange(
        first: RawSpan,
        last: RawSpan,
        idsOut: consuming MutableRawSpan,
        pagesOut: consuming MutableRawSpan,
    ) throws -> Int {
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        try getPagesRange(
                            first: firstBuf.cBuffer,
                            last: lastBuf.cBuffer,
                            idsOut: idsBuf.cMutableBuffer,
                            pagesOut: pagesBuf.cMutableBuffer,
                        )
                    }
                }
            }
        }
    }
}

// MARK: - Foundation

public extension PersistentCache {
    /// Retrieves the page identified by `id`.
    ///
    /// - Parameter id: Page identifier.
    ///
    /// - Returns: The page data.
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPage(id: some ContiguousBytes) throws -> Data {
        var data = try Data(count: configuration.pageSizeInt)
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeMutableBytes { dataBuf in
                try getPage(id: idBuf.cBuffer, data: dataBuf.cMutableBuffer)
            }
        }
        return data
    }

    /// Retrieves multiple pages from contiguous memory.
    ///
    /// - Parameter ids: Memory region containing `count` identifiers.
    ///
    /// - Returns: `Data` containing all retrieved pages concatenated.
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPages(ids: some ContiguousBytes) throws -> Data {
        let cfg = try configuration
        return try ids.withUnsafeBytes { idsBuf in
            guard idsBuf.count.isMultiple(of: cfg.idWidthInt) else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            let count = idsBuf.count / cfg.idWidthInt
            guard count > 0 else { return Data() }
            var data = Data(count: count * cfg.pageSizeInt)
            try data.withUnsafeMutableBytes { dataBuf in
                try getPages(ids: idsBuf.cBuffer, data: dataBuf.cMutableBuffer)
            }
            return data
        }
    }

    /// Retrieves multiple pages with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - count: Number of pages to retrieve. Must be non-negative.
    ///
    /// - Returns: `Data` containing all retrieved pages concatenated.
    /// - Throws: ``InvalidCall/invalidArguments`` if `count` is negative;
    ///   ``InvalidCall`` on invalid buffer size; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPages(counter: Counter, count: Int) throws -> Data {
        guard count >= 0 else { throw InvalidCall.invalidArguments }
        guard count > 0 else { return Data() }
        var data = try Data(count: count * configuration.pageSizeInt)
        try data.withUnsafeMutableBytes { dataBuf in
            try getPages(counter: counter, data: dataBuf.cMutableBuffer)
        }
        return data
    }

    /// Retrieves all pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive).
    ///   - last: Upper bound of the identifier range (inclusive).
    ///
    /// - Returns: Tuple containing `(ids, pages)`.
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPagesRange(first: some ContiguousBytes, last: some ContiguousBytes) throws -> (ids: Data, pages: Data) {
        let bufferCapacity = try checkPagesRange(first: first, last: last)
        guard bufferCapacity > 0 else { return (Data(), Data()) }

        let cfg = try configuration
        var idsOut = Data(count: bufferCapacity * cfg.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * cfg.pageSizeInt)
        let actualCount = try rawGetPagesRange(first: first, last: last, idsOut: &idsOut, pagesOut: &pagesOut)

        // Trim to the actual returned size; the C call may return fewer pages than the cap reported by checkPagesRange.
        idsOut.removeSubrange(actualCount * cfg.idWidthInt ..< idsOut.count)
        pagesOut.removeSubrange(actualCount * cfg.pageSizeInt ..< pagesOut.count)
        return (idsOut, pagesOut)
    }

    /// Retrieves all pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive).
    ///   - last: Upper bound of the identifier range (inclusive).
    ///
    /// - Returns: Array of `(id, page)` tuples, one per retrieved page.
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPagesRange(first: some ContiguousBytes, last: some ContiguousBytes) throws -> [(id: Data, page: Data)] {
        let bufferCapacity = try checkPagesRange(first: first, last: last)
        guard bufferCapacity > 0 else { return [] }

        let cfg = try configuration
        var idsOut = Data(count: bufferCapacity * cfg.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * cfg.pageSizeInt)
        let actualCount = try rawGetPagesRange(first: first, last: last, idsOut: &idsOut, pagesOut: &pagesOut)

        var result = [(id: Data, page: Data)]()
        result.reserveCapacity(actualCount)
        for i in 0 ..< actualCount {
            let idStart = i * cfg.idWidthInt
            let pageStart = i * cfg.pageSizeInt
            result.append((
                id: idsOut[idStart ..< idStart + cfg.idWidthInt],
                page: pagesOut[pageStart ..< pageStart + cfg.pageSizeInt],
            ))
        }
        return result
    }

    /// Shared implementation behind the two ``getPagesRange(first:last:)`` Foundation overloads.
    private func rawGetPagesRange(
        first: some ContiguousBytes,
        last: some ContiguousBytes,
        idsOut: inout Data,
        pagesOut: inout Data,
    ) throws -> Int {
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        try getPagesRange(
                            first: firstBuf.cBuffer,
                            last: lastBuf.cBuffer,
                            idsOut: idsBuf.cMutableBuffer,
                            pagesOut: pagesBuf.cMutableBuffer,
                        )
                    }
                }
            }
        }
    }
}

// MARK: - Foundation (Arrays)

public extension PersistentCache {
    /// Retrieves multiple pages from an array of identifiers.
    ///
    /// - Parameter ids: Array of page identifiers.
    ///
    /// - Returns: `Data` containing all retrieved pages concatenated.
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPages(ids: [Data]) throws -> Data {
        guard !ids.isEmpty else { return Data() }
        try validateIDArray(ids)
        let cfg = try configuration
        let idsSquashed = ids.squashed()
        var data = Data(count: ids.count * cfg.pageSizeInt)
        try idsSquashed.withUnsafeBytes { idsBuf in
            try data.withUnsafeMutableBytes { dataBuf in
                try getPages(ids: idsBuf.cBuffer, data: dataBuf.cMutableBuffer)
            }
        }
        return data
    }

    /// Retrieves multiple pages from an array of identifiers.
    ///
    /// - Parameter ids: Array of page identifiers.
    ///
    /// - Returns: Array of `Data` objects, one per page.
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPages(ids: [Data]) throws -> [Data] {
        guard !ids.isEmpty else { return [] }
        let pageSize = try configuration.pageSizeInt
        let dataSquashed: Data = try getPages(ids: ids)
        return (0 ..< ids.count).map { i in
            let start = i * pageSize
            return dataSquashed[start ..< start + pageSize]
        }
    }
}

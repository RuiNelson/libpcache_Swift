//
//  PC-Read.swift
//
//  MIT 2-Claude License.
//

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
    func getPages(
        counter: Counter,
        data: CMutableBuffer,
    ) throws {
        try validateCounter(counter)
        let count = try itemCount(fromPages: data)

        try counter.template.withUnsafeBytes { counterBuf in
            guard let base = counterBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try b_getPagesWithCounter(
                handle: handle,
                count: count,
                idBase: base,
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
        let configuration = try self.configuration
        guard first.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        guard last.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        guard idsOut.count % configuration.idWidthInt == 0 else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        guard pagesOut.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        let idCapacity = idsOut.count / configuration.idWidthInt
        let pageCapacity = pagesOut.count / configuration.pageSizeInt
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

// MARK: - Swift/Foundation

import Foundation

public extension PersistentCache {
    /// Retrieves the page identified by `id`.
    ///
    /// - Parameter id: Page identifier.
    ///
    /// - Returns: The page data, exactly ``Configuration/pageSizeInt`` bytes.
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPage(id: RawSpan) throws -> [UInt8] {
        let configuration = try self.configuration
        var data = [UInt8](repeating: 0, count: configuration.pageSizeInt)
        try id.withUnsafeBytes { idBuf in
            guard let idBase = idBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try data.withUnsafeMutableBytes { dataBuf in
                guard let dataBase = dataBuf.baseAddress else {
                    throw InvalidCall.dataBufferIsNotTheExpectedSize
                }
                try getPage(
                    id: (idBase, idBuf.count),
                    data: (dataBase, dataBuf.count),
                )
            }
        }
        return data
    }

    /// Retrieves multiple pages from contiguous memory.
    ///
    /// - Parameter ids: Contiguous memory region containing `count` identifiers.
    ///
    /// - Returns: `Data` containing all retrieved pages concatenated.
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPages(ids: RawSpan) throws -> Data {
        let configuration = try self.configuration
        return try ids.withUnsafeBytes { idsBuf in
            guard let idsBase = idsBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            let count = idsBuf.count / configuration.idWidthInt
            var data = Data(count: count * configuration.pageSizeInt)
            try data.withUnsafeMutableBytes { dataBuf in
                guard let dataBase = dataBuf.baseAddress else {
                    throw InvalidCall.dataBufferIsNotTheExpectedSize
                }
                try getPages(
                    ids: (idsBase, idsBuf.count),
                    data: (dataBase, dataBuf.count),
                )
            }
            return data
        }
    }

    /// Retrieves all pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive).
    ///   - last: Upper bound of the identifier range (inclusive).
    ///
    /// - Returns: Tuple containing `(ids, pages)` where `ids` is the identifiers and `pages` is the data.
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``GetPagesError`` on read failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func getPagesRange(first: RawSpan, last: RawSpan) throws -> (ids: Data, pages: Data) {
        let bufferCapacity = try checkPagesRange(first: first, last: last)
        guard bufferCapacity > 0 else { return (Data(), Data()) }

        let configuration = try self.configuration
        var idsOut = Data(count: bufferCapacity * configuration.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * configuration.pageSizeInt)
        var actualCount = 0

        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        guard let firstBase = firstBuf.baseAddress,
                              let lastBase = lastBuf.baseAddress,
                              let idsBase = idsBuf.baseAddress,
                              let pagesBase = pagesBuf.baseAddress else { throw InvalidCall.idBufferIsNotTheExpectedSize }
                        actualCount = try getPagesRange(
                            first: (firstBase, firstBuf.count),
                            last: (lastBase, lastBuf.count),
                            idsOut: (idsBase, idsBuf.count),
                            pagesOut: (pagesBase, pagesBuf.count),
                        )
                    }
                }
            }
        }

        return (
            idsOut.prefix(actualCount * configuration.idWidthInt),
            pagesOut.prefix(actualCount * configuration.pageSizeInt),
        )
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
        let configuration = try self.configuration
        var data = Data(count: configuration.pageSizeInt)
        try id.withUnsafeBytes { idBuf in
            guard let idBase = idBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try data.withUnsafeMutableBytes { dataBuf in
                guard let dataBase = dataBuf.baseAddress else {
                    throw InvalidCall.dataBufferIsNotTheExpectedSize
                }
                try getPage(
                    id: (idBase, idBuf.count),
                    data: (dataBase, dataBuf.count),
                )
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
        let configuration = try self.configuration
        return try ids.withUnsafeBytes { idsBuf in
            guard let idsBase = idsBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            let count = idsBuf.count / configuration.idWidthInt
            var data = Data(count: count * configuration.pageSizeInt)
            try data.withUnsafeMutableBytes { dataBuf in
                guard let dataBase = dataBuf.baseAddress else {
                    throw InvalidCall.dataBufferIsNotTheExpectedSize
                }
                try getPages(
                    ids: (idsBase, idsBuf.count),
                    data: (dataBase, dataBuf.count),
                )
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
        let configuration = try self.configuration
        var data = Data(count: count * configuration.pageSizeInt)
        try data.withUnsafeMutableBytes { dataBuf in
            guard let dataBase = dataBuf.baseAddress else {
                throw InvalidCall.dataBufferIsNotTheExpectedSize
            }
            try getPages(
                counter: counter,
                data: (dataBase, dataBuf.count),
            )
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

        let configuration = try self.configuration
        var idsOut = Data(count: bufferCapacity * configuration.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * configuration.pageSizeInt)
        var actualCount = 0

        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        guard let firstBase = firstBuf.baseAddress,
                              let lastBase = lastBuf.baseAddress,
                              let idsBase = idsBuf.baseAddress,
                              let pagesBase = pagesBuf.baseAddress else { throw InvalidCall.idBufferIsNotTheExpectedSize }
                        actualCount = try getPagesRange(
                            first: (firstBase, firstBuf.count),
                            last: (lastBase, lastBuf.count),
                            idsOut: (idsBase, idsBuf.count),
                            pagesOut: (pagesBase, pagesBuf.count),
                        )
                    }
                }
            }
        }

        return (
            idsOut.prefix(actualCount * configuration.idWidthInt),
            pagesOut.prefix(actualCount * configuration.pageSizeInt),
        )
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

        let configuration = try self.configuration
        var idsOut = Data(count: bufferCapacity * configuration.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * configuration.pageSizeInt)
        var actualCount = 0

        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        guard let firstBase = firstBuf.baseAddress,
                              let lastBase = lastBuf.baseAddress,
                              let idsBase = idsBuf.baseAddress,
                              let pagesBase = pagesBuf.baseAddress else { throw InvalidCall.idBufferIsNotTheExpectedSize }
                        actualCount = try getPagesRange(
                            first: (firstBase, firstBuf.count),
                            last: (lastBase, lastBuf.count),
                            idsOut: (idsBase, idsBuf.count),
                            pagesOut: (pagesBase, pagesBuf.count),
                        )
                    }
                }
            }
        }

        var result = [(id: Data, page: Data)]()
        result.reserveCapacity(actualCount)
        for i in 0 ..< actualCount {
            let idStart = i * configuration.idWidthInt
            let pageStart = i * configuration.pageSizeInt
            result.append((
                id: idsOut[idStart ..< idStart + configuration.idWidthInt],
                page: pagesOut[pageStart ..< pageStart + configuration.pageSizeInt],
            ))
        }
        return result
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
        let configuration = try self.configuration
        let idsSquashed = ids.squashed()
        return try idsSquashed.withUnsafeBytes { idsBuf in
            guard let idsBase = idsBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            var data = Data(count: ids.count * configuration.pageSizeInt)
            try data.withUnsafeMutableBytes { dataBuf in
                guard let dataBase = dataBuf.baseAddress else {
                    throw InvalidCall.dataBufferIsNotTheExpectedSize
                }
                try getPages(
                    ids: (idsBase, idsBuf.count),
                    data: (dataBase, dataBuf.count),
                )
            }
            return data
        }
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
        let configuration = try self.configuration
        let dataSquashed: Data = try getPages(ids: ids)
        return (0 ..< ids.count).map { i in
            let start = i * configuration.pageSizeInt
            return dataSquashed[start ..< start + configuration.pageSizeInt]
        }
    }
}

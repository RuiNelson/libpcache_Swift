//
//  PC-Read.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

// MARK: - C

public extension PersistentCache {
    /// Retrieves the page identified by `id`.
    ///
    /// - Parameters:
    ///   - id: Page identifier; must be exactly ``Configuration/idWidthInt`` bytes.
    ///   - data: Destination buffer; must be at least ``Configuration/pageSizeInt`` bytes.
    ///
    /// - Throws: ``GetPagesError`` if the page is not found or an error occurs.
    func getPage(id: CBuffer, data: CMutableBuffer) throws {
        guard id.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

        guard data.count == configuration.pageSizeInt else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }

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
    /// - Throws: ``GetPagesError`` if any page is not found or an error occurs.
    func getPages(ids: CBuffer, data: CMutableBuffer) throws {
        guard ids.count % configuration.idWidthInt == 0 else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

        guard data.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }

        let count1 = ids.count / configuration.idWidthInt
        let count2 = data.count / configuration.pageSizeInt

        guard count1 == count2 else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }

        let count = count1 // == count2

        try b_getPages(handle: handle, count: count, ids: ids.pointer, pageData: data.pointer)
    }

    /// Retrieves multiple pages with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - data: Destination buffer; must be `count * pageSize` bytes.
    ///
    /// - Throws: ``GetPagesError`` if any computed identifier is not found or an error occurs.
    func getPages(
        counter: Counter,
        data: CMutableBuffer,
    ) throws {
        guard counter.templateWidth == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

        guard data.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }

        let count = data.count / configuration.pageSizeInt

        try counter.template.withUnsafeBytes { counterBuf in
            try b_getPagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.baseAddress!,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianess,
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
    /// - Throws: ``GetPagesError`` if `first > last`, the buffer is too small, or another error occurs.
    func getPagesRange(
        first: CBuffer,
        last: CBuffer,
        idsOut: CMutableBuffer,
        pagesOut: CMutableBuffer,
    ) throws -> Int {
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

        let bufferCapacity = UInt32(idCapacity)

        return try b_getPagesRange(
            handle: handle,
            first: first.pointer,
            last: last.pointer,
            idsOut: idsOut.pointer,
            pagesOut: pagesOut.pointer,
            bufferCapacity: bufferCapacity,
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
    /// - Throws: ``GetPagesError`` if the page is not found or an error occurs.
    func getPage(id: RawSpan) throws -> [UInt8] {
        var data = [UInt8](repeating: 0, count: configuration.pageSizeInt)
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeMutableBytes { dataBuf in
                try getPage(
                    id: (idBuf.baseAddress!, idBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
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
    /// - Throws: ``GetPagesError`` if any page is not found or an error occurs.
    func getPages(ids: RawSpan) throws -> Data {
        try ids.withUnsafeBytes { idsBuf in
            let count = idsBuf.count / configuration.idWidthInt
            var data = Data(count: count * configuration.pageSizeInt)
            try data.withUnsafeMutableBytes { dataBuf in
                try getPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
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
    /// - Throws: ``GetPagesError`` if an error occurs.
    func getPagesRange(first: RawSpan, last: RawSpan) throws -> (ids: Data, pages: Data) {
        let bufferCapacity = try checkPagesRange(first: first, last: last)

        guard bufferCapacity > 0 else {
            return (Data(), Data())
        }

        var idsOut = Data(count: bufferCapacity * configuration.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * configuration.pageSizeInt)

        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        _ = try getPagesRange(
                            first: (firstBuf.baseAddress!, firstBuf.count),
                            last: (lastBuf.baseAddress!, lastBuf.count),
                            idsOut: (idsBuf.baseAddress!, idsBuf.count),
                            pagesOut: (pagesBuf.baseAddress!, pagesBuf.count),
                        )
                    }
                }
            }
        }

        return (idsOut, pagesOut)
    }
}

// MARK: - Foundation

public extension PersistentCache {
    /// Retrieves the page identified by `id`.
    ///
    /// - Parameter id: Page identifier.
    ///
    /// - Returns: The page data.
    /// - Throws: ``GetPagesError`` if the page is not found or an error occurs.
    func getPage(id: some ContiguousBytes) throws -> Data {
        var data = Data(count: configuration.pageSizeInt)
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeMutableBytes { dataBuf in
                try getPage(
                    id: (idBuf.baseAddress!, idBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
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
    /// - Throws: ``GetPagesError`` if any page is not found or an error occurs.
    func getPages(ids: some ContiguousBytes) throws -> Data {
        try ids.withUnsafeBytes { idsBuf in
            let count = idsBuf.count / configuration.idWidthInt
            var data = Data(count: count * configuration.pageSizeInt)
            try data.withUnsafeMutableBytes { dataBuf in
                try getPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                )
            }
            return data
        }
    }

    /// Retrieves multiple pages with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - count: Number of pages to retrieve.
    ///
    /// - Returns: `Data` containing all retrieved pages concatenated.
    /// - Throws: ``GetPagesError`` if any computed identifier is not found or an error occurs.
    func getPages(counter: Counter, count: Int) throws -> Data {
        var data = Data(count: count * configuration.pageSizeInt)
        try data.withUnsafeMutableBytes { dataBuf in
            try getPages(
                counter: counter,
                data: (dataBuf.baseAddress!, dataBuf.count),
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
    /// - Throws: ``GetPagesError`` if an error occurs.
    func getPagesRange(first: some ContiguousBytes, last: some ContiguousBytes) throws -> (ids: Data, pages: Data) {
        let bufferCapacity = try checkPagesRange(first: first, last: last)

        guard bufferCapacity > 0 else {
            return (Data(), Data())
        }

        var idsOut = Data(count: bufferCapacity * configuration.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * configuration.pageSizeInt)

        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        _ = try getPagesRange(
                            first: (firstBuf.baseAddress!, firstBuf.count),
                            last: (lastBuf.baseAddress!, lastBuf.count),
                            idsOut: (idsBuf.baseAddress!, idsBuf.count),
                            pagesOut: (pagesBuf.baseAddress!, pagesBuf.count),
                        )
                    }
                }
            }
        }

        return (idsOut, pagesOut)
    }

    /// Retrieves all pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive).
    ///   - last: Upper bound of the identifier range (inclusive).
    ///
    /// - Returns: Array of `(id, page)` tuples, one per retrieved page.
    /// - Throws: ``GetPagesError`` if an error occurs.
    func getPagesRange(first: some ContiguousBytes, last: some ContiguousBytes) throws -> [(id: Data, page: Data)] {
        let bufferCapacity = try checkPagesRange(first: first, last: last)

        guard bufferCapacity > 0 else {
            return []
        }

        var idsOut = Data(count: bufferCapacity * configuration.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * configuration.pageSizeInt)

        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        _ = try getPagesRange(
                            first: (firstBuf.baseAddress!, firstBuf.count),
                            last: (lastBuf.baseAddress!, lastBuf.count),
                            idsOut: (idsBuf.baseAddress!, idsBuf.count),
                            pagesOut: (pagesBuf.baseAddress!, pagesBuf.count),
                        )
                    }
                }
            }
        }

        // Split into tuples
        var result = [(id: Data, page: Data)]()
        result.reserveCapacity(bufferCapacity)
        for i in 0 ..< bufferCapacity {
            let idStart = i * configuration.idWidthInt
            let idEnd = idStart + configuration.idWidthInt
            let pageStart = i * configuration.pageSizeInt
            let pageEnd = pageStart + configuration.pageSizeInt
            result.append((id: idsOut[idStart ..< idEnd], page: pagesOut[pageStart ..< pageEnd]))
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
    /// - Throws: ``GetPagesError`` if any page is not found or an error occurs.
    func getPages(ids: [Data]) throws -> Data {
        for id in ids {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
        }

        let idsSquashed = ids.squashed()

        return try idsSquashed.withUnsafeBytes { idsBuf in
            var data = Data(count: ids.count * configuration.pageSizeInt)
            try data.withUnsafeMutableBytes { dataBuf in
                try getPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
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
    /// - Throws: ``GetPagesError`` if any page is not found or an error occurs.
    func getPages(ids: [Data]) throws -> [Data] {
        let dataSquashed: Data = try getPages(ids: ids)
        return (0 ..< ids.count).map { i in
            let start = i * configuration.pageSizeInt
            return dataSquashed[start ..< start + configuration.pageSizeInt]
        }
    }
}

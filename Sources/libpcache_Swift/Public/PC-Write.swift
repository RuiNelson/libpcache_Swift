//
//  PC-Write.swift
//
//  MIT 2-Claude License.
//

import Foundation

// MARK: - C

public extension PersistentCache {
    /// Stores a single page identified by `id`.
    ///
    /// On FIFO volumes, pages beyond `maxPages` are evicted automatically.
    /// On FIXED volumes, writes beyond capacity fail with ``PutPagesError/capacityExceeded``.
    ///
    /// - Parameters:
    ///   - id: Page identifier; must be exactly ``Configuration/idWidthInt`` bytes.
    ///   - data: Page content; must be exactly ``Configuration/pageSizeInt`` bytes.
    ///   - failIfExists: If `true`, verify that no page with the same identifier already exists before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` or ``InvalidCall/dataBufferIsNotTheExpectedSize``
    ///   if either buffer has the wrong length; ``PutPagesError`` on write failure;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func putPage(
        id: CBuffer,
        data: CBuffer,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try validateIDBuffer(id)
        try validateDataBuffer(data)
        try b_putPage(
            handle: handle,
            id: id.pointer,
            pageData: data.pointer,
            failIfExists: failIfExists,
            durable: durable,
        )
    }

    /// Stores multiple pages in a single atomic operation.
    ///
    /// The operation is atomic: either all pages are written, or none are.
    /// On FIFO volumes, pages beyond `maxPages` are evicted automatically.
    /// On FIXED volumes, writes beyond capacity fail with ``PutPagesError/capacityExceeded``.
    ///
    /// - Parameters:
    ///   - ids: Page identifiers; must be `count * idWidth` bytes, where `count = ids.count / idWidth`.
    ///   - data: Page contents; must be `count * pageSize` bytes.
    ///   - failIfExists: If `true`, verify that none of the identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` or
    ///   ``InvalidCall/numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer``
    ///   if the buffers are mismatched; ``PutPagesError`` on write failure;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func putPages(
        ids: CBuffer,
        data: CBuffer,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        let count = try validateMatchingCounts(ids: ids, pages: data)
        guard count > 0 else { return }
        try b_putPages(
            handle: handle,
            count: count,
            ids: ids.pointer,
            pagesData: data.pointer,
            failIfExists: failIfExists,
            durable: durable,
        )
    }

    /// Stores multiple pages with identifiers computed automatically from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - data: Page contents; must be `count * pageSize` bytes.
    ///   - failIfExists: If `true`, verify that none of the computed identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if the counter template width is wrong;
    ///   ``PutPagesError`` on write failure;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func putPages(
        counter: Counter,
        data: CBuffer,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try validateCounter(counter)
        let count = try itemCount(fromPages: data)
        guard count > 0 else { return }

        try counter.template.withUnsafeBytes { counterBuf in
            try b_putPagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.cBuffer.pointer,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianness,
                pagesData: data.pointer,
                failIfExists: failIfExists,
                durable: durable,
            )
        }
    }
}

// MARK: - Swift

public extension PersistentCache {
    /// Stores a single page identified by `id`.
    ///
    /// - Parameters:
    ///   - id: Page identifier; must be exactly ``Configuration/idWidthInt`` bytes.
    ///   - data: Page content; must be exactly ``Configuration/pageSizeInt`` bytes.
    ///   - failIfExists: If `true`, verify that no page with the same identifier already exists before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``PutPagesError`` on write failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func putPage(
        id: RawSpan,
        data: RawSpan,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPage(
                    id: idBuf.cBuffer,
                    data: dataBuf.cBuffer,
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }

    /// Stores multiple pages in a single atomic operation.
    ///
    /// - Parameters:
    ///   - ids: Contiguous memory region containing `count` identifiers of ``Configuration/idWidthInt`` bytes each.
    ///   - data: Contiguous memory region containing `count` pages of ``Configuration/pageSizeInt`` bytes each.
    ///   - failIfExists: If `true`, verify that none of the identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``PutPagesError`` on write failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func putPages(
        ids: RawSpan,
        data: RawSpan,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPages(
                    ids: idsBuf.cBuffer,
                    data: dataBuf.cBuffer,
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }

    /// Stores multiple pages with identifiers computed automatically from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - data: Page content.
    ///   - failIfExists: If `true`, verify that none of the computed identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``PutPagesError`` on write failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func putPages(counter: Counter, data: RawSpan, failIfExists: Bool = false, durable: Bool = true) throws {
        try data.withUnsafeBytes { dataBuf in
            try putPages(
                counter: counter,
                data: dataBuf.cBuffer,
                failIfExists: failIfExists,
                durable: durable,
            )
        }
    }
}

// MARK: - Foundation

public extension PersistentCache {
    /// Stores a single page identified by `id`.
    ///
    /// - Parameters:
    ///   - id: Page identifier; must be exactly ``Configuration/idWidthInt`` bytes.
    ///   - data: Page content; must be exactly ``Configuration/pageSizeInt`` bytes.
    ///   - failIfExists: If `true`, verify that no page with the same identifier already exists before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``PutPagesError`` on write failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func putPage(
        id: some ContiguousBytes,
        data: some ContiguousBytes,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPage(
                    id: idBuf.cBuffer,
                    data: dataBuf.cBuffer,
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }

    /// Stores multiple pages in a single atomic operation.
    ///
    /// - Parameters:
    ///   - ids: Memory region containing `count` identifiers.
    ///   - data: Memory region containing `count` pages.
    ///   - failIfExists: If `true`, verify that none of the identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``PutPagesError`` on write failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func putPages(
        ids: some ContiguousBytes,
        data: some ContiguousBytes,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPages(
                    ids: idsBuf.cBuffer,
                    data: dataBuf.cBuffer,
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }

    /// Stores multiple pages with identifiers computed automatically from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - data: Page content.
    ///   - failIfExists: If `true`, verify that none of the computed identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``PutPagesError`` on write failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func putPages(
        counter: Counter,
        data: some ContiguousBytes,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try data.withUnsafeBytes { dataBuf in
            try putPages(
                counter: counter,
                data: dataBuf.cBuffer,
                failIfExists: failIfExists,
                durable: durable,
            )
        }
    }
}

// MARK: - Foundation (Arrays)

extension [Data] {
    /// Concatenates the array's contents into a single contiguous `Data`.
    ///
    /// Returns an empty `Data` when the receiver is empty or all elements are empty.
    func squashed() -> Data {
        let totalSize = reduce(0) { $0 + $1.count }
        guard totalSize > 0 else { return Data() }

        var result = Data(count: totalSize)
        result.withUnsafeMutableBytes { buffer in
            // totalSize > 0 implies a non-nil baseAddress.
            let base = buffer.baseAddress!.assumingMemoryBound(to: UInt8.self)
            var offset = 0
            for chunk in self {
                chunk.copyBytes(to: base.advanced(by: offset), count: chunk.count)
                offset += chunk.count
            }
        }
        return result
    }
}

public extension PersistentCache {
    /// Stores multiple pages from separate `Data` objects.
    ///
    /// - Parameters:
    ///   - ids: Array of page identifiers, each exactly ``Configuration/idWidthInt`` bytes.
    ///   - data: Array of page contents, each exactly ``Configuration/pageSizeInt`` bytes.
    ///   - failIfExists: If `true`, verify that none of the identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size or mismatched array counts;
    ///   ``PutPagesError`` on write failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func putPages(ids: [Data], data: [Data], failIfExists: Bool = false, durable: Bool = true) throws {
        guard ids.count == data.count else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }
        guard !ids.isEmpty else { return }

        try validateIDArray(ids)
        try validatePageArray(data)

        try putPages(
            ids: ids.squashed(),
            data: data.squashed(),
            failIfExists: failIfExists,
            durable: durable,
        )
    }

    /// Stores multiple pages with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - data: Array of page contents, each exactly ``Configuration/pageSizeInt`` bytes.
    ///   - failIfExists: If `true`, verify that none of the computed identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``PutPagesError`` on write failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func putPages(counter: Counter, data: [Data], failIfExists: Bool = false, durable: Bool = true) throws {
        guard !data.isEmpty else { return }

        try validateCounter(counter)
        try validatePageArray(data)

        try putPages(
            counter: counter,
            data: data.squashed(),
            failIfExists: failIfExists,
            durable: durable,
        )
    }
}

// MARK: - Foundation (Tuples)

public extension PersistentCache {
    /// Stores multiple pages from an array of `(id, data)` tuples.
    ///
    /// - Parameters:
    ///   - pages: Array of `(id, data)` tuples.
    ///   - failIfExists: If `true`, verify that none of the identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size;
    ///   ``PutPagesError`` on write failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func putPages(
        pages: [(id: Data, data: Data)],
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        guard !pages.isEmpty else { return }

        try putPages(
            ids: pages.map(\.id),
            data: pages.map(\.data),
            failIfExists: failIfExists,
            durable: durable,
        )
    }
}

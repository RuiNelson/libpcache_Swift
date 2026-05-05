//
//  PC-Delete.swift
//
//  MIT 2-Claude License.
//

import Foundation

// MARK: - C

public extension PersistentCache {
    /// Deletes the page identified by `id` from the volume.
    ///
    /// If no page with `id` exists, the call is a silent no-op.
    ///
    /// - Parameters:
    ///   - id: Page identifier; must be exactly ``Configuration/idWidthInt`` bytes.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if `id` has the wrong length;
    ///   ``DeletePagesError`` on delete failure;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func deletePage(
        id: CBuffer,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try validateIDBuffer(id)
        try b_deletePage(
            handle: handle,
            id: id.pointer,
            wipeDataFile: wipe,
            durable: durable,
        )
    }

    /// Deletes multiple pages from the volume.
    ///
    /// Identifiers that are not present in the volume are silently skipped.
    /// The deletions of the matching pages are committed atomically in a single transaction.
    ///
    /// - Parameters:
    ///   - ids: Page identifiers; must be `count * idWidth` bytes.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if `ids` is not a multiple of `idWidth`;
    ///   ``DeletePagesError`` on delete failure;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func deletePages(
        ids: CBuffer,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        let count = try itemCount(fromIDs: ids)
        guard count > 0 else { return }
        try b_deletePages(
            handle: handle,
            count: count,
            ids: ids.pointer,
            wipeDataFile: wipe,
            durable: durable,
        )
    }

    /// Deletes multiple pages with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - count: Number of pages to delete. Must be non-negative.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall/invalidArguments`` if `count` is negative;
    ///   ``InvalidCall/idBufferIsNotTheExpectedSize`` if the counter template width is wrong;
    ///   ``DeletePagesError/invalidArgument`` if `position` is out of bounds, the counter overflows,
    ///   or `endianness` is invalid;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func deletePages(
        counter: Counter,
        count: Int,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        guard count >= 0 else { throw InvalidCall.invalidArguments }
        try validateCounter(counter)
        guard count > 0 else { return }
        try counter.template.withUnsafeBytes { counterBuf in
            try b_deletePagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.cBuffer.pointer,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianness,
                wipeDataFile: wipe,
                durable: durable,
            )
        }
    }

    /// Deletes all pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// Uses byte-by-byte comparison (SQLite BLOB ordering). An empty match is not an error.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive); exactly ``Configuration/idWidthInt`` bytes.
    ///   - last: Upper bound of the identifier range (inclusive); exactly ``Configuration/idWidthInt`` bytes.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if either buffer has the wrong length;
    ///   ``DeletePagesError/invalidRange`` if `first > last`;
    ///   ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func deletePagesRange(
        first: CBuffer,
        last: CBuffer,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try validateIDBuffer(first)
        try validateIDBuffer(last)
        try b_deletePagesRange(
            handle: handle,
            first: first.pointer,
            last: last.pointer,
            wipeDataFile: wipe,
            durable: durable,
        )
    }
}

// MARK: - Swift

public extension PersistentCache {
    /// Deletes the page identified by `id` from the volume.
    ///
    /// - Parameters:
    ///   - id: Page identifier.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``DeletePagesError`` on delete failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func deletePage(
        id: RawSpan,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            try deletePage(id: idBuf.cBuffer, wipe: wipe, durable: durable)
        }
    }

    /// Deletes multiple pages from the volume.
    ///
    /// - Parameters:
    ///   - ids: Contiguous memory region containing `count` identifiers.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``DeletePagesError`` on delete failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func deletePages(
        ids: RawSpan,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            try deletePages(ids: idsBuf.cBuffer, wipe: wipe, durable: durable)
        }
    }

    /// Deletes all pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive).
    ///   - last: Upper bound of the identifier range (inclusive).
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``DeletePagesError`` on delete failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func deletePagesRange(
        first: RawSpan,
        last: RawSpan,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try deletePagesRange(
                    first: firstBuf.cBuffer,
                    last: lastBuf.cBuffer,
                    wipe: wipe,
                    durable: durable,
                )
            }
        }
    }
}

// MARK: - Foundation

public extension PersistentCache {
    /// Deletes the page identified by `id` from the volume.
    ///
    /// - Parameters:
    ///   - id: Page identifier.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``DeletePagesError`` on delete failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func deletePage(
        id: Data,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            try deletePage(id: idBuf.cBuffer, wipe: wipe, durable: durable)
        }
    }

    /// Deletes multiple pages from the volume.
    ///
    /// - Parameters:
    ///   - ids: `Data` containing `count` identifiers concatenated.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``DeletePagesError`` on delete failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func deletePages(
        ids: Data,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            try deletePages(ids: idsBuf.cBuffer, wipe: wipe, durable: durable)
        }
    }

    /// Deletes all pages whose identifier falls within the closed interval `[first, last]`.
    ///
    /// - Parameters:
    ///   - first: Lower bound of the identifier range (inclusive).
    ///   - last: Upper bound of the identifier range (inclusive).
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``DeletePagesError`` on delete failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func deletePagesRange(
        first: Data,
        last: Data,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try deletePagesRange(
                    first: firstBuf.cBuffer,
                    last: lastBuf.cBuffer,
                    wipe: wipe,
                    durable: durable,
                )
            }
        }
    }
}

// MARK: - Foundation (Arrays)

public extension PersistentCache {
    /// Deletes multiple pages from the volume.
    ///
    /// - Parameters:
    ///   - ids: Array of page identifiers.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall`` on invalid buffer size; ``DeletePagesError`` on delete failure;
    ///   ``CommonErrors``, ``POSIXError``, ``SQLiteError``, or ``UnknownLibPCacheError`` from the underlying operation.
    func deletePages(
        ids: [Data],
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        guard !ids.isEmpty else { return }
        try validateIDArray(ids)
        try deletePages(ids: ids.squashed(), wipe: wipe, durable: durable)
    }
}

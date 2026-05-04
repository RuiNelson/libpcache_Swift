//
//  PC-Delete.swift
//
//  MIT 2-Claude License.
//

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
    ///   ``POSIXError`` or ``SQLiteError`` on I/O or database failure.
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
    ///   ``POSIXError`` or ``SQLiteError`` on I/O or database failure.
    func deletePages(
        ids: CBuffer,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        let count = try itemCount(fromIDs: ids)
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
    ///   or `endianness` is invalid.
    func deletePages(
        counter: Counter,
        count: Int,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        guard count >= 0 else { throw InvalidCall.invalidArguments }
        try validateCounter(counter)
        try counter.template.withUnsafeBytes { counterBuf in
            guard let base = counterBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try b_deletePagesWithCounter(
                handle: handle,
                count: count,
                idBase: base,
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
    ///   ``DeletePagesError/invalidRange`` if `first > last`.
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
    /// - Throws: Error if the delete fails.
    func deletePage(
        id: RawSpan,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            guard let base = idBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try deletePage(id: (base, idBuf.count), wipe: wipe, durable: durable)
        }
    }

    /// Deletes multiple pages from the volume.
    ///
    /// - Parameters:
    ///   - ids: Contiguous memory region containing `count` identifiers.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: Error if the delete fails.
    func deletePages(
        ids: RawSpan,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            guard let idsBase = idsBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try deletePages(ids: (idsBase, idsBuf.count), wipe: wipe, durable: durable)
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
    /// - Throws: Error if the delete fails.
    func deletePagesRange(
        first: RawSpan,
        last: RawSpan,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try first.withUnsafeBytes { firstBuf in
            guard let firstBase = firstBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try last.withUnsafeBytes { lastBuf in
                guard let lastBase = lastBuf.baseAddress else {
                    throw InvalidCall.idBufferIsNotTheExpectedSize
                }
                try deletePagesRange(
                    first: (firstBase, firstBuf.count),
                    last: (lastBase, lastBuf.count),
                    wipe: wipe,
                    durable: durable,
                )
            }
        }
    }
}

// MARK: - Foundation

import Foundation

public extension PersistentCache {
    /// Deletes the page identified by `id` from the volume.
    ///
    /// - Parameters:
    ///   - id: Page identifier.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: Error if the delete fails.
    func deletePage(
        id: Data,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            guard let base = idBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try deletePage(id: (base, idBuf.count), wipe: wipe, durable: durable)
        }
    }

    /// Deletes multiple pages from the volume.
    ///
    /// - Parameters:
    ///   - ids: `Data` containing `count` identifiers concatenated.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: Error if the delete fails.
    func deletePages(
        ids: Data,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            guard let idsBase = idsBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try deletePages(ids: (idsBase, idsBuf.count), wipe: wipe, durable: durable)
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
    /// - Throws: Error if the delete fails.
    func deletePagesRange(
        first: Data,
        last: Data,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        try first.withUnsafeBytes { firstBuf in
            guard let firstBase = firstBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try last.withUnsafeBytes { lastBuf in
                guard let lastBase = lastBuf.baseAddress else {
                    throw InvalidCall.idBufferIsNotTheExpectedSize
                }
                try deletePagesRange(
                    first: (firstBase, firstBuf.count),
                    last: (lastBase, lastBuf.count),
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
    /// - Throws: Error if the delete fails.
    func deletePages(
        ids: [Data],
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        guard !ids.isEmpty else { return }
        try validateIDArray(ids)
        let idsSquashed = ids.squashed()
        try idsSquashed.withUnsafeBytes { idsBuf in
            guard let idsBase = idsBuf.baseAddress else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            try deletePages(ids: (idsBase, idsBuf.count), wipe: wipe, durable: durable)
        }
    }
}

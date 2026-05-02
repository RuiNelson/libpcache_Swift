//
//  PC-Delete.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
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
    /// - Throws: Error if the delete fails.
    func deletePage(
        id: CBuffer,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        guard id.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

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
    /// - Throws: Error if the delete fails.
    func deletePages(
        ids: CBuffer,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        guard ids.count % configuration.idWidthInt == 0 else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

        let count = ids.count / configuration.idWidthInt

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
    ///   - count: Number of pages to delete.
    ///   - wipe: If `true`, overwrite the page data with zeros.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``DeletePagesError`` if `position` is out of bounds, the counter overflows,
    ///   or `endianess` is invalid.
    func deletePages(
        counter: Counter,
        count: Int,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        guard counter.templateWidth == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

        try counter.template.withUnsafeBytes { counterBuf in
            try b_deletePagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.baseAddress!,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianess,
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
    /// - Throws: ``DeletePagesError`` if `first > last`.
    func deletePagesRange(
        first: CBuffer,
        last: CBuffer,
        wipe: Bool = false,
        durable: Bool = true,
    ) throws {
        guard first.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

        guard last.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

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
            try deletePage(
                id: (idBuf.baseAddress!, idBuf.count),
                wipe: wipe,
                durable: durable,
            )
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
            try deletePages(
                ids: (idsBuf.baseAddress!, idsBuf.count),
                wipe: wipe,
                durable: durable,
            )
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
            try last.withUnsafeBytes { lastBuf in
                try deletePagesRange(
                    first: (firstBuf.baseAddress!, firstBuf.count),
                    last: (lastBuf.baseAddress!, lastBuf.count),
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
            try deletePage(
                id: (idBuf.baseAddress!, idBuf.count),
                wipe: wipe,
                durable: durable,
            )
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
            try deletePages(
                ids: (idsBuf.baseAddress!, idsBuf.count),
                wipe: wipe,
                durable: durable,
            )
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
            try last.withUnsafeBytes { lastBuf in
                try deletePagesRange(
                    first: (firstBuf.baseAddress!, firstBuf.count),
                    last: (lastBuf.baseAddress!, lastBuf.count),
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
        for id in ids {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
        }

        let idsSquashed = ids.squashed()

        try idsSquashed.withUnsafeBytes { idsBuf in
            try deletePages(
                ids: (idsBuf.baseAddress!, idsBuf.count),
                wipe: wipe,
                durable: durable,
            )
        }
    }
}

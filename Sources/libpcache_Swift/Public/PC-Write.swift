//
//  PC-Write.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

// MARK: - C

public extension PersistentCache {
    /// Stores a single page identified by `id`.
    ///
    /// On FIFO volumes, pages beyond `maxPages` are evicted automatically.
    /// On FIXED volumes, writes beyond capacity fail with ``PutPagesError/capacityExceeded``.
    ///
    /// - Parameters:
    ///   - id: Page identifier; must be exactly ``Configuration/idWidthInt`` bytes.
    ///   - data: Page content; must be at least ``Configuration/pageSizeInt`` bytes.
    ///   - failIfExists: If `true`, verify that no page with the same identifier already exists before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPage(
        id: CBuffer,
        data: CBuffer,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        guard id.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

        guard data.count == configuration.pageSizeInt else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }

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
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPages(
        ids: CBuffer,
        data: CBuffer,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
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
    /// Starting from `counter.template`, computes `count` identifiers by XORing a `UInt32` counter
    /// — initial value `counter.initialValue`, incremented by one per page — into four consecutive
    /// bytes of the template. The counter occupies bytes at indices
    /// `[idWidth − 4 − counter.position, idWidth − 1 − counter.position]`.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - data: Page contents; must be `count * pageSize` bytes.
    ///   - failIfExists: If `true`, verify that none of the computed identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPages(
        counter: Counter,
        data: CBuffer,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        guard counter.templateWidth == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

        guard data.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }

        let count = data.count / configuration.pageSizeInt

        try counter.template.withUnsafeBytes { counterBuf in
            try b_putPagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.baseAddress!,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianess,
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
    ///   - data: Page content; must be at least ``Configuration/pageSizeInt`` bytes.
    ///   - failIfExists: If `true`, verify that no page with the same identifier already exists before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPage(
        id: RawSpan,
        data: RawSpan,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPage(
                    id: (idBuf.baseAddress!, idBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
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
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPages(
        ids: RawSpan,
        data: RawSpan,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
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
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPages(counter: Counter, data: RawSpan, failIfExists: Bool = false, durable: Bool = true) throws {
        try data.withUnsafeBytes { dataBuf in
            try putPages(
                counter: counter,
                data: (dataBuf.baseAddress!, dataBuf.count),
                failIfExists: failIfExists,
                durable: durable,
            )
        }
    }
}

// MARK: - Foundation

import Foundation

public extension PersistentCache {
    /// Stores a single page identified by `id`.
    ///
    /// - Parameters:
    ///   - id: Page identifier; must be exactly ``Configuration/idWidthInt`` bytes.
    ///   - data: Page content; must be at least ``Configuration/pageSizeInt`` bytes.
    ///   - failIfExists: If `true`, verify that no page with the same identifier already exists before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPage(
        id: some ContiguousBytes,
        data: some ContiguousBytes,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPage(
                    id: (idBuf.baseAddress!, idBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
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
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPages(
        ids: some ContiguousBytes,
        data: some ContiguousBytes,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
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
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPages(
        counter: Counter,
        data: some ContiguousBytes,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try data.withUnsafeBytes { dataBuf in
            try putPages(
                counter: counter,
                data: (dataBuf.baseAddress!, dataBuf.count),
                failIfExists: failIfExists,
                durable: durable,
            )
        }
    }
}

// MARK: - Foundation (Arrays)

extension [Data] {
    func squashed() -> Data {
        let totalSize = self.reduce(0) { $0 + $1.count }

        guard totalSize > 0 else {
            return Data()
        }

        var result = Data(count: totalSize)
        result.withUnsafeMutableBytes { buffer in
            var offset = 0
            let base = buffer.baseAddress!.assumingMemoryBound(to: UInt8.self)
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
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPages(ids: [Data], data: [Data], failIfExists: Bool = false, durable: Bool = true) throws {
        guard ids.count == data.count else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }

        for id in ids {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
        }

        for page in data {
            guard page.count == configuration.pageSizeInt else {
                throw InvalidCall.dataBufferIsNotTheExpectedSize
            }
        }

        let idsSquashed = ids.squashed()
        let dataSquashed = data.squashed()

        try idsSquashed.withUnsafeBytes { idsBuf in
            try dataSquashed.withUnsafeBytes { dataBuf in
                try putPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }

    /// Stores multiple pages with identifiers computed from a ``Counter`` template.
    ///
    /// - Parameters:
    ///   - counter: ``Counter`` template and starting value.
    ///   - data: Array of page contents, each exactly ``Configuration/pageSizeInt`` bytes.
    ///   - failIfExists: If `true`, verify that none of the computed identifiers already exist before writing.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPages(counter: Counter, data: [Data], failIfExists: Bool = false, durable: Bool = true) throws {
        guard counter.templateWidth == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }

        for page in data {
            guard page.count == configuration.pageSizeInt else {
                throw InvalidCall.dataBufferIsNotTheExpectedSize
            }
        }

        let dataSquashed = data.squashed()

        try dataSquashed.withUnsafeBytes { dataBuf in
            try putPages(
                counter: counter,
                data: (dataBuf.baseAddress!, dataBuf.count),
                failIfExists: failIfExists,
                durable: durable,
            )
        }
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
    /// - Throws: ``PutPagesError`` if the write fails.
    func putPages(
        pages: [(id: Data, data: Data)],
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        for (id, data) in pages {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }

            guard data.count == configuration.pageSizeInt else {
                throw InvalidCall.dataBufferIsNotTheExpectedSize
            }
        }

        let idsSquashed = pages.map(\.id).squashed()
        let dataSquashed = pages.map(\.data).squashed()

        try idsSquashed.withUnsafeBytes { idsBuf in
            try dataSquashed.withUnsafeBytes { dataBuf in
                try putPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }
}

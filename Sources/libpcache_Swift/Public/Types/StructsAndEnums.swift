//
//  StructsAndEnums.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

import Foundation

/// A pair of files that compose a libpcache volume.
///
/// A ``PersistentCache`` volume consists of two files:
///
/// 1. A binary data file (``dataURL``) — fixed-size pages laid out sequentially
/// 2. A SQLite index database (``databaseURL``) — maps page identifiers to byte offsets in the data file
///
/// - SeeAlso: ``PersistentCache`` for read/write operations on this file pair
public struct FilePair: Sendable {
    /// URL to the SQLite index database file.
    public let databaseURL: URL
    /// URL to the binary data file.
    public let dataURL: URL

    /// Creates a new file pair.
    ///
    /// - Parameters:
    ///   - databaseURL: URL to the SQLite index database. Both files must use the `file://` scheme.
    ///   - dataURL: URL to the binary data file.
    ///
    /// - Returns: `nil` if either URL is not valid for the file system.
    public init?(databaseURL: URL, dataURL: URL) {
        guard databaseURL.isFileURL, dataURL.isFileURL else {
            return nil
        }

        self.databaseURL = databaseURL
        self.dataURL = dataURL
    }
}

/// Immutable configuration parameters of a ``PersistentCache`` volume.
///
/// These values are fixed at creation time and stored in the `metadata` table of the SQLite index.
///
/// - SeeAlso: ``PersistentCache.create(files:configuration:options:)`` to create a volume with this configuration
public struct Configuration: Sendable {
    /// Size of every page, in bytes.
    public let pageSize: UInt32
    /// Maximum number of pages the volume can hold.
    public let maxPages: UInt32
    /// Length of every page identifier, in bytes.
    public let idWidth: UInt32
    /// Eviction policy applied when the volume is full.
    public let capacityPolicy: CapacityPolicy

    /// Size of every page, in bytes (`Int` version).
    public var pageSizeInt: Int {
        Int(pageSize)
    }

    /// Maximum number of pages (`Int` version).
    public var maxPagesInt: Int {
        Int(maxPages)
    }

    /// Length of every identifier, in bytes (`Int` version).
    public var idWidthInt: Int {
        Int(idWidth)
    }

    /// Creates a new configuration.
    ///
    /// - Parameters:
    ///   - pageSize: Size of every page in bytes. Must be greater than zero.
    ///   - maxPages: Maximum number of pages. Must be greater than zero.
    ///   - idWidth: Length of the identifier in bytes. Must be greater than zero.
    ///   - capacityPolicy: Eviction policy applied when the volume is full.
    ///
    /// - Returns: `nil` if any parameter is invalid (zero or exceeds `INT_MAX`).
    public init?(pageSize: Int, maxPages: Int, idWidth: Int, capacityPolicy: CapacityPolicy) {
        guard pageSize > 0, maxPages > 0, idWidth > 0 else {
            return nil
        }

        guard pageSize <= INT_MAX, maxPages <= INT_MAX, idWidth <= INT_MAX else {
            return nil
        }

        self.pageSize = UInt32(pageSize)
        self.maxPages = UInt32(maxPages)
        self.idWidth = UInt32(idWidth)
        self.capacityPolicy = capacityPolicy
    }

    /// Creates a new configuration from a desired total capacity.
    ///
    /// The total capacity must be an exact multiple of the page size.
    ///
    /// - Parameters:
    ///   - capacity: Total capacity in bytes. Must be a multiple of `pageSize`.
    ///   - pageSize: Size of every page in bytes.
    ///   - idWidth: Length of the identifier in bytes.
    ///   - capacityPolicy: Eviction policy applied when the volume is full.
    ///
    /// - Returns: `nil` if the capacity is not a valid multiple of the page size.
    public init?(capacity: Int64, pageSize: Int, idWidth: Int, capacityPolicy: CapacityPolicy) {
        guard capacity >= pageSize, capacity % Int64(pageSize) == 0 else {
            return nil
        }

        let maxPages = capacity / Int64(pageSize)

        guard let new = Configuration(
            pageSize: pageSize,
            maxPages: Int(maxPages),
            idWidth: idWidth,
            capacityPolicy: capacityPolicy,
        ) else {
            return nil
        }

        self = new
    }
}

/// Page occupancy counts for an open ``PersistentCache`` volume.
public struct PageCount {
    /// Number of pages currently stored.
    public var used: Int
    /// Number of available slots (`max_pages - used`).
    public var free: Int
}

/// Eviction policy applied when a ``PersistentCache`` volume reaches capacity.
///
/// Choose ``fixed`` when the caller manages eviction explicitly and needs writes to fail at capacity.
/// Choose ``fifo`` for a rolling-window or circular-buffer pattern where the oldest pages are dropped
/// transparently on overflow.
public enum CapacityPolicy: Sendable {
    /// Writes beyond ``Configuration/maxPages`` fail with ``PutPagesError/capacityExceeded``.
    /// Deleted pages leave reusable free slots.
    case fixed
    /// Writes beyond ``Configuration/maxPages`` silently evict the oldest page.
    /// No explicit capacity error is raised.
    case fifo
}

/// Byte order used when embedding a counter in page identifiers.
///
/// When using range methods (``PersistentCache/getPagesRange(first:last:)``,
/// ``PersistentCache/checkPagesRange(first:last:)``, ``PersistentCache/deletePagesRange(first:last:)``)
/// prefer ``bigEndian``. Range methods compare identifiers byte-by-byte (SQLite BLOB ordering).
/// With big-endian, the most-significant byte occupies the lowest address, so a larger counter
/// always produces a lexicographically greater byte sequence — the two orderings coincide.
/// With little-endian the orderings diverge for values that cross a byte boundary.
public enum Endianness: Sendable {
    /// Host byte order. Not recommended: volumes become non-portable across machines with different byte orders.
    case native
    /// Least-significant byte at the lowest index.
    case littleEndian
    /// Most-significant byte at the lowest index. Recommended when using range methods.
    case bigEndian
}

/// A counter-based page sequence identifier generator.
///
/// ``Counter`` derives page identifiers automatically from a template: a `UInt32` counter — starting at
/// ``initialValue``, incremented per page — is XOR'd into four bytes of ``template``.
/// The operations ``PersistentCache/putPages(counter:data:failIfExists:durable:)``,
/// ``PersistentCache/getPages(counter:count:)``, ``PersistentCache/checkPages(counter:count:)``,
/// and ``PersistentCache/deletePages(counter:count:wipeDataFile:durable:)`` share the same identifier
/// derivation scheme. The operations are therefore symmetric: the same `template`, `start`, `count`,
/// `position`, and `endianess` reconstruct the same identifiers for reads, checks, and deletes
/// as were used for writes.
///
/// ### Identifier Derivation Example
/// With `idWidth = 8`, `template = {0xDE,0xAD,0xBE,0xEF, 0x00,0x00,0x00,0x00}`,
/// `start = 5`, `position = 0`, and ``bigEndian``, the counter value 5 is encoded as
/// `{0x00,0x00,0x00,0x05}` and XOR'd into bytes `[4..7]`:
/// ```swift
/// // template[4..7] ^ {0x00,0x00,0x00,0x05} = {0x00,0x00,0x00,0x05}
/// // Derived id for counter 5: {0xDE,0xAD,0xBE,0xEF, 0x00,0x00,0x00,0x05}
/// // Derived id for counter 6: {0xDE,0xAD,0xBE,0xEF, 0x00,0x00,0x00,0x06}
/// ```
public struct Counter: Sendable {
    /// Fixed template for identifier derivation.
    let template: Data
    /// Offset from the end of the identifier where the counter ends (0 = last four bytes).
    let position: UInt32
    /// Current counter value; incremented by page operations.
    public var initialValue: UInt32
    /// Byte order for the counter.
    let endianess: Endianness

    /// Creates a new counter.
    ///
    /// - Parameters:
    ///   - template: Fixed identifier template. The counter will be XOR'd into the last four bytes
    ///     (or at `position` bytes from the end).
    ///   - zeroPad: Number of zero bytes to append to the template.
    ///   - position: Offset from the end where the counter ends (0 = last four bytes).
    ///   - initialValue: Starting counter value.
    ///   - endianess: Byte order for the counter.
    public init(
        template: Data,
        zeroPad: Int,
        position: Int,
        initialValue: Int,
        endianess: Endianness,
    ) {
        var template = template

        if zeroPad > 0 {
            template.append(contentsOf: .init(repeating: 0, count: zeroPad))
        }

        self.template = template
        self.position = UInt32(position)
        self.initialValue = UInt32(initialValue)
        self.endianess = endianess
    }

    /// Advances the counter by a given amount.
    ///
    /// - Parameter by: Amount to increment the counter.
    public mutating func advance(_ by: Int) {
        initialValue = initialValue + UInt32(by)
    }

    /// Moves the counter backwards by a given amount.
    ///
    /// - Parameter by: Amount to decrement the counter.
    public mutating func backwards(_ by: Int) {
        initialValue = initialValue - UInt32(by)
    }

    /// Width of the template in bytes.
    var templateWidth: Int {
        template.count
    }
}

/// A read-only memory region for passing to libpcache C functions.
///
/// Use as ``CBuffer`` when the C function does not modify the data,
/// or ``CMutableBuffer`` when the C function writes to the buffer.
public typealias CBuffer = (pointer: UnsafeRawPointer, count: Int)

/// A mutable memory region for passing to libpcache C functions.
///
/// Use when the C function writes to the buffer. For read-only buffers, use ``CBuffer``.
public typealias CMutableBuffer = (pointer: UnsafeMutableRawPointer, count: Int)

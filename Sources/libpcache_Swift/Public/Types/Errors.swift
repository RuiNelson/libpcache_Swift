//
//  Errors.swift
//
//  MIT 2-Claude License.
//

import Foundation

// MARK: Common Errors

/// Errors common to multiple operations on a ``PersistentCache`` volume.
public enum CommonErrors: Error {
    /// The volume handle is zero or does not refer to an open volume.
    case invalidHandle
    /// Memory allocation failed.
    case outOfMemory
}

/// Error raised when a SQLite operation fails.
///
/// Inspect ``code`` for the specific SQLite error code.
public struct SQLiteError: Error {
    /// The SQLite error code.
    public let code: Int32

    init(code: Int32) {
        self.code = code
    }

    init(code: Int32?) {
        self.init(code: code ?? 0)
    }
}

// MARK: Operation-Specific Errors

/// Errors returned by ``PersistentCache/create(files:configuration:options:)``.
public enum CreateVolumeError: Error {
    /// A required pointer was NULL or a numeric parameter was zero.
    case invalidArgument
    /// At least one of the two volume files already exists.
    case fileExists
}

/// Errors returned by ``PersistentCache/init(files:)``.
public enum OpenVolumeError: Error {
    /// At least one of the two volume files does not exist.
    case notFound
    /// The index database is missing required metadata.
    case corrupt
    /// Database schema version is newer than library supports.
    case schemaVersionTooHigh
}

/// Errors returned by write operations on a ``PersistentCache`` volume.
///
/// Write operations include ``PersistentCache/putPage(id:data:failIfExists:durable:)``,
/// ``PersistentCache/putPages(ids:data:failIfExists:durable:)``, and
/// ``PersistentCache/putPages(counter:data:failIfExists:durable:)``.
public enum PutPagesError: Error {
    /// FIXED volume is full and has no free slots.
    case capacityExceeded
    /// A page with the same identifier already exists and `failIfExists` was `true`.
    case duplicateID
    /// `position` is out of bounds, the counter would overflow, or `endianess` is invalid.
    case invalidArgument
}

/// Errors returned by read operations on a ``PersistentCache`` volume.
///
/// Read operations include ``PersistentCache/getPage(id:data:)``,
/// ``PersistentCache/getPages(ids:data:)``, ``PersistentCache/getPages(counter:data:)``,
/// and ``PersistentCache/getPagesRange(first:last:idsOut:pagesOut:)``.
public enum GetPagesError: Error {
    /// No page with the given identifier exists in the volume.
    case notFound
    /// `position` is out of bounds, the counter would overflow, or `endianess` is invalid.
    case invalidArgument
    /// `first` is greater than `last`.
    case rangeInvalidRange
    /// `bufferCapacity` is smaller than the number of matching pages.
    case rangeBufferTooSmall
}

/// Errors returned by check operations on a ``PersistentCache`` volume.
///
/// Check operations include ``PersistentCache/checkPage(id:)``,
/// ``PersistentCache/checkPages(ids:)``, ``PersistentCache/checkPages(counter:count:)``,
/// and ``PersistentCache/checkPagesRange(first:last:)``.
public enum CheckPagesError: Error {
    /// `position` is out of bounds, the counter would overflow, or `endianess` is invalid.
    case invalidArgument
    /// `first` is greater than `last`.
    case rangeInvalidRange
}

/// Errors returned by delete operations on a ``PersistentCache`` volume.
///
/// Delete operations include ``PersistentCache/deletePage(id:wipeDataFile:durable:)``,
/// ``PersistentCache/deletePages(ids:wipeDataFile:durable:)``,
/// ``PersistentCache/deletePages(counter:count:wipeDataFile:durable:)``,
/// and ``PersistentCache/deletePagesRange(first:last:wipeDataFile:durable:)``.
public enum DeletePagesError: Error {
    /// `first` is greater than `last`.
    case invalidRange
    /// `position` is out of bounds, the counter would overflow, or `endianess` is invalid.
    case invalidArgument
}

/// Errors returned by ``PersistentCache/defragment(shrinkFile:durable:progress:)``.
public enum DefragmentVolumeError: Error {
    /// The progress callback returned `false`; the volume remains consistent.
    case cancelled
}

/// Errors returned by ``PersistentCache/setNewMaxPages(_:durable:)``.
public enum VolumeSetMaxPagesError: Error {
    /// FIXED volume: total live pages exceed `newMaxPages`.
    case wouldDiscardPages
}

/// Unknown error returned by the underlying libpcache library.
///
/// Inspect ``libpcacheCode`` for the raw error code,
/// ``sqlite3`` for the SQLite error code if available,
/// and ``posix`` for the POSIX error code if available.
public struct UnknownLibPCacheError: Error {
    /// The raw libpcache error code.
    public let libpcacheCode: UInt32
    /// The SQLite error code, if available.
    public let sqlite3: Int32?
    /// The POSIX error code, if available.
    public let posix: POSIXError?

    init(libpcacheCode: UInt32, sqlite3: Int32?, posix: Int32?) {
        self.libpcacheCode = libpcacheCode
        self.sqlite3 = sqlite3 != 0 ? sqlite3 : nil
        if let posix, posix != 0 {
            self.posix = POSIXError(code: posix)
        }
        else {
            self.posix = nil
        }
    }
}

// MARK: User Misuse

/// Errors caused by incorrect API usage (programming errors).
///
/// These errors indicate bugs in the calling code, not failures in the underlying library.
enum InvalidCall: Error {
    case invalidArguments
    case idBufferIsNotTheExpectedSize
    case dataBufferIsNotTheExpectedSize
    case numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
}

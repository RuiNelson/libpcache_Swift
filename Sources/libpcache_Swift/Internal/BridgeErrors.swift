//
//  BridgeErrors.swift
//
//  MIT 2-Claude License.
//

import Foundation
import pcache

/// Constructs a ``POSIXError`` from a raw code.
extension POSIXError {
    init(code: Int32?) {
        if let code, let pec = POSIXErrorCode(rawValue: code) {
            self.init(pec)
        }
        else {
            self.init(.EIO)
        }
    }
}

func bridgeCreateError(_ error: pcache_create_error, sqlite: Int32? = nil, posix: Int32? = nil) throws {
    switch error {
    case PCACHE_CREATE_OK: return
    case PCACHE_CREATE_INVALID_ARGUMENT: throw CreateVolumeError.invalidArgument
    case PCACHE_CREATE_FILE_EXISTS: throw CreateVolumeError.fileExists
    case PCACHE_CREATE_IO_ERROR: throw POSIXError(code: posix)
    case PCACHE_CREATE_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: posix)
    }
}

func bridgeOpenError(_ error: pcache_open_error, sqlite: Int32? = nil, posix: Int32? = nil) throws {
    switch error {
    case PCACHE_OPEN_OK: return
    case PCACHE_OPEN_NOT_FOUND: throw OpenVolumeError.notFound
    case PCACHE_OPEN_CORRUPT: throw OpenVolumeError.corrupt
    case PCACHE_OPEN_SCHEMA_VERSION_TOO_HIGH: throw OpenVolumeError.schemaVersionTooHigh
    case PCACHE_OPEN_OUT_OF_MEMORY: throw CommonErrors.outOfMemory
    case PCACHE_OPEN_IO_ERROR: throw POSIXError(code: posix)
    case PCACHE_OPEN_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: posix)
    }
}

func bridgeCloseError(_ error: pcache_close_error, sqlite: Int32? = nil, posix: Int32? = nil) throws {
    switch error {
    case PCACHE_CLOSE_OK: return
    case PCACHE_CLOSE_INVALID_HANDLE: throw CommonErrors.invalidHandle
    case PCACHE_CLOSE_IO_ERROR: throw POSIXError(code: posix)
    case PCACHE_CLOSE_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: posix)
    }
}

func bridgeInspectConfigurationError(_ error: pcache_inspect_configuration_error) throws {
    switch error {
    case PCACHE_INSPECT_CONFIGURATION_OK: return
    case PCACHE_INSPECT_CONFIGURATION_INVALID_HANDLE: throw CommonErrors.invalidHandle
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: nil, posix: nil)
    }
}

func bridgeInspectPageCountError(_ error: pcache_inspect_page_count_error, sqlite: Int32? = nil) throws {
    switch error {
    case PCACHE_INSPECT_PAGE_COUNT_OK: return
    case PCACHE_INSPECT_PAGE_COUNT_INVALID_HANDLE: throw CommonErrors.invalidHandle
    case PCACHE_INSPECT_PAGE_COUNT_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: nil)
    }
}

func bridgePutError(_ error: pcache_put_error, sqlite: Int32? = nil, posix: Int32? = nil) throws {
    switch error {
    case PCACHE_PUT_OK: return
    case PCACHE_PUT_INVALID_HANDLE: throw CommonErrors.invalidHandle
    case PCACHE_PUT_CAPACITY_EXCEEDED: throw PutPagesError.capacityExceeded
    case PCACHE_PUT_DUPLICATE_ID: throw PutPagesError.duplicateID
    case PCACHE_PUT_INVALID_ARGUMENT: throw PutPagesError.invalidArgument
    case PCACHE_PUT_OUT_OF_MEMORY: throw CommonErrors.outOfMemory
    case PCACHE_PUT_IO_ERROR: throw POSIXError(code: posix)
    case PCACHE_PUT_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: posix)
    }
}

func bridgeGetError(_ error: pcache_get_error, sqlite: Int32? = nil, posix: Int32? = nil) throws {
    switch error {
    case PCACHE_GET_OK: return
    case PCACHE_GET_INVALID_HANDLE: throw CommonErrors.invalidHandle
    case PCACHE_GET_NOT_FOUND: throw GetPagesError.notFound
    case PCACHE_GET_INVALID_ARGUMENT: throw GetPagesError.invalidArgument
    case PCACHE_GET_OUT_OF_MEMORY: throw CommonErrors.outOfMemory
    case PCACHE_GET_RANGE_INVALID_RANGE: throw GetPagesError.rangeInvalidRange
    case PCACHE_GET_RANGE_BUFFER_TOO_SMALL: throw GetPagesError.rangeBufferTooSmall
    case PCACHE_GET_IO_ERROR: throw POSIXError(code: posix)
    case PCACHE_GET_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: posix)
    }
}

func bridgeCheckError(_ error: pcache_check_error, sqlite: Int32? = nil) throws {
    switch error {
    case PCACHE_CHECK_OK: return
    case PCACHE_CHECK_INVALID_HANDLE: throw CommonErrors.invalidHandle
    case PCACHE_CHECK_INVALID_ARGUMENT: throw CheckPagesError.invalidArgument
    case PCACHE_CHECK_OUT_OF_MEMORY: throw CommonErrors.outOfMemory
    case PCACHE_CHECK_RANGE_INVALID_RANGE: throw CheckPagesError.rangeInvalidRange
    case PCACHE_CHECK_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: nil)
    }
}

func bridgeDeleteError(_ error: pcache_delete_error, sqlite: Int32? = nil, posix: Int32? = nil) throws {
    switch error {
    case PCACHE_DELETE_OK: return
    case PCACHE_DELETE_INVALID_HANDLE: throw CommonErrors.invalidHandle
    case PCACHE_DELETE_INVALID_ARGUMENT: throw DeletePagesError.invalidArgument
    case PCACHE_DELETE_OUT_OF_MEMORY: throw CommonErrors.outOfMemory
    case PCACHE_DELETE_INVALID_RANGE: throw DeletePagesError.invalidRange
    case PCACHE_DELETE_IO_ERROR: throw POSIXError(code: posix)
    case PCACHE_DELETE_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: posix)
    }
}

func bridgeDefragmentError(_ error: pcache_defragment_error, sqlite: Int32? = nil, posix: Int32? = nil) throws {
    switch error {
    case PCACHE_DEFRAGMENT_OK: return
    case PCACHE_DEFRAGMENT_INVALID_HANDLE: throw CommonErrors.invalidHandle
    case PCACHE_DEFRAGMENT_CANCELLED: throw DefragmentVolumeError.cancelled
    case PCACHE_DEFRAGMENT_OUT_OF_MEMORY: throw CommonErrors.outOfMemory
    case PCACHE_DEFRAGMENT_IO_ERROR: throw POSIXError(code: posix)
    case PCACHE_DEFRAGMENT_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: posix)
    }
}

func bridgeSetMaxPagesError(_ error: pcache_set_max_pages_error, sqlite: Int32? = nil, posix: Int32? = nil) throws {
    switch error {
    case PCACHE_SET_MAX_PAGES_OK: return
    case PCACHE_SET_MAX_PAGES_INVALID_HANDLE: throw CommonErrors.invalidHandle
    case PCACHE_SET_MAX_PAGES_WOULD_DISCARD_PAGES: throw VolumeSetMaxPagesError.wouldDiscardPages
    case PCACHE_SET_MAX_PAGES_OUT_OF_MEMORY: throw CommonErrors.outOfMemory
    case PCACHE_SET_MAX_PAGES_IO_ERROR: throw POSIXError(code: posix)
    case PCACHE_SET_MAX_PAGES_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: posix)
    }
}

func bridgePreallocateError(_ error: pcache_preallocate_error, sqlite: Int32? = nil, posix: Int32? = nil) throws {
    switch error {
    case PCACHE_PREALLOCATE_OK: return
    case PCACHE_PREALLOCATE_INVALID_HANDLE: throw CommonErrors.invalidHandle
    case PCACHE_PREALLOCATE_IO_ERROR: throw POSIXError(code: posix)
    case PCACHE_PREALLOCATE_SQLITE_ERROR: throw SQLiteError(code: sqlite)
    default: throw UnknownLibPCacheError(libpcacheCode: error.rawValue, sqlite3: sqlite, posix: posix)
    }
}

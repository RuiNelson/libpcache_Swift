//
//  BridgeErrors.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

import Foundation
import pcache

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

private func _bridgeError(
    create: pcache_create_error? = nil,
    open: pcache_open_error? = nil,
    close: pcache_close_error? = nil,
    inspectConfiguration: pcache_inspect_configuration_error? = nil,
    inspectPageCount: pcache_inspect_page_count_error? = nil,
    put: pcache_put_error? = nil,
    get: pcache_get_error? = nil,
    check: pcache_check_error? = nil,
    delete: pcache_delete_error? = nil,
    defragment: pcache_defragment_error? = nil,
    setMaxPages: pcache_set_max_pages_error? = nil,
    preallocate: pcache_preallocate_error? = nil,
    sqlite: Int32? = nil,
    posix: Int32? = nil
) -> (any Error)? {
    if let e = create {
        switch e {
        case PCACHE_CREATE_OK: return nil
        case PCACHE_CREATE_INVALID_ARGUMENT: return CreateVolumeError.invalidArgument
        case PCACHE_CREATE_FILE_EXISTS: return CreateVolumeError.fileExists
        case PCACHE_CREATE_IO_ERROR: return POSIXError(code: posix)
        case PCACHE_CREATE_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = open {
        switch e {
        case PCACHE_OPEN_OK: return nil
        case PCACHE_OPEN_NOT_FOUND: return OpenVolumeError.notFound
        case PCACHE_OPEN_CORRUPT: return OpenVolumeError.corrupt
        case PCACHE_OPEN_SCHEMA_VERSION_TOO_HIGH: return OpenVolumeError.schemaVersionTooHigh
        case PCACHE_OPEN_OUT_OF_MEMORY: return CommonErrors.outOfMemory
        case PCACHE_OPEN_IO_ERROR: return POSIXError(code: posix)
        case PCACHE_OPEN_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = close {
        switch e {
        case PCACHE_CLOSE_OK: return nil
        case PCACHE_CLOSE_INVALID_HANDLE: return CommonErrors.invalidHandle
        case PCACHE_CLOSE_IO_ERROR: return POSIXError(code: posix)
        case PCACHE_CLOSE_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = inspectConfiguration {
        switch e {
        case PCACHE_INSPECT_CONFIGURATION_OK: return nil
        case PCACHE_INSPECT_CONFIGURATION_INVALID_HANDLE: return CommonErrors.invalidHandle
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = inspectPageCount {
        switch e {
        case PCACHE_INSPECT_PAGE_COUNT_OK: return nil
        case PCACHE_INSPECT_PAGE_COUNT_INVALID_HANDLE: return CommonErrors.invalidHandle
        case PCACHE_INSPECT_PAGE_COUNT_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = put {
        switch e {
        case PCACHE_PUT_OK: return nil
        case PCACHE_PUT_INVALID_HANDLE: return CommonErrors.invalidHandle
        case PCACHE_PUT_CAPACITY_EXCEEDED: return PutPagesError.capacityExceeded
        case PCACHE_PUT_DUPLICATE_ID: return PutPagesError.duplicateID
        case PCACHE_PUT_INVALID_ARGUMENT: return PutPagesError.invalidArgument
        case PCACHE_PUT_OUT_OF_MEMORY: return CommonErrors.outOfMemory
        case PCACHE_PUT_IO_ERROR: return POSIXError(code: posix)
        case PCACHE_PUT_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = get {
        switch e {
        case PCACHE_GET_OK: return nil
        case PCACHE_GET_INVALID_HANDLE: return CommonErrors.invalidHandle
        case PCACHE_GET_NOT_FOUND: return GetPagesError.notFound
        case PCACHE_GET_INVALID_ARGUMENT: return GetPagesError.invalidArgument
        case PCACHE_GET_OUT_OF_MEMORY: return CommonErrors.outOfMemory
        case PCACHE_GET_RANGE_INVALID_RANGE: return GetPagesError.rangeInvalidRange
        case PCACHE_GET_RANGE_BUFFER_TOO_SMALL: return GetPagesError.rangeBufferTooSmall
        case PCACHE_GET_IO_ERROR: return POSIXError(code: posix)
        case PCACHE_GET_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = check {
        switch e {
        case PCACHE_CHECK_OK: return nil
        case PCACHE_CHECK_INVALID_HANDLE: return CommonErrors.invalidHandle
        case PCACHE_CHECK_INVALID_ARGUMENT: return CheckPagesError.invalidArgument
        case PCACHE_CHECK_OUT_OF_MEMORY: return CommonErrors.outOfMemory
        case PCACHE_CHECK_RANGE_INVALID_RANGE: return CheckPagesError.rangeInvalidRange
        case PCACHE_CHECK_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = delete {
        switch e {
        case PCACHE_DELETE_OK: return nil
        case PCACHE_DELETE_INVALID_HANDLE: return CommonErrors.invalidHandle
        case PCACHE_DELETE_INVALID_ARGUMENT: return DeletePagesError.invalidArgument
        case PCACHE_DELETE_OUT_OF_MEMORY: return CommonErrors.outOfMemory
        case PCACHE_DELETE_INVALID_RANGE: return DeletePagesError.invalidRange
        case PCACHE_DELETE_IO_ERROR: return POSIXError(code: posix)
        case PCACHE_DELETE_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = defragment {
        switch e {
        case PCACHE_DEFRAGMENT_OK: return nil
        case PCACHE_DEFRAGMENT_INVALID_HANDLE: return CommonErrors.invalidHandle
        case PCACHE_DEFRAGMENT_CANCELLED: return DefragmentVolumeError.cancelled
        case PCACHE_DEFRAGMENT_OUT_OF_MEMORY: return CommonErrors.outOfMemory
        case PCACHE_DEFRAGMENT_IO_ERROR: return POSIXError(code: posix)
        case PCACHE_DEFRAGMENT_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = setMaxPages {
        switch e {
        case PCACHE_SET_MAX_PAGES_OK: return nil
        case PCACHE_SET_MAX_PAGES_INVALID_HANDLE: return CommonErrors.invalidHandle
        case PCACHE_SET_MAX_PAGES_WOULD_DISCARD_PAGES: return VolumeSetMaxPagesError.wouldDiscardPages
        case PCACHE_SET_MAX_PAGES_OUT_OF_MEMORY: return CommonErrors.outOfMemory
        case PCACHE_SET_MAX_PAGES_IO_ERROR: return POSIXError(code: posix)
        case PCACHE_SET_MAX_PAGES_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    if let e = preallocate {
        switch e {
        case PCACHE_PREALLOCATE_OK: return nil
        case PCACHE_PREALLOCATE_INVALID_HANDLE: return CommonErrors.invalidHandle
        case PCACHE_PREALLOCATE_IO_ERROR: return POSIXError(code: posix)
        case PCACHE_PREALLOCATE_SQLITE_ERROR: return SQLiteError(code: sqlite)
        default: return UnknownLibPCacheError(libpcacheCode: e.rawValue, sqlite3: sqlite, posix: posix)
        }
    }
    return nil
}

func bridgeError(
    create: pcache_create_error? = nil,
    open: pcache_open_error? = nil,
    close: pcache_close_error? = nil,
    inspectConfiguration: pcache_inspect_configuration_error? = nil,
    inspectPageCount: pcache_inspect_page_count_error? = nil,
    put: pcache_put_error? = nil,
    get: pcache_get_error? = nil,
    check: pcache_check_error? = nil,
    delete: pcache_delete_error? = nil,
    defragment: pcache_defragment_error? = nil,
    setMaxPages: pcache_set_max_pages_error? = nil,
    preallocate: pcache_preallocate_error? = nil,
    sqlite: Int32? = nil,
    posix: Int32? = nil
) throws {
    if let e = _bridgeError(
        create: create,
        open: open,
        close: close,
        inspectConfiguration: inspectConfiguration,
        inspectPageCount: inspectPageCount,
        put: put,
        get: get,
        check: check,
        delete: delete,
        defragment: defragment,
        setMaxPages: setMaxPages,
        preallocate: preallocate,
        sqlite: sqlite,
        posix: posix
    ) {
        throw e
    }
}

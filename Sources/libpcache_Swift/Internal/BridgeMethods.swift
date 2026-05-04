//
//  BridgeMethods.swift
//
//  MIT 2-Claude License.
//

import Foundation
import pcache

// MARK: - Progress callback bridge

private final class ProgressBox: @unchecked Sendable {
    let fn: @Sendable (Double) -> Bool
    init(_ fn: @escaping @Sendable (Double) -> Bool) {
        self.fn = fn
    }
}

private func withCProgressCallback<R>(
    _ callback: (@Sendable (Double) -> Bool)?,
    _ body: (pcache_progress_fn?, UnsafeMutableRawPointer?) -> R,
) -> R {
    guard let callback else { return body(nil, nil) }
    let box = ProgressBox(callback)
    let ptr = Unmanaged.passRetained(box).toOpaque()
    defer { Unmanaged<ProgressBox>.fromOpaque(ptr).release() }
    let cFn: pcache_progress_fn = { progress, userData in
        guard let ptr = userData else { return false }
        return Unmanaged<ProgressBox>.fromOpaque(ptr).takeUnretainedValue().fn(progress)
    }
    return body(cFn, ptr)
}

/// Opaque handle to the underlying C volume.
typealias Handle = pcache_handle

// MARK: - Lifecycle

/// Creates a new volume on the filesystem.
/// - Throws: ``CreateVolumeError`` on failure.
func b_create(
    paths: FilePair,
    config: Configuration,
    preallocateDatabase: Bool,
    preallocateDatafile: Bool,
) throws {
    var err: pcache_create_error = PCACHE_CREATE_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    var cConfig = config.cValue
    paths.databaseURL.path.withCString { dbPath in
        paths.dataURL.path.withCString { dataPath in
            var pair = pcache_file_pair(database_path: dbPath, data_path: dataPath)
            pcache_create(&pair, &cConfig, preallocateDatabase, preallocateDatafile, &err, &sqliteErr, &posixErr)
        }
    }
    try bridgeError(create: err, sqlite: sqliteErr, posix: posixErr)
}

/// Opens an existing volume.
/// - Returns: Handle to the open volume.
/// - Throws: ``OpenVolumeError`` on failure.
func b_open(paths: FilePair) throws -> Handle {
    var err: pcache_open_error = PCACHE_OPEN_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    let h = paths.databaseURL.path.withCString { dbPath in
        paths.dataURL.path.withCString { dataPath in
            var pair = pcache_file_pair(database_path: dbPath, data_path: dataPath)
            return pcache_open(&pair, &err, &sqliteErr, &posixErr)
        }
    }
    if h == 0 {
        try bridgeError(open: err, sqlite: sqliteErr, posix: posixErr)
    }
    return h
}

/// Closes an open volume.
/// - Throws: ``CloseVolumeError`` on failure.
func b_close(handle: Handle) throws {
    var err: pcache_close_error = PCACHE_CLOSE_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_close(handle, &err, &sqliteErr, &posixErr)
    try bridgeError(close: err, sqlite: sqliteErr, posix: posixErr)
}

// MARK: - Introspection

/// Returns the configuration of an open volume.
/// - Throws: Error if inspection fails.
func b_inspectConfiguration(handle: Handle) throws -> Configuration {
    var err: pcache_inspect_configuration_error = PCACHE_INSPECT_CONFIGURATION_OK
    let c = pcache_inspect_configuration(handle, &err)
    try bridgeError(inspectConfiguration: err)
    return Configuration(c)
}

/// Returns the page counts for an open volume.
/// - Throws: Error if inspection fails.
func b_inspectPageCount(handle: Handle) throws -> PageCount {
    var err: pcache_inspect_page_count_error = PCACHE_INSPECT_PAGE_COUNT_OK
    var sqliteErr: Int32 = .init()
    let c = pcache_inspect_page_count(handle, &err, &sqliteErr)
    try bridgeError(inspectPageCount: err, sqlite: sqliteErr)
    return PageCount(c)
}

// MARK: - Put

/// Stores a single page.
/// - Throws: ``PutPagesError`` on failure.
func b_putPage(
    handle: Handle,
    id: UnsafeRawPointer,
    pageData: UnsafeRawPointer,
    failIfExists: Bool,
    durable: Bool,
) throws {
    var err: pcache_put_error = PCACHE_PUT_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_put_page(handle, id, pageData, failIfExists, durable, &err, &sqliteErr, &posixErr)
    try bridgeError(put: err, sqlite: sqliteErr, posix: posixErr)
}

/// Stores multiple pages atomically.
/// - Throws: ``PutPagesError`` on failure.
func b_putPages(
    handle: Handle,
    count: Int,
    ids: UnsafeRawPointer,
    pagesData: UnsafeRawPointer,
    failIfExists: Bool,
    durable: Bool,
) throws {
    var err: pcache_put_error = PCACHE_PUT_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_put_pages(handle, count, ids, pagesData, failIfExists, durable, &err, &sqliteErr, &posixErr)
    try bridgeError(put: err, sqlite: sqliteErr, posix: posixErr)
}

/// Stores multiple pages with auto-derived identifiers.
/// - Throws: ``PutPagesError`` on failure.
func b_putPagesWithCounter(
    handle: Handle,
    count: Int,
    idBase: UnsafeRawPointer,
    start: UInt32,
    position: UInt32,
    endianness: Endianness,
    pagesData: UnsafeRawPointer,
    failIfExists: Bool,
    durable: Bool,
) throws {
    var err: pcache_put_error = PCACHE_PUT_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_put_pages_with_counter(
        handle,
        count,
        idBase,
        start,
        position,
        endianness.cValue,
        pagesData,
        failIfExists,
        durable,
        &err,
        &sqliteErr,
        &posixErr,
    )
    try bridgeError(put: err, sqlite: sqliteErr, posix: posixErr)
}

// MARK: - Get

/// Retrieves a single page.
/// - Throws: ``GetPagesError`` on failure.
func b_getPage(handle: Handle, id: UnsafeRawPointer, pageData: UnsafeMutableRawPointer) throws {
    var err: pcache_get_error = PCACHE_GET_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_get_page(handle, id, pageData, &err, &sqliteErr, &posixErr)
    try bridgeError(get: err, sqlite: sqliteErr, posix: posixErr)
}

/// Retrieves multiple pages.
/// - Throws: ``GetPagesError`` on failure.
func b_getPages(handle: Handle, count: Int, ids: UnsafeRawPointer, pageData: UnsafeMutableRawPointer) throws {
    var err: pcache_get_error = PCACHE_GET_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_get_pages(handle, count, ids, pageData, &err, &sqliteErr, &posixErr)
    try bridgeError(get: err, sqlite: sqliteErr, posix: posixErr)
}

/// Retrieves multiple pages with auto-derived identifiers.
/// - Throws: ``GetPagesError`` on failure.
func b_getPagesWithCounter(
    handle: Handle,
    count: Int,
    idBase: UnsafeRawPointer,
    start: UInt32,
    position: UInt32,
    endianness: Endianness,
    pageData: UnsafeMutableRawPointer,
) throws {
    var err: pcache_get_error = PCACHE_GET_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_get_pages_with_counter(
        handle,
        count,
        idBase,
        start,
        position,
        endianness.cValue,
        pageData,
        &err,
        &sqliteErr,
        &posixErr,
    )
    try bridgeError(get: err, sqlite: sqliteErr, posix: posixErr)
}

/// Retrieves pages within a range.
/// - Returns: Number of pages retrieved.
/// - Throws: ``GetPagesError`` on failure.
func b_getPagesRange(
    handle: Handle,
    first: UnsafeRawPointer,
    last: UnsafeRawPointer,
    idsOut: UnsafeMutableRawPointer,
    pagesOut: UnsafeMutableRawPointer,
    bufferCapacity: UInt32,
) throws -> Int {
    var countOut: UInt32 = 0
    var err: pcache_get_error = PCACHE_GET_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_get_pages_range(
        handle,
        first,
        last,
        idsOut,
        pagesOut,
        bufferCapacity,
        &countOut,
        &err,
        &sqliteErr,
        &posixErr,
    )
    try bridgeError(get: err, sqlite: sqliteErr, posix: posixErr)
    return Int(countOut)
}

// MARK: - Check

/// Checks if a single page exists.
/// - Returns: `true` if page exists.
/// - Throws: Error on failure.
func b_checkPage(handle: Handle, id: UnsafeRawPointer) throws -> Bool {
    var err: pcache_check_error = PCACHE_CHECK_OK
    var sqliteErr: Int32 = .init()
    let result = pcache_check_page(handle, id, &err, &sqliteErr)
    try bridgeError(check: err, sqlite: sqliteErr)
    return result
}

/// Checks if multiple pages exist.
/// - Throws: Error on failure.
func b_checkPages(handle: Handle, count: Int, ids: UnsafeRawPointer, results: UnsafeMutablePointer<Bool>) throws {
    var err: pcache_check_error = PCACHE_CHECK_OK
    var sqliteErr: Int32 = .init()
    pcache_check_pages(handle, count, ids, results, &err, &sqliteErr)
    try bridgeError(check: err, sqlite: sqliteErr)
}

/// Checks if multiple pages exist using auto-derived identifiers.
/// - Throws: ``CheckPagesError`` on failure.
func b_checkPagesWithCounter(
    handle: Handle,
    count: Int,
    idBase: UnsafeRawPointer,
    start: UInt32,
    position: UInt32,
    endianness: Endianness,
    results: UnsafeMutablePointer<Bool>,
) throws {
    var err: pcache_check_error = PCACHE_CHECK_OK
    var sqliteErr: Int32 = .init()
    pcache_check_pages_with_counter(
        handle,
        count,
        idBase,
        start,
        position,
        endianness.cValue,
        results,
        &err,
        &sqliteErr,
    )
    try bridgeError(check: err, sqlite: sqliteErr)
}

/// Counts pages within a range.
/// - Returns: Number of pages in range.
/// - Throws: ``CheckPagesError`` on failure.
func b_checkPagesRange(handle: Handle, first: UnsafeRawPointer, last: UnsafeRawPointer) throws -> Int {
    var countOut: UInt32 = 0
    var err: pcache_check_error = PCACHE_CHECK_OK
    var sqliteErr: Int32 = .init()
    pcache_check_pages_range(handle, first, last, &countOut, &err, &sqliteErr)
    try bridgeError(check: err, sqlite: sqliteErr)
    return Int(countOut)
}

// MARK: - Delete

/// Deletes a single page.
/// - Throws: Error on failure.
func b_deletePage(handle: Handle, id: UnsafeRawPointer, wipeDataFile: Bool, durable: Bool) throws {
    var err: pcache_delete_error = PCACHE_DELETE_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_delete_page(handle, id, wipeDataFile, durable, &err, &sqliteErr, &posixErr)
    try bridgeError(delete: err, sqlite: sqliteErr, posix: posixErr)
}

/// Deletes multiple pages.
/// - Throws: Error on failure.
func b_deletePages(
    handle: Handle,
    count: Int,
    ids: UnsafeRawPointer,
    wipeDataFile: Bool,
    durable: Bool,
) throws {
    var err: pcache_delete_error = PCACHE_DELETE_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_delete_pages(handle, count, ids, wipeDataFile, durable, &err, &sqliteErr, &posixErr)
    try bridgeError(delete: err, sqlite: sqliteErr, posix: posixErr)
}

/// Deletes multiple pages with auto-derived identifiers.
/// - Throws: ``DeletePagesError`` on failure.
func b_deletePagesWithCounter(
    handle: Handle,
    count: Int,
    idBase: UnsafeRawPointer,
    start: UInt32,
    position: UInt32,
    endianness: Endianness,
    wipeDataFile: Bool,
    durable: Bool,
) throws {
    var err: pcache_delete_error = PCACHE_DELETE_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_delete_pages_with_counter(
        handle,
        count,
        idBase,
        start,
        position,
        endianness.cValue,
        wipeDataFile,
        durable,
        &err,
        &sqliteErr,
        &posixErr,
    )
    try bridgeError(delete: err, sqlite: sqliteErr, posix: posixErr)
}

/// Deletes pages within a range.
/// - Throws: ``DeletePagesError`` on failure.
func b_deletePagesRange(
    handle: Handle,
    first: UnsafeRawPointer,
    last: UnsafeRawPointer,
    wipeDataFile: Bool,
    durable: Bool,
) throws {
    var err: pcache_delete_error = PCACHE_DELETE_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_delete_pages_range(handle, first, last, wipeDataFile, durable, &err, &sqliteErr, &posixErr)
    try bridgeError(delete: err, sqlite: sqliteErr, posix: posixErr)
}

// MARK: - Maintenance

/// Defragments the volume, relocating pages contiguously.
/// - Throws: ``DefragmentVolumeError`` if cancelled.
func b_defragment(
    handle: Handle,
    progress: (@Sendable (Double) -> Bool)?,
    shrinkFile: Bool,
    durable: Bool,
) throws {
    var err: pcache_defragment_error = PCACHE_DEFRAGMENT_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    withCProgressCallback(progress) { cFn, userData in
        pcache_defragment(handle, cFn, userData, shrinkFile, durable, &err, &sqliteErr, &posixErr)
    }
    try bridgeError(defragment: err, sqlite: sqliteErr, posix: posixErr)
}

/// Adjusts the maximum page count.
/// - Throws: ``VolumeSetMaxPagesError`` if reduction would discard pages.
func b_setMaxPages(handle: Handle, newMaxPages: UInt32, durable: Bool) throws {
    var err: pcache_set_max_pages_error = PCACHE_SET_MAX_PAGES_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_set_max_pages(handle, newMaxPages, durable, &err, &sqliteErr, &posixErr)
    try bridgeError(setMaxPages: err, sqlite: sqliteErr, posix: posixErr)
}

/// Preallocates space in an open volume.
/// - Throws: Error on failure.
func b_preallocate(handle: Handle, database: Bool, datafile: Bool, durable: Bool) throws {
    var err: pcache_preallocate_error = PCACHE_PREALLOCATE_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_preallocate(handle, database, datafile, durable, &err, &sqliteErr, &posixErr)
    try bridgeError(preallocate: err, sqlite: sqliteErr, posix: posixErr)
}

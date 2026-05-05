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

/// Invokes `body` with C string pointers to the database and data file paths.
///
/// Uses ``URL/withUnsafeFileSystemRepresentation(_:)`` so paths are encoded in the
/// platform's native filesystem representation and survive Unicode and percent-encoding edge cases.
private func withFilePairCStrings<R>(
    _ paths: FilePair,
    _ body: (UnsafePointer<CChar>, UnsafePointer<CChar>) -> R,
) -> R? {
    paths.databaseURL.withUnsafeFileSystemRepresentation { dbPath in
        paths.dataURL.withUnsafeFileSystemRepresentation { dataPath in
            guard let dbPath, let dataPath else { return nil }
            return body(dbPath, dataPath)
        }
    }
}

/// Creates a new volume on the filesystem.
///
/// - Throws: ``CreateVolumeError`` if the volume cannot be created;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    let invoked: Void? = withFilePairCStrings(paths) { dbPath, dataPath in
        var pair = pcache_file_pair(database_path: dbPath, data_path: dataPath)
        pcache_create(&pair, &cConfig, preallocateDatabase, preallocateDatafile, &err, &sqliteErr, &posixErr)
    }
    guard invoked != nil else { throw CreateVolumeError.invalidArgument }
    try bridgeCreateError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Opens an existing volume.
///
/// - Throws: ``OpenVolumeError`` if the volume cannot be opened;
///   ``CommonErrors``/`.outOfMemory` on allocation failure;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
///
/// - Returns: Handle to the open volume.
func b_open(paths: FilePair) throws -> Handle {
    var err: pcache_open_error = PCACHE_OPEN_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    let h: Handle? = withFilePairCStrings(paths) { dbPath, dataPath in
        var pair = pcache_file_pair(database_path: dbPath, data_path: dataPath)
        return pcache_open(&pair, &err, &sqliteErr, &posixErr)
    }
    guard let h else { throw OpenVolumeError.notFound }
    if h == 0 {
        try bridgeOpenError(err, sqlite: sqliteErr, posix: posixErr)
    }
    return h
}

/// Closes an open volume.
///
/// - Throws: ``CommonErrors``/`.invalidHandle` if the handle is invalid;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on WAL checkpoint failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
func b_close(handle: Handle) throws {
    var err: pcache_close_error = PCACHE_CLOSE_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_close(handle, &err, &sqliteErr, &posixErr)
    try bridgeCloseError(err, sqlite: sqliteErr, posix: posixErr)
}

// MARK: - Introspection

/// Returns the configuration of an open volume.
///
/// - Throws: ``CommonErrors``/`.invalidHandle` if the handle is invalid;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
func b_inspectConfiguration(handle: Handle) throws -> Configuration {
    var err: pcache_inspect_configuration_error = PCACHE_INSPECT_CONFIGURATION_OK
    let c = pcache_inspect_configuration(handle, &err)
    try bridgeInspectConfigurationError(err)
    return Configuration(c)
}

/// Returns the page counts for an open volume.
///
/// - Throws: ``CommonErrors``/`.invalidHandle` if the handle is invalid;
///   ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
func b_inspectPageCount(handle: Handle) throws -> PageCount {
    var err: pcache_inspect_page_count_error = PCACHE_INSPECT_PAGE_COUNT_OK
    var sqliteErr: Int32 = .init()
    let c = pcache_inspect_page_count(handle, &err, &sqliteErr)
    try bridgeInspectPageCountError(err, sqlite: sqliteErr)
    return PageCount(c)
}

// MARK: - Put

/// Stores a single page.
///
/// - Throws: ``PutPagesError`` if the write fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    try bridgePutError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Stores multiple pages atomically.
///
/// - Throws: ``PutPagesError`` if the write fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    try bridgePutError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Stores multiple pages with auto-derived identifiers.
///
/// - Throws: ``PutPagesError`` if the write fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    try bridgePutError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Retrieves a single page.
///
/// - Throws: ``GetPagesError`` if the read fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
func b_getPage(handle: Handle, id: UnsafeRawPointer, pageData: UnsafeMutableRawPointer) throws {
    var err: pcache_get_error = PCACHE_GET_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_get_page(handle, id, pageData, &err, &sqliteErr, &posixErr)
    try bridgeGetError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Retrieves multiple pages.
///
/// - Throws: ``GetPagesError`` if the read fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
func b_getPages(handle: Handle, count: Int, ids: UnsafeRawPointer, pageData: UnsafeMutableRawPointer) throws {
    var err: pcache_get_error = PCACHE_GET_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_get_pages(handle, count, ids, pageData, &err, &sqliteErr, &posixErr)
    try bridgeGetError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Retrieves multiple pages with auto-derived identifiers.
///
/// - Throws: ``GetPagesError`` if the read fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    try bridgeGetError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Retrieves pages within a range.
///
/// - Throws: ``GetPagesError`` if the read fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
///
/// - Returns: Number of pages retrieved.
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
    try bridgeGetError(err, sqlite: sqliteErr, posix: posixErr)
    return Int(countOut)
}

// MARK: - Check

/// Checks if a single page exists.
///
/// - Throws: ``CheckPagesError`` if the check fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
///
/// - Returns: `true` if page exists.
func b_checkPage(handle: Handle, id: UnsafeRawPointer) throws -> Bool {
    var err: pcache_check_error = PCACHE_CHECK_OK
    var sqliteErr: Int32 = .init()
    let result = pcache_check_page(handle, id, &err, &sqliteErr)
    try bridgeCheckError(err, sqlite: sqliteErr)
    return result
}

/// Checks if multiple pages exist.
///
/// - Throws: ``CheckPagesError`` if the check fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
func b_checkPages(handle: Handle, count: Int, ids: UnsafeRawPointer, results: UnsafeMutablePointer<Bool>) throws {
    var err: pcache_check_error = PCACHE_CHECK_OK
    var sqliteErr: Int32 = .init()
    pcache_check_pages(handle, count, ids, results, &err, &sqliteErr)
    try bridgeCheckError(err, sqlite: sqliteErr)
}

/// Checks if multiple pages exist using auto-derived identifiers.
///
/// - Throws: ``CheckPagesError`` if the check fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    try bridgeCheckError(err, sqlite: sqliteErr)
}

/// Counts pages within a range.
///
/// - Throws: ``CheckPagesError`` if the check fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
///
/// - Returns: Number of pages in range.
func b_checkPagesRange(handle: Handle, first: UnsafeRawPointer, last: UnsafeRawPointer) throws -> Int {
    var countOut: UInt32 = 0
    var err: pcache_check_error = PCACHE_CHECK_OK
    var sqliteErr: Int32 = .init()
    pcache_check_pages_range(handle, first, last, &countOut, &err, &sqliteErr)
    try bridgeCheckError(err, sqlite: sqliteErr)
    return Int(countOut)
}

// MARK: - Delete

/// Deletes a single page.
///
/// - Throws: ``DeletePagesError`` if the delete fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
func b_deletePage(handle: Handle, id: UnsafeRawPointer, wipeDataFile: Bool, durable: Bool) throws {
    var err: pcache_delete_error = PCACHE_DELETE_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_delete_page(handle, id, wipeDataFile, durable, &err, &sqliteErr, &posixErr)
    try bridgeDeleteError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Deletes multiple pages.
///
/// - Throws: ``DeletePagesError`` if the delete fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    try bridgeDeleteError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Deletes multiple pages with auto-derived identifiers.
///
/// - Throws: ``DeletePagesError`` if the delete fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    try bridgeDeleteError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Deletes pages within a range.
///
/// - Throws: ``DeletePagesError`` if the delete fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    try bridgeDeleteError(err, sqlite: sqliteErr, posix: posixErr)
}

// MARK: - Maintenance

/// Defragments the volume, relocating pages contiguously.
///
/// - Throws: ``DefragmentVolumeError`` if defragmentation fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
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
    try bridgeDefragmentError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Adjusts the maximum page count.
///
/// - Throws: ``VolumeSetMaxPagesError`` if the adjustment fails;
///   ``CommonErrors``/`.invalidHandle` or ``CommonErrors``/`.outOfMemory` for shared failures;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
func b_setMaxPages(handle: Handle, newMaxPages: UInt32, durable: Bool) throws {
    var err: pcache_set_max_pages_error = PCACHE_SET_MAX_PAGES_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_set_max_pages(handle, newMaxPages, durable, &err, &sqliteErr, &posixErr)
    try bridgeSetMaxPagesError(err, sqlite: sqliteErr, posix: posixErr)
}

/// Preallocates space in an open volume.
///
/// - Throws: ``CommonErrors``/`.invalidHandle` if the handle is invalid;
///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
///   ``UnknownLibPCacheError`` for unrecognized C error codes.
func b_preallocate(handle: Handle, database: Bool, datafile: Bool, durable: Bool) throws {
    var err: pcache_preallocate_error = PCACHE_PREALLOCATE_OK
    var sqliteErr: Int32 = .init()
    var posixErr: Int32 = .init()
    pcache_preallocate(handle, database, datafile, durable, &err, &sqliteErr, &posixErr)
    try bridgePreallocateError(err, sqlite: sqliteErr, posix: posixErr)
}

//
//  PC-Lifecycle.swift
//
//  MIT 2-Claude License.
//

import Foundation

/// A persistent, paged, random-access storage volume indexed by key.
///
/// A volume consists of two files: a binary data file and a SQLite index.
/// All public functions are thread-safe.
///
/// ### Creating a Volume
/// ```swift
/// let files = FilePair(databaseURL: dbURL, dataURL: dataURL)!
/// let config = Configuration(pageSize: 4096, maxPages: 1000, idWidth: 16, capacityPolicy: .fixed)!
///
/// // Create the volume on disk
/// try PersistentCache.create(files: files, configuration: config)
///
/// // Open the volume
/// let cache = try PersistentCache(files: files)
/// ```
///
/// ### Storing and Retrieving Pages
/// ```swift
/// var id = "Hello World!".data(using: .ascii)!
/// id.append(contentsOf: .init(repeating: 0, count: 16 - id.count)) // zero-pad
///
/// let page = Data(repeating: 0x00, count: 4096)
///
/// // Store a page
/// try cache.putPage(id: id, data: page)
///
/// // Retrieve it back
/// let retrieved: Data = try cache.getPage(id: id)
/// ```
///
/// - SeeAlso: ``Configuration`` for volume configuration parameters
public final class PersistentCache: @unchecked Sendable {
    private let lock: NSLock
    /// Opaque handle to the underlying C volume. Access only under `lock`. Zero means already closed.
    private var _handle: Handle

    /// Thread-safe read of the underlying C handle.
    var handle: Handle {
        lock.lock()
        defer { lock.unlock() }
        return _handle
    }

    init(handle: Handle) {
        self._handle = handle
        self.lock = NSLock()
    }

    deinit {
        lock.lock()
        let h = _handle
        _handle = 0
        lock.unlock()
        if h != 0 {
            try? b_close(handle: h)
        }
    }
}

// MARK: - Lifecycle

public extension PersistentCache {
    /// Creates a new volume on the filesystem.
    ///
    /// Fails immediately if either file already exists.
    ///
    /// - Parameters:
    ///   - files: Paths to the database and data files.
    ///   - configuration: Volume parameters (page size, capacity, id width, eviction policy).
    ///   - options.preallocateDatabase: If `true`, pre-allocate slots in the index for all `maxPages` pages,
    ///     enabling O(1) allocation on subsequent inserts.
    ///   - options.preallocateDatafile: If `true`, extend the data file to its maximum size immediately.
    ///
    /// - Throws: ``CreateVolumeError`` if the volume cannot be created;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    static func create(
        files: FilePair,
        configuration: Configuration,
        options: (preallocateDatabase: Bool, preallocateDatafile: Bool) = (false, false),
    ) throws {
        try b_create(
            paths: files,
            config: configuration,
            preallocateDatabase: options.preallocateDatabase,
            preallocateDatafile: options.preallocateDatafile,
        )
    }

    /// Opens an existing volume.
    ///
    /// The volume is closed automatically when the ``PersistentCache`` object is deallocated.
    ///
    /// - Parameter files: Paths to the database and data files.
    ///
    /// - Throws: ``OpenVolumeError`` if the volume cannot be opened;
    ///   ``CommonErrors/outOfMemory`` on allocation failure;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on database failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    convenience init(files: FilePair) throws {
        let h = try b_open(paths: files)
        self.init(handle: h)
    }

    /// Explicitly closes the volume and flushes any pending writes.
    ///
    /// After this call the object must not be used again.
    /// The volume is also closed automatically on deallocation, but errors are silenced in that path.
    ///
    /// - Throws: ``CommonErrors/invalidHandle`` if the volume handle is invalid;
    ///   ``POSIXError`` on I/O failure; ``SQLiteError`` on WAL checkpoint failure;
    ///   ``UnknownLibPCacheError`` for unrecognized C error codes.
    func close() throws {
        lock.lock()
        let h = _handle
        _handle = 0
        lock.unlock()
        guard h != 0 else { throw CommonErrors.invalidHandle }
        try b_close(handle: h)
    }
}

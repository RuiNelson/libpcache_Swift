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
/// ### Closing
/// ```swift
/// try cache.close()
/// ```
///
/// - SeeAlso: ``Configuration`` for volume configuration parameters
public final class PersistentCache: Sendable {
    /// Opaque handle to the underlying C volume.
    let handle: Handle

    init(handle: Handle) {
        self.handle = handle
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
    ///   - options.prealocateDatabase: If `true`, pre-allocate slots in the index for all `maxPages` pages,
    ///     enabling O(1) allocation on subsequent inserts.
    ///   - options.prealocateDatafile: If `true`, extend the data file to its maximum size immediately.
    ///
    /// - Throws: ``CreateVolumeError`` if creation fails.
    static func create(
        files: FilePair,
        configuration: Configuration,
        options: (prealocateDatabase: Bool, prealocateDatafile: Bool) = (false, false),
    ) throws {
        try b_create(
            paths: files,
            config: configuration,
            preallocateDatabase: options.prealocateDatabase,
            preallocateDatafile: options.prealocateDatafile,
        )
    }

    /// Opens an existing volume.
    ///
    /// - Parameter files: Paths to the database and data files.
    ///
    /// - Throws: ``OpenVolumeError`` if the volume cannot be opened.
    convenience init(files: FilePair) throws {
        let h = try b_open(paths: files)
        self.init(handle: h)
    }

    /// Closes an open volume and releases all associated resources.
    ///
    /// On return, the data file and index database are fsync'd before the handles are closed,
    /// ensuring all data is persisted to stable storage.
    ///
    /// - Throws: Error if the close operation fails.
    func close() throws {
        try b_close(handle: handle)
    }
}

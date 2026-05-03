//
//  PC-Introspection.swift
//
//  MIT 2-Claude License.
//

public extension PersistentCache {
    /// Returns the current configuration of this volume.
    ///
    /// The configuration is kept in memory and updated by operations such as
    /// ``setNewMaxPages(_:durable:)``, so the returned value always reflects the current
    /// state of the volume.
    ///
    /// - SeeAlso: ``Configuration`` for the meaning of each field.
    var configuration: Configuration {
        // The handle is managed internally, there is no possibility of providing an invalid handle to the C library
        try! b_inspectConfiguration(handle: handle)
    }

    /// Returns the number of used and free pages in this volume.
    ///
    /// - Returns: ``PageCount`` with the current occupancy.
    /// - Throws: Error if the inspection fails.
    func pageCounts() throws -> PageCount {
        try b_inspectPageCount(handle: handle)
    }
}

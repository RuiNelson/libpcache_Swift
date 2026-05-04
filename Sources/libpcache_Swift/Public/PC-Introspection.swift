//
//  PC-Introspection.swift
//
//  MIT 2-Claude License.
//

public extension PersistentCache {
    /// Returns the current configuration of this volume.
    ///
    /// - Returns: The ``Configuration`` stored in the volume's index.
    /// - Throws: ``CommonErrors/invalidHandle`` if the volume has been closed.
    ///
    /// - SeeAlso: ``Configuration`` for the meaning of each field.
    var configuration: Configuration {
        get throws {
            try b_inspectConfiguration(handle: handle)
        }
    }

    /// Returns the number of used and free pages in this volume.
    ///
    /// - Returns: ``PageCount`` with the current occupancy.
    /// - Throws: ``CommonErrors/invalidHandle`` if the volume has been closed; ``SQLiteError`` on database failure.
    func pageCounts() throws -> PageCount {
        try b_inspectPageCount(handle: handle)
    }
}

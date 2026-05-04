//
//  PC-Maintenance.swift
//
//  MIT 2-Claude License.
//

import Foundation

public extension PersistentCache {
    /// Relocates live pages contiguously toward the start of the data file.
    ///
    /// The `progress` closure is invoked after each page and may cancel the operation
    /// by returning `false`; the volume remains consistent in all cases.
    ///
    /// On ``CapacityPolicy/fifo`` volumes this is a no-op: the FIFO eviction order is
    /// encoded in the relative positions of live and empty slots, so any rearrangement
    /// would corrupt that order. The callback is invoked once with `progress = 1.0`.
    ///
    /// - Note: Fragmentation accumulates on ``CapacityPolicy/fixed`` volumes: each deletion
    ///   leaves a hole that is filled one slot at a time by subsequent writes. Call this
    ///   function after bulk deletions to consolidate live pages toward the start of the
    ///   data file. Pass `shrinkFile = true` to also reclaim the trailing empty space.
    ///
    /// - Parameters:
    ///   - shrinkFile: If `true`, truncate the data file to the minimum size after relocation.
    ///   - durable: If `true`, block until data is durable on disk.
    ///   - progress: Closure called after each page; return `false` to cancel.
    ///
    /// - Throws: ``DefragmentVolumeError`` if the progress callback cancels the operation.
    func defragment(shrinkFile: Bool, durable: Bool = true, progress: @escaping @Sendable (Double) -> Bool) throws {
        try b_defragment(handle: handle, progress: progress, shrinkFile: shrinkFile, durable: durable)
    }

    /// Adjusts the maximum capacity of the volume.
    ///
    /// Growth is always permitted. On ``CapacityPolicy/fixed`` volumes, any live pages that
    /// reside beyond `newMaxPages` are automatically moved into free slots within `[1, newMaxPages]`;
    /// the operation fails with ``VolumeSetMaxPagesError/wouldDiscardPages`` only when the total
    /// number of live pages exceeds `newMaxPages`.
    ///
    /// On ``CapacityPolicy/fifo`` volumes, reduction physically drops every row with
    /// `ROWID > newMaxPages` and truncates the data file accordingly.
    ///
    /// - Parameters:
    ///   - maxPages: New maximum page count; must be ≥ 1.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall/invalidArguments`` if `maxPages` is ≤ 0;
    ///   ``VolumeSetMaxPagesError`` if the reduction would discard pages on a FIXED volume.
    func setNewMaxPages(_ maxPages: Int, durable: Bool = true) throws {
        guard maxPages > 0, maxPages <= INT_MAX else {
            throw InvalidCall.invalidArguments
        }

        try b_setMaxPages(handle: handle, newMaxPages: UInt32(maxPages), durable: durable)
    }

    /// Preallocates space in an already-open volume.
    ///
    /// - Parameters:
    ///   - database: If `true`, preallocate slots in the index for all `maxPages` pages.
    ///   - datafile: If `true`, extend the data file to its maximum size.
    ///   - durable: If `true`, block until data is durable on disk.
    ///
    /// - Throws: ``InvalidCall/invalidArguments`` if both flags are `false`;
    ///   ``POSIXError`` or ``SQLiteError`` if preallocation fails.
    func preallocate(database: Bool, datafile: Bool, durable: Bool = true) throws {
        guard database || datafile else {
            throw InvalidCall.invalidArguments
        }

        try b_preallocate(handle: handle, database: database, datafile: datafile, durable: durable)
    }
}

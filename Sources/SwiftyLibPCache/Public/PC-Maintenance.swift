//
//  PC-Maintenance.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

import Foundation

public extension PersistentCache {
    func defragment(shrinkFile: Bool, durable: Bool = true, progress: @escaping @Sendable (Double) -> Bool) throws {
        try b_defragment(handle: handle, progress: progress, shrinkFile: shrinkFile, durable: durable)
    }
    
    func setNewMaxPages(_ maxPages: Int, durable: Bool = true) throws {
        guard maxPages > 0, maxPages <= UINT32_MAX else {
            throw InvalidCall.invalidArguments
        }
        
        try b_setMaxPages(handle: handle, newMaxPages: UInt32(maxPages), durable: durable)
    }
    
    func prealocate(database: Bool, datafile: Bool, durable: Bool = true) throws {
        guard database || database else {
            throw InvalidCall.invalidArguments
        }
        
        try b_preallocate(handle: handle, database: database, datafile: datafile, durable: durable)
    }
}

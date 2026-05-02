//
//  PC-Lifecycle.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

import Foundation

public final class PersistentCache: Sendable {
    let handle: Handle
    
    init(handle: Handle) {
        self.handle = handle
    }
}

public extension PersistentCache {
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
    
    convenience init(files: FilePair) throws {
        let h = try b_open(paths: files)
        self.init(handle: h)
    }
    
    func close() throws {
        try b_close(handle: handle)
    }
}

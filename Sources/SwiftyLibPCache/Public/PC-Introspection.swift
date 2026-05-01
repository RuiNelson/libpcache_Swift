//
//  PC-Introspection.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

public extension PersistentCache {
    var configuration: Configuration {
        // The handle is managed internally, there is no possibility of providing an invalid handle to the C library
        try! b_inspectConfiguration(handle: handle)
    }
    
    func pageCounts() throws -> PageCount {
        try b_inspectPageCount(handle: handle)
    }
}

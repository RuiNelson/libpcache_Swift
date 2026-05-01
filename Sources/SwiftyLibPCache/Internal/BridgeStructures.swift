//
//  BridgeStructures.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

import CLibPCache
import Foundation

extension Configuration {
    init(_ c: pcache_configuration) {
        self.capacityPolicy = CapacityPolicy(c.capacity_policy)
        self.pageSize = c.page_size
        self.maxPages = c.max_pages
        self.idWidth = c.id_size
    }
    
    var cValue: pcache_configuration {
        pcache_configuration(
            capacity_policy: capacityPolicy.cValue,
            page_size: pageSize,
            max_pages: maxPages,
            id_size: idWidth,
        )
    }
}

extension PageCount {
    init(_ c: pcache_page_count) {
        self.used = Int(c.used)
        self.free = Int(c.free)
    }
}

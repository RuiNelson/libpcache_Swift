//
//  BridgeEnums.swift
//
//  MIT 2-Claude License.
//

import Foundation
import pcache

/// Converts C `pcache_capacity_policy` to Swift ``Endianness``.
extension CapacityPolicy {
    init(_ c: pcache_capacity_policy) {
        switch c {
        case PCACHE_CAPACITY_FIFO: self = .fifo
        default: self = .fixed
        }
    }

    var cValue: pcache_capacity_policy {
        switch self {
        case .fixed: PCACHE_CAPACITY_FIXED
        case .fifo: PCACHE_CAPACITY_FIFO
        }
    }
}

/// Converts Swift ``Endianness`` to C `pcache_endianness`.
extension Endianness {
    var cValue: pcache_endianness {
        switch self {
        case .native: PCACHE_ENDIANNESS_NATIVE
        case .littleEndian: PCACHE_ENDIANNESS_LITTLE_ENDIAN
        case .bigEndian: PCACHE_ENDIANNESS_BIG_ENDIAN
        }
    }
}

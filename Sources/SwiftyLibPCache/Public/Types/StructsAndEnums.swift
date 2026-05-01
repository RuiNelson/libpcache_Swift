//
//  StructsAndEnums.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

import Foundation

public struct FilePair: Sendable {
    let databaseURL: URL
    let dataURL: URL
    
    public init?(databaseURL: URL, dataURL: URL) {
        guard dataURL.isFileURL, dataURL.isFileURL else {
            return nil
        }
        
        self.databaseURL = databaseURL
        self.dataURL = dataURL
    }
}

public struct Configuration: Sendable {
    let pageSize: UInt32
    let maxPages: UInt32
    let idWidth: UInt32
    let capacityPolicy: CapacityPolicy
    
    var pageSizeInt: Int {
        Int(pageSize)
    }
    
    var maxPagesInt: Int {
        Int(maxPages)
    }
    
    var idWidthInt: Int {
        Int(idWidth)
    }
    
    public init?(pageSize: Int, maxPages: Int, idWidth: Int, capacityPolicy: CapacityPolicy) {
        guard pageSize > 0, maxPages > 0, idWidth > 0 else {
            return nil
        }
        
        guard pageSize <= UINT32_MAX, maxPages <= UINT32_MAX, idWidth <= UINT32_MAX else {
            return nil
        }
        
        self.pageSize = UInt32(pageSize)
        self.maxPages = UInt32(maxPages)
        self.idWidth = UInt32(idWidth)
        self.capacityPolicy = capacityPolicy
    }
    
    public init?(capacity: Int64, pageSize: Int, idWidth: Int, capacityPolicy: CapacityPolicy) {
        guard capacity >= pageSize, capacity % Int64(pageSize) == 0 else {
            return nil
        }
        
        let maxPages = capacity / Int64(pageSize)
        
        guard let new = Configuration(
            pageSize: pageSize,
            maxPages: Int(maxPages),
            idWidth: idWidth,
            capacityPolicy: capacityPolicy,
        ) else {
            return nil
        }
        
        self = new
    }
}

public struct PageCount {
    public var used: Int
    public var free: Int
}

public enum CapacityPolicy: Sendable {
    case fixed
    case fifo
}

public enum Endianness: Sendable {
    case native
    case littleEndian
    case bigEndian
}

public struct Counter: Sendable {
    let template: Data
    let position: UInt32
    var initialValue: UInt32
    let endianess: Endianness
    
    public init(
        template: Data,
        zeroPad: Int = 0,
        position: Int = 0,
        initialValue: Int = 0,
        endianess: Endianness = .littleEndian,
    ) {
        var template = template
        
        if zeroPad > 0 {
            template.append(contentsOf: .init(repeating: 0, count: zeroPad))
        }
        
        self.template = template
        self.position = UInt32(position)
        self.initialValue = UInt32(initialValue)
        self.endianess = endianess
    }
    
    public mutating func advance(_ by: Int) {
        initialValue = initialValue + UInt32(by)
    }
    
    public mutating func backwards(_ by: Int) {
        initialValue = initialValue - UInt32(by)
    }
    
    var templateWidth: Int {
        template.count
    }
}

public typealias UnsafeBuffer = (pointer: UnsafeRawPointer, count: Int)
public typealias UnsafeMutableBuffer = (pointer: UnsafeMutableRawPointer, count: Int)

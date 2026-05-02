//
//  PC-Delete.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

// MARK: - C

public extension PersistentCache {
    func deletePage(
        id: CBuffer,
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        guard id.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        try b_deletePage(
            handle: handle,
            id: id.pointer,
            wipeDataFile: wipeDataFile,
            durable: durable,
        )
    }
    
    func deletePages(
        ids: CBuffer,
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        guard ids.count % configuration.idWidthInt == 0 else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        let count = ids.count / configuration.idWidthInt
        
        try b_deletePages(
            handle: handle,
            count: count,
            ids: ids.pointer,
            wipeDataFile: wipeDataFile,
            durable: durable,
        )
    }
    
    func deletePages(
        counter: Counter,
        count: Int,
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        guard counter.templateWidth == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        try counter.template.withUnsafeBytes { counterBuf in
            try b_deletePagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.baseAddress!,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianess,
                wipeDataFile: wipeDataFile,
                durable: durable,
            )
        }
    }
    
    func deletePagesRange(
        first: CBuffer,
        last: CBuffer,
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        guard first.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        guard last.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        try b_deletePagesRange(
            handle: handle,
            first: first.pointer,
            last: last.pointer,
            wipeDataFile: wipeDataFile,
            durable: durable,
        )
    }
}

// MARK: - Swift

public extension PersistentCache {
    func deletePage(
        id: RawSpan,
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            try deletePage(
                id: (idBuf.baseAddress!, idBuf.count),
                wipeDataFile: wipeDataFile,
                durable: durable,
            )
        }
    }
    
    func deletePages(
        ids: RawSpan,
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            try deletePages(
                ids: (idsBuf.baseAddress!, idsBuf.count),
                wipeDataFile: wipeDataFile,
                durable: durable,
            )
        }
    }
    
    func deletePagesRange(
        first: RawSpan,
        last: RawSpan,
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try deletePagesRange(
                    first: (firstBuf.baseAddress!, firstBuf.count),
                    last: (lastBuf.baseAddress!, lastBuf.count),
                    wipeDataFile: wipeDataFile,
                    durable: durable,
                )
            }
        }
    }
}

// MARK: - Foundation

import Foundation

public extension PersistentCache {
    func deletePage(
        id: Data,
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            try deletePage(
                id: (idBuf.baseAddress!, idBuf.count),
                wipeDataFile: wipeDataFile,
                durable: durable,
            )
        }
    }
    
    func deletePages(
        ids: Data,
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            try deletePages(
                ids: (idsBuf.baseAddress!, idsBuf.count),
                wipeDataFile: wipeDataFile,
                durable: durable,
            )
        }
    }
    
    func deletePagesRange(
        first: Data,
        last: Data,
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try deletePagesRange(
                    first: (firstBuf.baseAddress!, firstBuf.count),
                    last: (lastBuf.baseAddress!, lastBuf.count),
                    wipeDataFile: wipeDataFile,
                    durable: durable,
                )
            }
        }
    }
}

// MARK: - Foundation (Arrays)

public extension PersistentCache {
    func deletePages(
        ids: [Data],
        wipeDataFile: Bool = false,
        durable: Bool = true,
    ) throws {
        for id in ids {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
        }
        
        let idsSquashed = ids.squashed()
        
        try idsSquashed.withUnsafeBytes { idsBuf in
            try deletePages(
                ids: (idsBuf.baseAddress!, idsBuf.count),
                wipeDataFile: wipeDataFile,
                durable: durable,
            )
        }
    }
}

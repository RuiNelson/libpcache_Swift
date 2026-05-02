//
//  PC-Check.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

// MARK: - C

public extension PersistentCache {
    func checkPage(id: CBuffer) throws -> Bool {
        try validateIDBuffer(id)
        return try b_checkPage(handle: handle, id: id.pointer)
    }
    
    func checkPages(ids: CBuffer, results: UnsafeMutablePointer<Bool>) throws {
        let count = try itemCount(fromIDs: ids)
        try b_checkPages(handle: handle, count: count, ids: ids.pointer, results: results)
    }
    
    func checkPages(counter: Counter, count: Int, results: UnsafeMutablePointer<Bool>) throws {
        try validateCounter(counter)
        try counter.template.withUnsafeBytes { counterBuf in
            try b_checkPagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.baseAddress!,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianess,
                results: results,
            )
        }
    }
    
    func checkPagesRange(first: CBuffer, last: CBuffer) throws -> Int {
        try validateIDBuffer(first)
        try validateIDBuffer(last)
        return try b_checkPagesRange(handle: handle, first: first.pointer, last: last.pointer)
    }
}

// MARK: - Swift

public extension PersistentCache {
    func checkPage(id: RawSpan) throws -> Bool {
        try id.withUnsafeBytes { try checkPage(id: ($0.baseAddress!, $0.count)) }
    }
    
    func checkPages(ids: RawSpan) throws -> [Bool] {
        try ids.withUnsafeBytes { idsBuf in
            let count = idsBuf.count / configuration.idWidthInt
            var results = [Bool](repeating: false, count: count)
            try checkPages(ids: (idsBuf.baseAddress!, idsBuf.count), results: &results)
            return results
        }
    }
    
    func checkPages(counter: Counter, count: Int) throws -> [Bool] {
        var results = [Bool](repeating: false, count: count)
        try checkPages(counter: counter, count: count, results: &results)
        return results
    }
    
    internal func checkPagesRange(first: RawSpan, last: RawSpan) throws -> Int {
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try checkPagesRange(
                    first: (firstBuf.baseAddress!, firstBuf.count),
                    last: (lastBuf.baseAddress!, lastBuf.count),
                )
            }
        }
    }
}

// MARK: - Foundation

import Foundation

public extension PersistentCache {
    func checkPage(id: some ContiguousBytes) throws -> Bool {
        try id.withUnsafeBytes { try checkPage(id: ($0.baseAddress!, $0.count)) }
    }

    func checkPages(ids: some ContiguousBytes) throws -> [Bool] {
        try ids.withUnsafeBytes { idsBuf in
            let count = idsBuf.count / configuration.idWidthInt
            var results = [Bool](repeating: false, count: count)
            try checkPages(ids: (idsBuf.baseAddress!, idsBuf.count), results: &results)
            return results
        }
    }

    internal func checkPagesRange(first: some ContiguousBytes, last: some ContiguousBytes) throws -> Int {
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try checkPagesRange(
                    first: (firstBuf.baseAddress!, firstBuf.count),
                    last: (lastBuf.baseAddress!, lastBuf.count),
                )
            }
        }
    }
}

// MARK: - Foundation (Arrays)

public extension PersistentCache {
    func checkPages(ids: [Data]) throws -> [Bool] {
        for id in ids {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
        }
        
        let idsSquashed = ids.squashed()
        
        return try idsSquashed.withUnsafeBytes { idsBuf in
            var results = [Bool](repeating: false, count: ids.count)
            try checkPages(ids: (idsBuf.baseAddress!, idsBuf.count), results: &results)
            return results
        }
    }
}

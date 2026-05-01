//
//  PC-Write.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

// MARK: - C

public extension PersistentCache {
    func putPage(
        id: UnsafeBuffer,
        data: UnsafeBuffer,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        guard id.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        guard data.count == configuration.pageSizeInt else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        
        try b_putPage(
            handle: handle,
            id: id.pointer,
            pageData: data.pointer,
            failIfExists: failIfExists,
            durable: durable,
        )
    }
    
    func putPages(
        ids: UnsafeBuffer,
        data: UnsafeBuffer,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        guard ids.count % configuration.idWidthInt == 0 else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        guard data.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        
        let count1 = ids.count / configuration.idWidthInt
        let count2 = data.count / configuration.pageSizeInt
        
        guard count1 == count2 else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }
        
        let count = count1 // == count2
        
        try b_putPages(
            handle: handle,
            count: count,
            ids: ids.pointer,
            pagesData: data.pointer,
            failIfExists: failIfExists,
            durable: durable,
        )
    }
    
    func putPages(
        counter: Counter,
        data: UnsafeBuffer,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        guard counter.templateWidth == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        guard data.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        
        let count = data.count / configuration.pageSizeInt
        
        try counter.template.withUnsafeBytes { counterBuf in
            try b_putPagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.baseAddress!,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianess,
                pagesData: data.pointer,
                failIfExists: failIfExists,
                durable: durable,
            )
        }
    }
}

// MARK: - Swift

public extension PersistentCache {
    func putPage(
        id: RawSpan,
        data: RawSpan,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPage(
                    id: (idBuf.baseAddress!, idBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }
    
    func putPages(
        ids: RawSpan,
        data: RawSpan,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }
    
    func putPages(counter: Counter, data: RawSpan, failIfExists: Bool = false, durable: Bool = true) throws {
        try data.withUnsafeBytes { dataBuf in
            try putPages(
                counter: counter,
                data: (dataBuf.baseAddress!, dataBuf.count),
                failIfExists: failIfExists,
                durable: durable,
            )
        }
    }
}

// MARK: - Foundation

import Foundation

public extension PersistentCache {
    func putPage(
        id: some ContiguousBytes,
        data: some ContiguousBytes,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPage(
                    id: (idBuf.baseAddress!, idBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }

    func putPages(
        ids: some ContiguousBytes,
        data: some ContiguousBytes,
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        try ids.withUnsafeBytes { idsBuf in
            try data.withUnsafeBytes { dataBuf in
                try putPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }

    func putPages(counter: Counter, data: some ContiguousBytes, failIfExists: Bool = false, durable: Bool = true) throws {
        try data.withUnsafeBytes { dataBuf in
            try putPages(
                counter: counter,
                data: (dataBuf.baseAddress!, dataBuf.count),
                failIfExists: failIfExists,
                durable: durable,
            )
        }
    }
}

// MARK: - Foundation (Arrays)

extension [Data] {
    func squashed() -> Data {
        let totalSize = self.reduce(0) { $0 + $1.count }
        
        guard totalSize > 0 else {
            return Data()
        }
        
        var result = Data(count: totalSize)
        result.withUnsafeMutableBytes { buffer in
            var offset = 0
            let base = buffer.baseAddress!.assumingMemoryBound(to: UInt8.self)
            for chunk in self {
                chunk.copyBytes(to: base.advanced(by: offset), count: chunk.count)
                offset += chunk.count
            }
        }
        return result
    }
}

public extension PersistentCache {
    func putPages(ids: [Data], data: [Data], failIfExists: Bool = false, durable: Bool = true) throws {
        guard ids.count == data.count else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }
        
        for id in ids {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
        }
        
        for page in data {
            guard page.count == configuration.pageSizeInt else {
                throw InvalidCall.dataBufferIsNotTheExpectedSize
            }
        }
        
        let idsSquashed = ids.squashed()
        let dataSquashed = data.squashed()
        
        try idsSquashed.withUnsafeBytes { idsBuf in
            try dataSquashed.withUnsafeBytes { dataBuf in
                try putPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }
    
    func putPages(counter: Counter, data: [Data], failIfExists: Bool = false, durable: Bool = true) throws {
        guard counter.templateWidth == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        for page in data {
            guard page.count == configuration.pageSizeInt else {
                throw InvalidCall.dataBufferIsNotTheExpectedSize
            }
        }
        
        let dataSquashed = data.squashed()
        
        try dataSquashed.withUnsafeBytes { dataBuf in
            try putPages(
                counter: counter,
                data: (dataBuf.baseAddress!, dataBuf.count),
                failIfExists: failIfExists,
                durable: durable,
            )
        }
    }
}

// MARK: - Foundation (Tuples)

public extension PersistentCache {
    func putPages(
        pages: [(id: Data, data: Data)],
        failIfExists: Bool = false,
        durable: Bool = true,
    ) throws {
        for (id, data) in pages {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
            
            guard data.count == configuration.pageSizeInt else {
                throw InvalidCall.dataBufferIsNotTheExpectedSize
            }
        }
        
        let idsSquashed = pages.map(\.id).squashed()
        let dataSquashed = pages.map(\.data).squashed()
        
        try idsSquashed.withUnsafeBytes { idsBuf in
            try dataSquashed.withUnsafeBytes { dataBuf in
                try putPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                    failIfExists: failIfExists,
                    durable: durable,
                )
            }
        }
    }
}

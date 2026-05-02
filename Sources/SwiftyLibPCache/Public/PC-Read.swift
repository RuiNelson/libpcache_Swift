//
//  PC-Read.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

// MARK: - C

public extension PersistentCache {
    func getPage(id: CBuffer, data: CMutableBuffer) throws {
        guard id.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        guard data.count == configuration.pageSizeInt else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        
        try b_getPage(handle: handle, id: id.pointer, pageData: data.pointer)
    }
    
    func getPages(ids: CBuffer, data: CMutableBuffer) throws {
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
        
        try b_getPages(handle: handle, count: count, ids: ids.pointer, pageData: data.pointer)
    }
    
    func getPages(
        counter: Counter,
        data: CMutableBuffer,
    ) throws {
        guard counter.templateWidth == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        guard data.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        
        let count = data.count / configuration.pageSizeInt
        
        try counter.template.withUnsafeBytes { counterBuf in
            try b_getPagesWithCounter(
                handle: handle,
                count: count,
                idBase: counterBuf.baseAddress!,
                start: counter.initialValue,
                position: counter.position,
                endianness: counter.endianess,
                pageData: data.pointer,
            )
        }
    }
    
    func getPagesRange(
        first: CBuffer,
        last: CBuffer,
        idsOut: CMutableBuffer,
        pagesOut: CMutableBuffer,
    ) throws -> Int {
        guard first.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        guard last.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        guard idsOut.count % configuration.idWidthInt == 0 else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
        
        guard pagesOut.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
        
        let idCapacity = idsOut.count / configuration.idWidthInt
        let pageCapacity = pagesOut.count / configuration.pageSizeInt
        
        guard idCapacity == pageCapacity else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }
        
        let bufferCapacity = UInt32(idCapacity)
        
        return try b_getPagesRange(
            handle: handle,
            first: first.pointer,
            last: last.pointer,
            idsOut: idsOut.pointer,
            pagesOut: pagesOut.pointer,
            bufferCapacity: bufferCapacity,
        )
    }
}

// MARK: - Swift/Foundation

import Foundation

public extension PersistentCache {
    func getPage(id: RawSpan) throws -> [UInt8] {
        var data = [UInt8](repeating: 0, count: configuration.pageSizeInt)
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeMutableBytes { dataBuf in
                try getPage(
                    id: (idBuf.baseAddress!, idBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                )
            }
        }
        return data
    }
    
    func getPages(ids: RawSpan) throws -> Data {
        try ids.withUnsafeBytes { idsBuf in
            let count = idsBuf.count / configuration.idWidthInt
            var data = Data(count: count * configuration.pageSizeInt)
            try data.withUnsafeMutableBytes { dataBuf in
                try getPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                )
            }
            return data
        }
    }
    
    func getPagesRange(first: RawSpan, last: RawSpan) throws -> (ids: Data, pages: Data) {
        let bufferCapacity = try checkPagesRange(first: first, last: last)
        
        guard bufferCapacity > 0 else {
            return (Data(), Data())
        }
        
        var idsOut = Data(count: bufferCapacity * configuration.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * configuration.pageSizeInt)
        
        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        _ = try getPagesRange(
                            first: (firstBuf.baseAddress!, firstBuf.count),
                            last: (lastBuf.baseAddress!, lastBuf.count),
                            idsOut: (idsBuf.baseAddress!, idsBuf.count),
                            pagesOut: (pagesBuf.baseAddress!, pagesBuf.count),
                        )
                    }
                }
            }
        }
        
        return (idsOut, pagesOut)
    }
}

// MARK: - Foundation

public extension PersistentCache {
    func getPage(id: some ContiguousBytes) throws -> Data {
        var data = Data(count: configuration.pageSizeInt)
        try id.withUnsafeBytes { idBuf in
            try data.withUnsafeMutableBytes { dataBuf in
                try getPage(
                    id: (idBuf.baseAddress!, idBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                )
            }
        }
        return data
    }

    func getPages(ids: some ContiguousBytes) throws -> Data {
        try ids.withUnsafeBytes { idsBuf in
            let count = idsBuf.count / configuration.idWidthInt
            var data = Data(count: count * configuration.pageSizeInt)
            try data.withUnsafeMutableBytes { dataBuf in
                try getPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                )
            }
            return data
        }
    }

    func getPages(counter: Counter, count: Int) throws -> Data {
        var data = Data(count: count * configuration.pageSizeInt)
        try data.withUnsafeMutableBytes { dataBuf in
            try getPages(
                counter: counter,
                data: (dataBuf.baseAddress!, dataBuf.count),
            )
        }
        return data
    }

    func getPagesRange(first: some ContiguousBytes, last: some ContiguousBytes) throws -> (ids: Data, pages: Data) {
        let bufferCapacity = try checkPagesRange(first: first, last: last)

        guard bufferCapacity > 0 else {
            return (Data(), Data())
        }

        var idsOut = Data(count: bufferCapacity * configuration.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * configuration.pageSizeInt)

        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        _ = try getPagesRange(
                            first: (firstBuf.baseAddress!, firstBuf.count),
                            last: (lastBuf.baseAddress!, lastBuf.count),
                            idsOut: (idsBuf.baseAddress!, idsBuf.count),
                            pagesOut: (pagesBuf.baseAddress!, pagesBuf.count),
                        )
                    }
                }
            }
        }

        return (idsOut, pagesOut)
    }

    func getPagesRange(first: some ContiguousBytes, last: some ContiguousBytes) throws -> [(id: Data, page: Data)] {
        let bufferCapacity = try checkPagesRange(first: first, last: last)

        guard bufferCapacity > 0 else {
            return []
        }

        var idsOut = Data(count: bufferCapacity * configuration.idWidthInt)
        var pagesOut = Data(count: bufferCapacity * configuration.pageSizeInt)

        try first.withUnsafeBytes { firstBuf in
            try last.withUnsafeBytes { lastBuf in
                try idsOut.withUnsafeMutableBytes { idsBuf in
                    try pagesOut.withUnsafeMutableBytes { pagesBuf in
                        _ = try getPagesRange(
                            first: (firstBuf.baseAddress!, firstBuf.count),
                            last: (lastBuf.baseAddress!, lastBuf.count),
                            idsOut: (idsBuf.baseAddress!, idsBuf.count),
                            pagesOut: (pagesBuf.baseAddress!, pagesBuf.count),
                        )
                    }
                }
            }
        }

        // Split into tuples
        var result = [(id: Data, page: Data)]()
        result.reserveCapacity(bufferCapacity)
        for i in 0 ..< bufferCapacity {
            let idStart = i * configuration.idWidthInt
            let idEnd = idStart + configuration.idWidthInt
            let pageStart = i * configuration.pageSizeInt
            let pageEnd = pageStart + configuration.pageSizeInt
            result.append((id: idsOut[idStart ..< idEnd], page: pagesOut[pageStart ..< pageEnd]))
        }
        return result
    }
}

// MARK: - Foundation (Arrays)

public extension PersistentCache {
    func getPages(ids: [Data]) throws -> Data {
        for id in ids {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
        }
        
        let idsSquashed = ids.squashed()
        
        return try idsSquashed.withUnsafeBytes { idsBuf in
            var data = Data(count: ids.count * configuration.pageSizeInt)
            try data.withUnsafeMutableBytes { dataBuf in
                try getPages(
                    ids: (idsBuf.baseAddress!, idsBuf.count),
                    data: (dataBuf.baseAddress!, dataBuf.count),
                )
            }
            return data
        }
    }
    
    func getPages(ids: [Data]) throws -> [Data] {
        let dataSquashed: Data = try getPages(ids: ids)
        return (0 ..< ids.count).map { i in
            let start = i * configuration.pageSizeInt
            return dataSquashed[start ..< start + configuration.pageSizeInt]
        }
    }
}

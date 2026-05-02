//
//  ValidationHelpers.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

import Foundation

// MARK: - Buffer Validation

extension PersistentCache {
    /// Validates that a single ID buffer has the correct size
    func validateIDBuffer(_ buffer: CBuffer) throws {
        guard buffer.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
    }
    
    /// Validates that an IDs buffer contains complete IDs (count is multiple of idWidth)
    func validateIDsBuffer(_ buffer: CBuffer) throws {
        guard buffer.count % configuration.idWidthInt == 0 else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
    }
    
    /// Validates that a single data buffer has the correct page size
    func validateDataBuffer(_ buffer: CBuffer) throws {
        guard buffer.count == configuration.pageSizeInt else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
    }
    
    /// Validates that a single mutable data buffer has the correct page size
    func validateDataBuffer(_ buffer: CMutableBuffer) throws {
        guard buffer.count == configuration.pageSizeInt else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
    }
    
    /// Validates that a pages buffer contains complete pages (count is multiple of pageSize)
    func validatePagesBuffer(_ buffer: CBuffer) throws {
        guard buffer.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
    }
    
    /// Validates that a mutable pages buffer contains complete pages
    func validatePagesBuffer(_ buffer: CMutableBuffer) throws {
        guard buffer.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
    }
    
    /// Validates that a counter's template width matches the expected ID width
    func validateCounter(_ counter: Counter) throws {
        guard counter.templateWidth == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
    }
    
    /// Validates that an array of Data IDs all have the correct size
    func validateIDArray(_ ids: [Data]) throws {
        for id in ids {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
        }
    }
    
    /// Validates that an array of Data pages all have the correct size
    func validatePageArray(_ pages: [Data]) throws {
        for page in pages {
            guard page.count == configuration.pageSizeInt else {
                throw InvalidCall.dataBufferIsNotTheExpectedSize
            }
        }
    }
    
    /// Calculates item count from IDs buffer and validates
    func itemCount(fromIDs buffer: CBuffer) throws -> Int {
        try validateIDsBuffer(buffer)
        return buffer.count / configuration.idWidthInt
    }
    
    /// Calculates item count from pages buffer and validates
    func itemCount(fromPages buffer: CBuffer) throws -> Int {
        try validatePagesBuffer(buffer)
        return buffer.count / configuration.pageSizeInt
    }
    
    /// Calculates item count from mutable pages buffer and validates
    func itemCount(fromPages buffer: CMutableBuffer) throws -> Int {
        try validatePagesBuffer(buffer)
        return buffer.count / configuration.pageSizeInt
    }
    
    /// Validates that IDs and pages buffers have matching item counts
    func validateMatchingCounts(ids: CBuffer, pages: CBuffer) throws -> Int {
        let idCount = try itemCount(fromIDs: ids)
        let pageCount = try itemCount(fromPages: pages)
        guard idCount == pageCount else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }
        return idCount
    }
    
    /// Validates that IDs and mutable pages buffers have matching item counts
    func validateMatchingCounts(ids: CBuffer, pages: CMutableBuffer) throws -> Int {
        let idCount = try itemCount(fromIDs: ids)
        let pageCount = try itemCount(fromPages: pages)
        guard idCount == pageCount else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }
        return idCount
    }
}

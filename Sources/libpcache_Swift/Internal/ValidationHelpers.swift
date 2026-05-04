//
//  ValidationHelpers.swift
//
//  MIT 2-Claude License.
//

import Foundation

// MARK: - Buffer Validation

extension PersistentCache {
    /// Validates that a single ID buffer has the correct size.
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if size mismatch.
    func validateIDBuffer(_ buffer: CBuffer) throws {
        let configuration = try self.configuration
        guard buffer.count == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
    }

    /// Validates that an IDs buffer contains complete IDs.
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if not divisible by idWidth.
    func validateIDsBuffer(_ buffer: CBuffer) throws {
        let configuration = try self.configuration
        guard buffer.count % configuration.idWidthInt == 0 else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
    }

    /// Validates that a data buffer has the correct page size.
    /// - Throws: ``InvalidCall/dataBufferIsNotTheExpectedSize`` if size mismatch.
    func validateDataBuffer(_ buffer: CBuffer) throws {
        let configuration = try self.configuration
        guard buffer.count == configuration.pageSizeInt else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
    }

    /// Validates that a mutable data buffer has the correct page size.
    /// - Throws: ``InvalidCall/dataBufferIsNotTheExpectedSize`` if size mismatch.
    func validateDataBuffer(_ buffer: CMutableBuffer) throws {
        let configuration = try self.configuration
        guard buffer.count == configuration.pageSizeInt else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
    }

    /// Validates that a pages buffer contains complete pages.
    /// - Throws: ``InvalidCall/dataBufferIsNotTheExpectedSize`` if not divisible by pageSize.
    func validatePagesBuffer(_ buffer: CBuffer) throws {
        let configuration = try self.configuration
        guard buffer.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
    }

    /// Validates that a mutable pages buffer contains complete pages.
    /// - Throws: ``InvalidCall/dataBufferIsNotTheExpectedSize`` if not divisible by pageSize.
    func validatePagesBuffer(_ buffer: CMutableBuffer) throws {
        let configuration = try self.configuration
        guard buffer.count % configuration.pageSizeInt == 0 else {
            throw InvalidCall.dataBufferIsNotTheExpectedSize
        }
    }

    /// Validates that a counter's template width matches the expected ID width.
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if width mismatch.
    func validateCounter(_ counter: Counter) throws {
        let configuration = try self.configuration
        guard counter.templateWidth == configuration.idWidthInt else {
            throw InvalidCall.idBufferIsNotTheExpectedSize
        }
    }

    /// Validates that an array of Data IDs all have the correct size.
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if any ID has wrong size.
    func validateIDArray(_ ids: [Data]) throws {
        let configuration = try self.configuration
        for id in ids {
            guard id.count == configuration.idWidthInt else {
                throw InvalidCall.idBufferIsNotTheExpectedSize
            }
        }
    }

    /// Validates that an array of Data pages all have the correct size.
    /// - Throws: ``InvalidCall/dataBufferIsNotTheExpectedSize`` if any page has wrong size.
    func validatePageArray(_ pages: [Data]) throws {
        let configuration = try self.configuration
        for page in pages {
            guard page.count == configuration.pageSizeInt else {
                throw InvalidCall.dataBufferIsNotTheExpectedSize
            }
        }
    }

    /// Calculates item count from IDs buffer.
    /// - Returns: Number of IDs (buffer.count / idWidth).
    /// - Throws: ``InvalidCall/idBufferIsNotTheExpectedSize`` if buffer not properly sized.
    func itemCount(fromIDs buffer: CBuffer) throws -> Int {
        try validateIDsBuffer(buffer)
        let configuration = try self.configuration
        return buffer.count / configuration.idWidthInt
    }

    /// Calculates item count from pages buffer.
    /// - Returns: Number of pages (buffer.count / pageSize).
    /// - Throws: ``InvalidCall/dataBufferIsNotTheExpectedSize`` if buffer not properly sized.
    func itemCount(fromPages buffer: CBuffer) throws -> Int {
        try validatePagesBuffer(buffer)
        let configuration = try self.configuration
        return buffer.count / configuration.pageSizeInt
    }

    /// Calculates item count from mutable pages buffer.
    /// - Returns: Number of pages (buffer.count / pageSize).
    /// - Throws: ``InvalidCall/dataBufferIsNotTheExpectedSize`` if buffer not properly sized.
    func itemCount(fromPages buffer: CMutableBuffer) throws -> Int {
        try validatePagesBuffer(buffer)
        let configuration = try self.configuration
        return buffer.count / configuration.pageSizeInt
    }

    /// Validates that IDs and pages buffers have matching item counts.
    /// - Returns: The validated item count.
    /// - Throws: ``InvalidCall/numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer`` on mismatch.
    func validateMatchingCounts(ids: CBuffer, pages: CBuffer) throws -> Int {
        let idCount = try itemCount(fromIDs: ids)
        let pageCount = try itemCount(fromPages: pages)
        guard idCount == pageCount else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }
        return idCount
    }

    /// Validates that IDs and mutable pages buffers have matching item counts.
    /// - Returns: The validated item count.
    /// - Throws: ``InvalidCall/numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer`` on mismatch.
    func validateMatchingCounts(ids: CBuffer, pages: CMutableBuffer) throws -> Int {
        let idCount = try itemCount(fromIDs: ids)
        let pageCount = try itemCount(fromPages: pages)
        guard idCount == pageCount else {
            throw InvalidCall.numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
        }
        return idCount
    }
}

//
//  Errors.swift
//
//  Copyright Rui Nelson Magalhães Carneiro.
//

import Foundation

// MARK: Common errors

public enum CommonErrors: Error {
    case invalidHandle
    case outOfMemory
}

public struct SQLiteError: Error {
    public let code: Int32
    
    init(code: Int32) {
        self.code = code
    }
    
    init(code: Int32?) {
        self.init(code: code ?? 0)
    }
}

// MARK: Operation-specific errors

public enum CreateVolumeError: Error {
    case invalidArgument
    case fileExists
}

public enum OpenVolumeError: Error {
    case notFound
    case corrupt
    case schemaVersionTooHigh
}

public enum PutPagesError: Error {
    case capacityExceeded
    case duplicateID
    case invalidArgument
}

public enum GetPagesError: Error {
    case notFound
    case invalidArgument
    case rangeInvalidRange
    case rangeBufferTooSmall
}

public enum CheckPagesError: Error {
    case invalidArgument
    case rangeInvalidRange
}

public enum DeletePagesError: Error {
    case invalidRange
    case invalidArgument
}

public enum DefragmentVolumeError: Error {
    case cancelled
}

public enum VolumeSetMaxPagesError: Error {
    case wouldDiscardPages
}

// Unknown Error

public struct UnknownLibPCacheError: Error {
    let libpcacheCode: UInt32
    let sqlite3: Int32?
    let posix: POSIXError?
    
    init(libpcacheCode: UInt32, sqlite3: Int32?, posix: Int32?) {
        self.libpcacheCode = libpcacheCode
        self.sqlite3 = sqlite3
        if let posix {
            self.posix = POSIXError(code: posix)
        }
        else {
            self.posix = nil
        }
    }
}

// MARK: User Misuse

enum InvalidCall: Error {
    case invalidArguments
    case idBufferIsNotTheExpectedSize
    case dataBufferIsNotTheExpectedSize
    case numberOfItemsInIDBufferDoesNotMatchTheNumberOfItemsInDataBuffer
}

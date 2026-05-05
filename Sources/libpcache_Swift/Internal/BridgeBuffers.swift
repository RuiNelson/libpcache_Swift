//
//  BridgeBuffers.swift
//
//  MIT 2-Claude License.
//

import Foundation

/// A stable non-null pointer used in place of a `nil` `baseAddress` for empty buffers.
///
/// `Data().withUnsafeBytes`, an empty `RawSpan`, and similar containers may yield a buffer
/// whose `baseAddress` is `nil`. Forwarding such a `nil` to C is undefined behaviour even when
/// the count is zero, so we substitute a sentinel address. The C layer never reads from it
/// because every code path that uses this helper either validates the size first (rejecting
/// empties of the wrong shape) or early-returns when the count is zero.
private nonisolated(unsafe) let emptyBufferSentinel: UnsafeMutableRawPointer = {
    let bytes = UnsafeMutableRawPointer.allocate(byteCount: 1, alignment: 1)
    bytes.storeBytes(of: UInt8(0), as: UInt8.self)
    return bytes
}()

extension UnsafeRawBufferPointer {
    /// Returns a ``CBuffer`` view of this buffer pointer with a guaranteed non-null base.
    var cBuffer: CBuffer {
        (baseAddress ?? UnsafeRawPointer(emptyBufferSentinel), count)
    }
}

extension UnsafeMutableRawBufferPointer {
    /// Returns a ``CMutableBuffer`` view of this buffer pointer with a guaranteed non-null base.
    var cMutableBuffer: CMutableBuffer {
        (baseAddress ?? emptyBufferSentinel, count)
    }
}

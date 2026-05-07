# libpcache_Swift — Persistent Key-Value Cache for Swift

[![Swift 6.2+](https://img.shields.io/badge/Swift-6.2+-orange.svg)](https://swift.org)
[![Platform](https://img.shields.io/badge/platform-iOS%20%7C%20macOS%20%7C%20macCatalyst%20%7C%20watchOS%20%7C%20tvOS%20%7C%20visionOS%20%7C%20Linux-lightgrey.svg)](https://github.com/apple/swift-package-manager)
[![Swift Package Manager](https://img.shields.io/badge/SPM-compatible-brightgreen.svg)](https://swift.org/package-manager/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A high-performance persistent key-value cache for Swift — stores fixed-size pages on disk indexed by binary keys of arbitrary length. Built on SQLite for indexing and a binary data file for page storage. Thread-safe, atomic, with sequential counter-based batch I/O.

**Keywords:** Swift persistent cache, high-performance, key-value store, disk cache, SQLite storage, binary page cache, SwiftPM, iOS, macOS, Linux

For a complete description of the volume format, capacity policies, and C API, consult the reference manual of the wrapped library:

https://github.com/RuiNelson/libpcache

## Adding This Library To Your App

### Swift Package Manager

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/RuiNelson/libpcache_Swift.git", from: "1.0.0")
]
```

Add to your target dependencies:

```swift
.target(
    name: "YourTarget",
    dependencies: ["libpcache_Swift"]
)
```

### Xcode

1. File → Add Package Dependencies
2. Enter: `https://github.com/RuiNelson/libpcache_Swift.git`
3. Select version and add to your target

## Cookbook

```swift
import Foundation
import libpcache_Swift

let fourGB = Int(pow(2.0,32.0))

let pageSize  = 32 * 1024          // 32kB
let pageCount = fourGB / pageSize  // 4GB / 32kB = 131,072 pages
let idSize    = 36

// Create a volume. Both files must not exist yet.
let dbURL  = URL(fileURLWithPath: "my_volume.db")
let datURL = URL(fileURLWithPath: "my_volume.dat")
let files  = FilePair(databaseURL: dbURL, dataURL: datURL)!

let config = Configuration(
    pageSize: pageSize,
    maxPages: pageCount,
    idWidth: idSize,
    capacityPolicy: .fixed
)!

try PersistentCache.create(files: files, configuration: config)

// Open the volume.
let cache = try PersistentCache(files: files)

// Write a page. durable=true waits for the data to reach disk.
let id   = Data(repeating: 0xAB, count: idSize)
let page = Data(repeating: 0x42, count: pageSize)
try cache.putPage(id: id, data: page)

// Read the page back using the same key.
let retrieved: Data = try cache.getPage(id: id)
assert(retrieved == page)

// Delete the page. wipe=false keeps the bytes on disk —
// only the index entry is removed.
try cache.deletePage(id: id)

// Close the volume.
try cache.close()
```

## Cookbook (Counter)

The `Counter` mechanism generates sequential page identifiers from a template, so you never
need to manage keys by hand. Pass a `Counter` to `putPages` / `getPages` / `deletePages` and
the identifiers are derived automatically from the template, starting value, and byte order.
The write/read/delete operations themselves do not mutate the counter — call `advance(_:)` or
`backwards(_:)` explicitly to move it between batches.

```swift
let pageSize = 4096

var counter = Counter(
    template: Data(repeating: 0xAB, count: (idSize - 2)),
    zeroPad: 2,
    position: 0,
    initialValue: 0,
    endianess: .bigEndian
)

// Write pages 0..99
let batchData = Data(repeating: 0x12, count: pageSize * 100)
try cache.putPages(counter: counter, data: batchData)

// Advance and write pages 100..199
counter.advance(100)
let nextBatch = Data(repeating: 0x34, count: pageSize * 100)
try cache.putPages(counter: counter, data: nextBatch)

// Read back the second batch
let secondBatch: Data = try cache.getPages(counter: counter, count: 100)

// Rewind to read back the first batch
counter.backwards(100)
let firstBatch: Data = try cache.getPages(counter: counter, count: 100)

assert(firstBatch == batchData)
assert(secondBatch == nextBatch)
```

## Copyright/Licensing

Copyright (c) 2026 Rui Nelson.

Licensed under the MIT 2-Clause License

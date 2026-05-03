# libpcache_Swift

This document describes the Swift language binding for `libpcache`, a persistent storage system for fixed-size pages indexed by binary keys. The underlying implementation is a C library that maintains a SQLite database for the index and a binary file for page data. All operations are atomic and safe for concurrent use.

For a complete description of the volume format, capacity policies, and C API, consult the reference manual of the wrapped library:

https://github.com/RuiNelson/libpcache

## Adding This Library To Your App

### Swift Package Manager

Add the following entry to the dependencies array in `Package.swift`:

```swift
.package(url: "https://github.com/RuiNelson/libpcache_Swift.git", from: "1.0.0")
```

Then add `"libpcache_Swift"` to the target dependencies.

### Xcode

Open the project. Navigate to **Project → Package Dependencies**. Add a package dependency with the URL `https://github.com/RuiNelson/libpcache_Swift.git` and specify a version rule (up to next major version from `1.0.0`). Link the library to the application target.

## Cookbook

The following program creates a volume, stores a page, and retrieves it.

```swift
import Foundation
import libpcache_Swift

let dbURL  = URL(fileURLWithPath: "/tmp/volume.db")
let datURL = URL(fileURLWithPath: "/tmp/volume.dat")
let files  = FilePair(databaseURL: dbURL, dataURL: datURL)!

let config = Configuration(
    pageSize: 4096,
    maxPages: 1000,
    idWidth: 16,
    capacityPolicy: .fixed
)!

try PersistentCache.create(files: files, configuration: config)
let cache = try PersistentCache(files: files)

// The identifier must be exactly idWidth bytes.
var id = "Hello, World!!!".data(using: .ascii)!
id.append(contentsOf: repeatElement(0, count: 16 - id.count))

let page = Data(repeatElement(0x42, count: 4096))

try cache.putPage(id: id, data: page)

let retrieved: Data = try cache.getPage(id: id)
assert(retrieved == page)

try cache.close()
```

## Cookbook (Counter)

The `Counter` mechanism provides sequential page identifiers derived from a template. This is useful for batch operations where the caller does not wish to manage keys explicitly. The operation derives `count` identifiers starting from `initialValue`, but the `Counter` object itself is not modified.

```swift

var counter = Counter(
    template: Data(repeatElement(0xAB, count: 14)),
    zeroPad: 2,
    position: 0,
    initialValue: 0,
    endianess: .bigEndian
)

let batchData = Data(repeatElement(0xFE, count: 4096 * 100)) // pages 0..99
try cache.putPages(counter: counter, data: batchData)

// Advance the counter and write the next 100 pages (100..199).
counter.advance(100)
let nextBatch = Data(repeatElement(0xED, count: 4096 * 100))
try cache.putPages(counter: counter, data: nextBatch)

// Read back the second batch.
let secondBatch: Data = try cache.getPages(counter: counter, count: 100)

// Rewind to read back the first batch.
counter.backwards(100)
let firstBatch: Data = try cache.getPages(counter: counter, count: 100)

assert(firstBatch == batchData)
assert(secondBatch == nextBatch)
```

## Copyright/Licensing

Copyright (c) 2026 Rui Nelson.

Licensed under the MIT 2-Clause License

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) or other agents when working with code in this repository.

## Build & Test

```bash
swift build
swift test                    # run all tests
swift test --filter testName  # run specific test
```

## Formatting

Uses SwiftFormat. Config is in `.swiftformat` (4-space indent, `some`/`any` enabled).

```bash
swiftformat .
```

**Always run `swiftformat .` before creating any commit.**

## Architecture

SwiftyLibPCache is a Swift wrapper around a C library called `libpcache` (submodule at `Sources/CLibPCache`). The library implements a persistent cache backed by SQLite + a binary data file.

### Code Structure

```
Sources/SwiftyLibPCache/
├── Public/
│   ├── PC-Lifecycle.swift      # create/open/close PersistentCache
│   ├── PC-Read.swift           # getPage(s), getPagesRange
│   ├── PC-Write.swift          # putPage(s)
│   ├── PC-Check.swift          # checkPage(s), checkPagesRange
│   ├── PC-Delete.swift         # deletePage(s), deletePagesRange
│   ├── PC-Introspection.swift  # inspectConfiguration, inspectPageCount
│   ├── PC-Maintenance.swift    # defragment, setMaxPages, preallocate
│   └── Types/
│       └── StructsAndEnums.swift  # Configuration, Counter, FilePair, CapacityPolicy
└── Internal/
    ├── BridgeMethods.swift     # all C calls (b_putPage, b_getPage, etc.)
    ├── BridgeErrors.swift      # C error → Swift mapping
    ├── BridgeStructures.swift   # Swift ↔ C struct conversion
    ├── BridgeEnums.swift        # Swift ↔ C enum mapping
    └── ValidationHelpers.swift  # Buffer/parameter validation
```

### Layer Pattern

1. **C layer** (`UnsafeBuffer`/`UnsafeMutableBuffer`): raw pointer signatures
2. **Swift layer** (`RawSpan` = `UnsafeBuffer`): wrappers that manage pointer lifecycle
3. **Foundation layer** (`some ContiguousBytes`): public API accepting `Data`, `[UInt8]`, etc.

The type `Handle` is an alias for `pcache_handle` (opaque C type). The `PersistentCache` class holds the handle and exposes methods via extensions organized by functional domain.

### Key Types

- `PersistentCache`: main class, thread-safe (`Sendable`)
- `Configuration`: `pageSize`, `maxPages`, `idWidth`, `capacityPolicy`
- `Counter`: template for sequential IDs with endianness support
- `FilePair`: URL pair (database + datafile)

## Platform Support

- macOS 10.15+, iOS 13+, macCatalyst 13+, tvOS 12+, visionOS 1+, watchOS 4+
- Depends on `libpcache` (C) linked with `sqlite3` and `pthread` (Linux)

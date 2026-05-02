// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "libpcache_Swift",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .macCatalyst(.v13),
        .tvOS(.v12),
        .visionOS(.v1),
        .watchOS(.v4),
    ],
    products: [
        .library(
            name: "libpcache_Swift",
            targets: ["libpcache_Swift"],
        ),
    ],
    targets: [
        .target(
            name: "CLibPCache",
            path: "Sources/CLibPCache",
            sources: [
                "libpcache/src/db.c",
                "libpcache/src/handle.c",
                "libpcache/src/maintenance.c",
                "libpcache/src/pages.c",
                "libpcache/src/pages_util.c",
                "libpcache/src/volume.c",
                "xxhash/xxhash.c",
            ],
            publicHeadersPath: "libpcache/include",
            cSettings: [
                .headerSearchPath("libpcache/src"),
                .headerSearchPath("xxhash"),
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3"),
                .linkedLibrary("pthread", .when(platforms: [.linux])),
            ],
        ),
        .target(
            name: "libpcache_Swift",
            dependencies: ["CLibPCache"],
        ),
        .testTarget(
            name: "libpcache_SwiftTests",
            dependencies: ["libpcache_Swift"],
        ),
    ],
)

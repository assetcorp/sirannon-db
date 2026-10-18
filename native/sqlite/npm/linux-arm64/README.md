# @delali/sirannon-sqlite-linux-arm64

[![npm](https://img.shields.io/npm/v/@delali/sirannon-sqlite-linux-arm64)](https://www.npmjs.com/package/@delali/sirannon-sqlite-linux-arm64)
[![license](https://img.shields.io/npm/l/@delali/sirannon-sqlite-linux-arm64)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

This package contains better-sqlite3's native module, built for Linux on 64-bit Arm against Sirannon's build of SQLite, which counts the rows that each statement reads and writes.

## Install

When you install `@delali/sirannon-sqlite`, npm installs this package with it:

```bash
pnpm add -E @delali/sirannon-db @delali/sirannon-sqlite
```

[`@delali/sirannon-sqlite`](https://www.npmjs.com/package/@delali/sirannon-sqlite) lists all six platform packages as optional dependencies, so npm installs only the one whose `os` and `cpu` match your host. The release workflow publishes this package at the library's version, which is the exact version that `@delali/sirannon-sqlite` requires.

## What it does

When you pass `rowCounts: true`, Sirannon's better-sqlite3 driver loads this module and reports the rows that each statement reads and writes through `onQueryComplete`. The counting rules are in the [core engine guide](https://github.com/assetcorp/sirannon-db/blob/main/docs/core.md#rows-read-and-rows-written).

This package contains two builds, one for glibc and one for musl, because a native module built against one C library cannot load against the other. The JavaScript in `@delali/sirannon-sqlite` loads the musl build when Node's process report shows no glibc version.

## Licence

The Apache-2.0 and MIT licences cover this package. Each project whose code is in this package has its name and licence in the package's `NOTICE` file, and the patches, the build script, and the tests are in [assetcorp/sirannon-db](https://github.com/assetcorp/sirannon-db).

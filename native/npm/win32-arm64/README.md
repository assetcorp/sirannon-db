# @delali/sirannon-vfs-win32-arm64

[![npm](https://img.shields.io/npm/v/@delali/sirannon-vfs-win32-arm64)](https://www.npmjs.com/package/@delali/sirannon-vfs-win32-arm64)
[![license](https://img.shields.io/npm/l/@delali/sirannon-vfs-win32-arm64)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

This package holds one compiled SQLite extension, built for Windows on 64-bit Arm. The extension sends each piece of a Sirannon backup to the destination you supply, so a full copy writes no local file.

## Install

You install the library, and npm installs this package with it:

```bash
pnpm add -E @delali/sirannon-db
```

[`@delali/sirannon-db`](https://www.npmjs.com/package/@delali/sirannon-db) lists all six platform packages as optional dependencies, so npm installs only the one whose `os` and `cpu` match your host. The release workflow publishes this package at the library's version, and the library pins that version exactly.

## What it does

The extension registers a SQLite virtual file system named `sirannon`. Sirannon opens every copy through that file system, so the bytes reach no file on your disk. It takes each piece as SQLite writes it and passes that piece to your destination. Where this binary is absent, Sirannon reports `streamedCopy: false` and copies through a local file instead. Streaming also needs a runtime that parses URI file names, which the [backup documentation](https://github.com/assetcorp/sirannon-db/blob/main/docs/core.md) explains.

## Licence

This package is published under the Apache-2.0 licence. [assetcorp/sirannon-db](https://github.com/assetcorp/sirannon-db) holds the C source, the build script, and the tests.

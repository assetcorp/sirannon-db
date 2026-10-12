# @delali/sirannon-sqlite-darwin-arm64

[![npm](https://img.shields.io/npm/v/@delali/sirannon-sqlite-darwin-arm64)](https://www.npmjs.com/package/@delali/sirannon-sqlite-darwin-arm64)
[![license](https://img.shields.io/npm/l/@delali/sirannon-sqlite-darwin-arm64)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

This package holds better-sqlite3's native module, built for macOS on Apple silicon against a SQLite that counts the rows each statement reads and writes.

## Install

You install `@delali/sirannon-sqlite`, and npm installs this package with it:

```bash
pnpm add -E @delali/sirannon-db @delali/sirannon-sqlite
```

[`@delali/sirannon-sqlite`](https://www.npmjs.com/package/@delali/sirannon-sqlite) lists all six platform packages as optional dependencies, so npm installs only the one whose `os` and `cpu` match your host. The release workflow publishes this package at the library's version, and `@delali/sirannon-sqlite` pins that version exactly.

## What it does

Sirannon's better-sqlite3 driver loads this module when you pass `rowCounts: true`, and it reports each statement's rows read and rows written through `onQueryComplete`. The [core engine guide](https://github.com/assetcorp/sirannon-db/blob/main/docs/core.md#rows-read-and-rows-written) lists the counting rules.

## Licence

This package is published under the Apache-2.0 and MIT licences. Its `NOTICE` names the projects whose code it carries, and [assetcorp/sirannon-db](https://github.com/assetcorp/sirannon-db) holds the patches, the build script, and the tests.

# @delali/sirannon-sqlite

[![npm](https://img.shields.io/npm/v/@delali/sirannon-sqlite)](https://www.npmjs.com/package/@delali/sirannon-sqlite)
[![license](https://img.shields.io/npm/l/@delali/sirannon-sqlite)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

This package contains better-sqlite3 13.0.2, built against Sirannon's own build of SQLite, which counts the rows that each statement reads and writes. When you pass `rowCounts: true`, Sirannon's better-sqlite3 driver loads this package and reports both counts for every statement through `onQueryComplete`, so that a host that puts many tenants on one machine can bill each of them.

## Install

Install it with the library:

```bash
pnpm add -E @delali/sirannon-db @delali/sirannon-sqlite
```

```ts
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

const sirannon = new Sirannon({
  driver: betterSqlite3({ rowCounts: true }),
  metrics: { onQueryComplete: m => console.log(m.databaseId, m.rowsRead, m.rowsWritten) },
})
```

This package lists six platform packages as optional dependencies, so npm installs only the one whose `os` and `cpu` match your host. The release workflow publishes all seven packages at the library's version, and this package requires exactly that version of each platform package.

## What it contains

The `lib` directory contains better-sqlite3's JavaScript, unchanged. Sirannon compiles the native module from better-sqlite3's C++ source and the SQLite amalgamation, after it applies each patch in the repository's `native/sqlite/patches` directory. The first SQLite patch is Cloudflare's row-counting patch, which Sirannon changed to widen both counters to 64 bits and to leave the work of its own change-capture triggers out of both counts, and the first better-sqlite3 patch adds the `takeRowCounts` method that reads them. The counting rules are in the [core engine guide](https://github.com/assetcorp/sirannon-db/blob/main/docs/core.md#rows-read-and-rows-written).

On a platform without a published build, the driver throws `ROW_COUNTS_UNSUPPORTED` when it opens a database. Compile a build with `native/sqlite/build.mjs` from the repository, and pass its path to the driver as `sqliteBindingPath`.

## Licence

The Apache-2.0 and MIT licences cover this package. Each project whose code is in this package has its name and licence in the package's `NOTICE` file, and the patches, the build script, and the tests are in [assetcorp/sirannon-db](https://github.com/assetcorp/sirannon-db).

# @delali/sirannon-sqlite

[![npm](https://img.shields.io/npm/v/@delali/sirannon-sqlite)](https://www.npmjs.com/package/@delali/sirannon-sqlite)
[![license](https://img.shields.io/npm/l/@delali/sirannon-sqlite)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

This package holds better-sqlite3 13.0.2, built against a SQLite that counts the rows each statement reads and writes. Sirannon's better-sqlite3 driver loads it when you pass `rowCounts: true`, and reports both counts for every statement through `onQueryComplete`, so that a host can bill each tenant on a shared machine.

## Install

Install it beside the library:

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

This package lists six platform packages as optional dependencies, so npm installs only the one whose `os` and `cpu` match your host. The release workflow publishes this package and the six at the library's version, and this package pins them at that version exactly.

## What it holds

The `lib` directory is better-sqlite3's JavaScript, copied unchanged. The native module comes from better-sqlite3's C++ source with one method added, `takeRowCounts`, and from SQLite with Cloudflare's row-counting patch applied. Sirannon widens that patch's counters to 64 bits and leaves the work of its own change-capture triggers out of both counts. The [core engine guide](https://github.com/assetcorp/sirannon-db/blob/main/docs/core.md#rows-read-and-rows-written) lists the counting rules.

Where your platform has no published build, the driver fails to open a database with `ROW_COUNTS_UNSUPPORTED`. Compile a build with `native/sqlite/build.mjs` from the repository, and pass its path to the driver as `rowCountsBindingPath`.

## Licence

This package is published under the Apache-2.0 and MIT licences. Its `NOTICE` names the projects whose code it carries, and [assetcorp/sirannon-db](https://github.com/assetcorp/sirannon-db) holds the patches, the build script, and the tests.

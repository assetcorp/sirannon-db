# @delali/sirannon-sqlite

[![npm](https://img.shields.io/npm/v/@delali/sirannon-sqlite)](https://www.npmjs.com/package/@delali/sirannon-sqlite)
[![license](https://img.shields.io/npm/l/@delali/sirannon-sqlite)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

This package contains better-sqlite3 13.0.2, built against Sirannon's build of SQLite, which is SQLite3 Multiple Ciphers with a patch that counts the rows that each statement reads and writes. When you pass `rowCounts: true`, Sirannon's better-sqlite3 driver loads this package and reports both counts for every statement through `onQueryComplete`, so that a host that puts many tenants on one machine can bill each of them. When you pass `encryption: true`, the driver loads this package and keeps every page of a database that you open with the `encryption` option as ciphertext, as the [encryption guide](https://github.com/assetcorp/sirannon-db/blob/main/docs/encryption.md) describes.

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

const encrypting = new Sirannon({ driver: betterSqlite3({ encryption: true }) })
const ledger = await encrypting.open('ledger', './ledger.db', { encryption: { key: loadLedgerKey } })
```

`loadLedgerKey` stands for a function of yours that returns the 32 key bytes from a key service or a keychain.

This package lists six platform packages as optional dependencies, so npm installs only the one whose `os` and `cpu` match your host. The release workflow publishes all seven packages at the library's version, and this package requires exactly that version of each platform package.

## What it contains

The `lib` directory contains better-sqlite3's JavaScript, unchanged. Sirannon compiles the native module from better-sqlite3's C++ source and the SQLite3 Multiple Ciphers 2.6.1 amalgamation, which carries SQLite 3.54.0, after it applies each patch in the repository's `native/sqlite/patches` directory. The first SQLite patch is Cloudflare's row-counting patch, which Sirannon changed to widen both counters to 64 bits and to leave the work of its own change-capture triggers out of both counts, and the first better-sqlite3 patch adds the `takeRowCounts` method that reads them. The counting rules are in the [core engine guide](https://github.com/assetcorp/sirannon-db/blob/main/docs/core.md#rows-read-and-rows-written).

The second SQLite patch adds the `sirannon_export` function, which copies the main database into an attached database under that database's key and keeps the rowid of every row, as `VACUUM INTO` does. The second better-sqlite3 patch adds the `key` method that hands SQLite3 Multiple Ciphers a raw 32-byte key, keys the destination of a backup with the source's key, and adds the calls that record a connection's changes through SQLite's session extension, so that a key change can apply the writes that commit while it copies the file. SQLite3 Multiple Ciphers encrypts each page with AEGIS-256, from code that is compiled into the native module.

On a platform without a published build, the driver throws `ROW_COUNTS_UNSUPPORTED` or `ENCRYPTION_UNSUPPORTED` when it opens a database. Compile a build with `native/sqlite/build.mjs` from the repository, and pass its path to the driver as `sqliteBindingPath`.

## Licence

The Apache-2.0 and MIT licences cover this package, and the BSD-3-Clause licence also covers each platform package, since the SQLite3 Multiple Ciphers amalgamation in those carries a SHA-2 implementation under that licence. Each project whose code is in this package has its name and licence in the package's `NOTICE` file, and the patches, the build script, and the tests are in [assetcorp/sirannon-db](https://github.com/assetcorp/sirannon-db).

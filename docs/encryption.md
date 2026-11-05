# Encryption

Sirannon can keep a database file, its write-ahead log, and every backup that it writes as ciphertext under a 32-byte key that you supply. Someone who copies the file, a backup piece, or a disk image without the key can read none of the rows, and a page that somebody changes on disk fails its authentication check, so Sirannon reports an error where it would otherwise return wrong rows.

Encryption protects the data at rest. A process that holds the key can read every row, so keep the key in a key service or a keychain, and hand it to Sirannon through a function each time that Sirannon opens the database.

## Which driver encrypts

| Driver | Scheme | Engine |
| --- | --- | --- |
| better-sqlite3, created with `encryption: true` | AEGIS-256 | SQLite3 Multiple Ciphers 2.6.1 on SQLite 3.54.0, in `@delali/sirannon-sqlite` |
| Expo | SQLCipher 4 | The SQLCipher that expo-sqlite 15.0.0 or later carries when you turn on its `useSQLCipher` option |

The `node:sqlite`, Bun, and wa-sqlite drivers fail an open that carries a key with `ENCRYPTION_UNSUPPORTED`, because each of them opens SQLite through a library that Sirannon cannot replace with an encrypting build.

The two schemes lay out each page differently, so a file that one driver encrypts fails to open under the other driver, even with the right key. That open fails with `ENCRYPTION_KEY_REJECTED`, and the message says that the file can come from the other scheme. When you need to move a file between the two, remove its encryption with `changeEncryption({ key: null })`, copy the plaintext file, and encrypt it again through the other driver.

## Open an encrypted database on Node

Install the build of SQLite that encrypts beside the library:

```bash
pnpm add -E @delali/sirannon-db @delali/sirannon-sqlite
```

Create the better-sqlite3 driver with `encryption: true`, and give `sirannon.open` a function that returns the key:

```ts
import { readFile } from 'node:fs/promises'
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

const loadLedgerKey = async () => new Uint8Array(await readFile('/run/secrets/ledger-key'))

const sirannon = new Sirannon({ driver: betterSqlite3({ encryption: true }) })
const ledger = await sirannon.open('ledger', './data/ledger.db', { encryption: { key: loadLedgerKey } })
```

With `encryption: true`, the driver opens every database through `@delali/sirannon-sqlite`, which carries prebuilt native modules for Linux, macOS, and Windows on x86-64 and on 64-bit Arm. A database that you open without the `encryption` option stays plaintext under the same driver. When the file does not exist yet, Sirannon creates it encrypted, so no page of it reaches the disk as plaintext.

## The key

The function returns 32 raw bytes as a `Uint8Array`. A key of any other length, or a string, fails the open with `INVALID_ENCRYPTION_KEY`, and a function that throws or rejects fails it with `ENCRYPTION_KEY_UNAVAILABLE`, which carries your error as its cause.

Sirannon uses those bytes as the key with no key derivation, so generate them at random, for example with `crypto.getRandomValues(new Uint8Array(32))`, or take a data key from your key service. When you start from a password, derive the 32 bytes yourself with a password hashing function such as Argon2id before you return them.

Sirannon calls the function each time it opens the database's connections, which happens at the open, when a writer worker restarts, and when the database reopens after a restore or a key change. Sirannon copies the bytes and wipes its copy once those connections are open, so your function can return a buffer that it keeps. Sirannon sends the bytes to a writer worker once, through the worker's message port.

Return `{ key, id }` to store an identifier with the key:

```ts
const ledger = await sirannon.open('ledger', './data/ledger.db', {
  encryption: { key: async () => ({ key: await loadLedgerKey(), id: 'ledger-2026-11' }) },
})
```

Sirannon stores that identifier inside the database, reports it as `keyId` from `encryptionStatus()`, and writes it into every backup record, so you can tell which key each backup needs. No status, error, log line, or metric that Sirannon produces contains the key.

## Errors at open

| Code | When |
| --- | --- |
| `ENCRYPTION_KEY_MISSING` | You open an encrypted file without the `encryption` option |
| `ENCRYPTION_KEY_REJECTED` | The key fails the authentication check of the first page, because the key is wrong or because the other driver's scheme encrypted the file |
| `DATABASE_NOT_ENCRYPTED` | You give a key for a plaintext file; encrypt it through `changeEncryption` |
| `ENCRYPTED_PAGE_CORRUPT` | A statement reads a later page that fails its authentication check |
| `ENCRYPTION_UNSUPPORTED` | The driver cannot encrypt, the better-sqlite3 driver was created without `encryption: true`, the database is in memory, or the Expo app carries no SQLCipher |

## Encrypt, change the key of, or decrypt a database that is open

`changeEncryption` takes the target key function, or `null` to store the database as plaintext:

```ts
await ledger.changeEncryption({ key: loadNewLedgerKey })
await ledger.changeEncryption({ key: null })
```

The same call encrypts a plaintext database and gives an encrypted one a new key. Sirannon copies a snapshot of the database to a new file under the target key, applies the writes that commit after that snapshot in rounds, and holds writes only for the last round, after which it replaces the file. Reads and writes go on until that last round, so the database stays available during the change, although the change needs enough free disk space for a second copy of the database. The new file keeps every row, rowid, and trigger of the original.

After the replacement, every connection of this open uses the new key. When your process starts again, open the database with the new key function, or without the `encryption` option once you have removed encryption.

The promise resolves with the status once the change ends. While it works, `encryptionStatus()` reports its progress:

```ts
const status = await ledger.encryptionStatus()

status.encrypted        // whether the file holds ciphertext
status.cipher           // 'aegis-256' on Node, 'sqlcipher-4' on Expo
status.cipherVersion    // the version that the engine reports
status.keyId            // the identifier that came with the current key
status.changedAt        // when the last change finished
status.change.state     // 'idle', 'copying', 'catching-up', 'swapping', 'done', or 'failed'
status.change.rowsCopied
status.change.rounds
```

The change fails with one of these codes and leaves the database under its current key:

| Code | When |
| --- | --- |
| `ENCRYPTION_CHANGE_IN_PROGRESS` | Another change on the same database has not finished |
| `ENCRYPTION_CHANGE_ABORTED` | The schema changes during the copy, or a write arrives that the change cannot apply exactly; Sirannon deletes the new file |
| `READ_ONLY` | You opened the database read-only |
| `REPLICATION_ACTIVE` | A replication engine writes to the database; the [replication guide](replication.md#replicate-an-encrypted-database) shows how to change the key of a node |

### A process that stops part way

Once the new file exists, Sirannon writes a small marker file beside the database, which records the new file's first 16 bytes and whether the replacement has begun. When the process stops at any point during the change, the next open reads that marker and either finishes the replacement or deletes the new file, and every committed row is still there afterwards.

Until the replacement, the old key opens the database, and after it, the new key does. Keep both keys available until `changeEncryption` resolves, because a process that stops close to the replacement can leave either one in force. When the key that you give fails after such a stop, the open fails with `ENCRYPTION_KEY_REJECTED`, and the message says whether the file needs the key from before the change or the key from after it.

## Backups and restores

Every full copy, change piece, and streamed copy of an encrypted database holds ciphertext under the database's key, so the storage that receives your backups receives no plaintext. Each record in the chain stores the `keyId` of the key that its pieces need, when the key came with one.

A key change makes the next capture of the backup cycle start a new chain with a full copy, because the earlier chain holds ciphertext under the earlier key.

Pass the key that a chain holds ciphertext under to `restoreBackup`, where `s3Destination` is a destination like the one in the [backups guide](backups.md#supplying-a-destination):

```ts
import { restoreBackup } from '@delali/sirannon-db/backup'

await restoreBackup({
  destination: s3Destination,
  driver: betterSqlite3({ encryption: true }),
  destPath: './restored/ledger.db',
  encryption: { key: loadLedgerKey },
})
```

Sirannon rebuilds the file under that key, so the restored database holds ciphertext under the same key as its backups. When the key fails on the full copy, the restore fails with `ENCRYPTION_KEY_REJECTED`, and the message states the `keyId` that the chain records. A restore through the server's restore route uses the database's current key, which is the key from its last key change when the change happened in the same process.

## Over the server

The server serves two routes to an operator, and it calls your `authenticate` hook before each of them:

```text
POST /db/{id}/encryption  { keyRef: string or null }  -> 202 { started: true }
GET  /db/{id}/encryption                              -> the same status as encryptionStatus()
```

The server keeps the `POST` route closed until you set `acceptEncryptionChange: true`, and until then it answers `403 ENCRYPTION_CHANGE_NOT_ACCEPTED`. A caller sends a `keyRef`, which is a name for a key, and your `encryptionKeys` function turns that name into the key, so no key crosses the network:

```ts
const server = createServer(sirannon, {
  authenticate: identifyOperator,
  acceptEncryptionChange: true,
  encryptionKeys: async (databaseId, keyRef) => ({ key: await keyService.dataKey(databaseId, keyRef), id: keyRef }),
})
```

`createServer` throws `INVALID_ENCRYPTION_CHANGE` when you set `acceptEncryptionChange: true` without an `authenticate` hook or without `encryptionKeys`. The server calls `encryptionKeys` once to check the key before it answers, and it answers `202` once the change has started, because copying a large database can take longer than the timeout of a proxy between the caller and the server. Read the outcome from the `GET` route:

```bash
curl -XPOST -H "$AUTH" -d '{"keyRef":"ledger-2026-12"}' https://db.example.com/db/ledger/encryption
curl -H "$AUTH" https://db.example.com/db/ledger/encryption
```

A `keyRef` of `null` removes encryption. The server sends a fixed message for every encryption error code, so that no reply contains a path on the server, and it keeps the full error for you in the error that Sirannon raises on the server.

## Replication

An encrypted node sends no batch and no snapshot to a peer that holds a plaintext database, and the peer receives `ENCRYPTION_REQUIRED`. The [replication guide](replication.md#replicate-an-encrypted-database) shows how to open the engine's connections under the key and how to change the key of each node in turn.

## Expo

Turn on SQLCipher in expo-sqlite's config plugin in `app.json`, and rebuild the app:

```json
{
  "expo": {
    "plugins": [["expo-sqlite", { "useSQLCipher": true }]]
  }
}
```

```ts
import { Sirannon } from '@delali/sirannon-db'
import { expoSqlite } from '@delali/sirannon-db/driver/expo'

const sirannon = new Sirannon({ driver: expoSqlite() })
const notes = await sirannon.open('notes', 'notes.db', { encryption: { key: loadDeviceKey } })
```

`loadDeviceKey` stands for a function of yours that returns the 32 key bytes from the device's keychain.

Encryption on Expo needs expo-sqlite 15.0.0 or later with `useSQLCipher` turned on, which carries SQLCipher 4.6.0 or later. Sirannon accepts any expo-sqlite from 14.0.0 for a plaintext database, so it checks the engine when you open with a key, and an app with an older expo-sqlite, or without `useSQLCipher`, fails that open with `ENCRYPTION_UNSUPPORTED`, whose message states the version that it needs.

`changeEncryption` on Expo also needs expo-file-system 19.0.0 or later, because the change reads, deletes, and moves the database files through `expo-file-system/legacy`. Without it, `changeEncryption` fails with `ENCRYPTION_UNSUPPORTED` before it writes any file. Once your app has called `changeEncryption`, keep expo-file-system 19.0.0 or later installed in every later build of the app, because the open that recovers from an interrupted change reads the files beside the database through it. Without expo-file-system, that open cannot see that a change stopped part way.

The Expo driver uses SQLCipher 4's default settings with the raw key, so SQLCipher 4 opens the file with the same 32 bytes as `PRAGMA key = "x'<64 hex digits>'"`.

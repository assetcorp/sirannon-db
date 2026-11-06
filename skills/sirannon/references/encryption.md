# Encrypt a Sirannon database

Read the installed package's declarations before you add encryption, because the project may use a release whose API differs from the repository documentation.

## Choose a supported driver

Sirannon encrypts databases through the better-sqlite3 driver on Node.js and the Expo driver on React Native. The Node built-in, Bun, and wa-sqlite drivers refuse a database that has an encryption key.

On Node.js, install the encrypting SQLite build at an exact version:

```bash
pnpm add -E @delali/sirannon-db @delali/sirannon-sqlite
```

Enable encryption on the driver:

```ts
import { readFile } from 'node:fs/promises'
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

const loadKey = async () => new Uint8Array(await readFile('/run/secrets/app-database-key'))

const sirannon = new Sirannon({ driver: betterSqlite3({ encryption: true }) })
const db = await sirannon.open('app', './data/app.db', {
  encryption: { key: async () => ({ key: await loadKey(), id: 'app-key-1' }) },
})
```

The key function returns 32 raw bytes. Store those bytes in a key service, a system keychain, or a secret file whose permissions restrict access to the application's operating-system account. Return an identifier with the key so that encryption status and backup records show which key they need.

## Change a key

Call `changeEncryption` with a new key function to encrypt a plaintext database or replace its current key. Pass `null` to store the database as plaintext:

```ts
await db.changeEncryption({ key: loadNewKey })
await db.changeEncryption({ key: null })
```

Keep the current and target keys available until the key change finishes. If the process stops near the file replacement, the next open may need either key to finish recovery without losing a committed write.

## Restore an encrypted backup

Enable encryption on the restore driver and return the key recorded by the selected chain:

```ts
import { restoreBackup } from '@delali/sirannon-db/backup'

await restoreBackup({
  destination,
  driver: betterSqlite3({ encryption: true }),
  destPath: './data/restored.db',
  encryption: { key: loadKey },
})
```

Retain the key for every backup chain that the application keeps. A key change starts a new chain with a full copy under the target key.

## Expose a key change through the server

Keep the change route closed unless the user asks for remote key management. When the route is required, set `acceptEncryptionChange`, authenticate every request, and resolve a `keyRef` inside the server through `encryptionKeys`. Send only the `keyRef` over the route.

## Configure Expo

The installed `expo-sqlite` release must include SQLCipher. The app config must also turn on its `useSQLCipher` plugin option. `changeEncryption` needs an installed `expo-file-system` release that matches the minimum version in the installed Sirannon documentation. Rebuild the native app after changing the plugin option.

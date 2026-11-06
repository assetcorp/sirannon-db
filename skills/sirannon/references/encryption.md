# Encrypt a Sirannon database

Read the installed package's declarations before you add encryption, because the project may use a release whose API differs from the repository documentation.

## Choose a supported driver

Sirannon encrypts databases through the better-sqlite3 driver on Node.js and the Expo driver on React Native. The Node built-in, Bun, and wa-sqlite drivers refuse a database that has an encryption key.

On Node.js, install both packages at exact versions when Sirannon is absent:

```bash
pnpm add -E @delali/sirannon-db @delali/sirannon-sqlite
```

When Sirannon is installed, read the `@delali/sirannon-sqlite` version from its `peerDependencies`. Install that exact peer version without adding Sirannon again. If the installed declarations have no encryption option, ask the user before upgrading Sirannon.

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

Call `changeEncryption` with a new key function to encrypt a plaintext database or replace its current key:

```ts
await db.changeEncryption({ key: loadNewKey })
```

Pass `null` when the user wants to store the database as plaintext:

```ts
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

Keep the change route closed unless the user asks for remote key management. When the route is required, set `acceptEncryptionChange` and use `authenticate` to authorise the operator for the requested database. Resolve a `keyRef` inside the server through `encryptionKeys`. Send only the `keyRef` over the route.

## Configure Expo

The installed `expo-sqlite` release must include SQLCipher. The app config must also turn on its `useSQLCipher` plugin option. `changeEncryption` needs an installed `expo-file-system` release that matches the minimum version in the installed Sirannon documentation. Keep `expo-file-system` installed in every later build so that the next open can recover an interrupted key change. Rebuild the native app after changing the plugin option.

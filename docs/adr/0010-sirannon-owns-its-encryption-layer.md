# Sirannon owns its encryption layer

PCI DSS 4.0 requirement 3.5.1.2 accepts disk-level encryption only on removable media, so a Sirannon node on an encrypted volume fails that audit. SQLite includes no encryption, and it removed the `SQLITE_HAS_CODEC` hook in version 3.32.0, so every encryption scheme since then is either a replacement SQLite build or a VFS shim. Sirannon already publishes a VFS shim as a loadable extension for streamed backups, so the encryption layer is a second shim in the same extension, and Sirannon owns it.

The layer encrypts every page with AES-256-GCM, with a fresh nonce and an authentication tag stored in each page's reserved bytes. The cipher comes from the platform's own audited library, which is OpenSSL through Node, BoringSSL through Bun, and WebCrypto in the browser. Sirannon writes no cipher code of its own, and it offers no cipher choice.

## Why this over the alternatives

- **SQLCipher** disables SQLite's online backup interface for an encrypted database, and the interface fails at initialisation. Every copy Sirannon makes, the stepped copy of ADR 0005, the checkpoint cycle of ADR 0006, and the streamed copy through the extension, uses that interface. Its community licence also requires attribution in a screen of the application that embeds it, which would place an obligation on every Sirannon user.
- **SQLite3MultipleCiphers** is the mature open-source layer, and its installation guide states that it replaces the SQLite amalgamation. It cannot load as an extension, so it reaches Node's own SQLite module on no path at all, and it reaches Bun only through `setCustomSQLite` with a library the operator compiles. Its authors also warn against two SQLite builds in one process, which Sirannon already forbids for the streaming extension.
- **The SQLite Encryption Extension** is closed source under a paid licence, so it cannot be a dependency of a published npm package.
- **Volume encryption** decrypts for every process on the running machine, which is the reason the PCI Security Standards Council removed it from requirement 3.5.1.2.

## Decision

- Sirannon builds the encryption shim into the same extension as the streaming shim. The streaming shim captures the default VFS at registration and registers itself as non-default, so two separate extensions would stack in whichever order they loaded. One extension fixes the order.
- Node's own SQLite, better-sqlite3, and Bun load the extension. Expo reports no extension loading, so the Expo driver uses expo-sqlite's own SQLCipher build. The browser driver uses a JavaScript VFS that calls WebCrypto, which accepts a non-extractable key, so a browser key can be one that script cannot read out.
- Every runtime exposes the same option, and a runtime without a mechanism refuses to open an encrypted database with a named error, which is how a missing `loadExtension` or `copyDatabase` already fails.
- Sirannon makes four refusals with no override. It forces temporary storage to memory, because SQLite's VFS interface gives a shim no way to tie a temporary file or a sub-journal back to its database, and the SQLite3MultipleCiphers authors document that those files cannot be encrypted. It refuses a backup whose destination would receive plaintext, because Node's backup call opens the destination itself and would otherwise write a readable copy. It refuses to open an encrypted database on a runtime without the capability. It refuses a key shorter than 32 bytes.
- A backup holds ciphertext under the same data key. A full copy and a change piece are byte copies, so the destination, the server's backup routes, and a restore never hold plaintext, and a restore needs the master key.
- A group with the encryption-required setting admits only encrypted members. MongoDB Atlas applies a customer key to every node and every snapshot in a cluster, and SQL Server documents that its replication leaves a subscriber unencrypted unless an operator encrypts it separately.
- Encrypting an existing database, removing encryption, and changing the data key go through one re-encryption job. The job pauses and resumes. Writes continue while it copies, and it holds them only for the final swap, which is the window `Sirannon.withDatabaseOffline` already uses for an in-place restore. SQL Server keeps a database online during its encryption scan and encrypts new writes from the moment encryption turns on, and since SQL Server 2019 the scan suspends and resumes.
- An operator starts encryption, a rotation, or a re-encryption job over the server's authenticated routes behind the flag that already guards restore, and the routes answer 202 with progress. Status reports whether a database is encrypted, which master key it uses by name, and when it was last rotated, and it never reports a key.

## Cost

Sirannon owns cryptographic code, so a security review of the shim is part of every change to it. Reserved bytes take 128 bytes of every page, which ADR 0011 accounts for. Temporary storage in memory means a large sort or a large temporary table uses memory in place of disk. The extension gains a second build in every platform matrix. Only a holder of the master key can open the database or any backup made from it, and MySQL's reference manual states the same of its master key.

## Sources

- PCI DSS 4.0 requirement 3.5.1.2, through the GuidePoint summary of future-dated requirements: <https://www.guidepointsecurity.com/blog/pci-dss-4-0-major-future-dated-requirements/>
- SQLite on the removal of `SQLITE_HAS_CODEC`: <https://sqlite.org/forum/forumpost/7d7f6633f1>
- SQLite's checksum VFS shim, the worked example of a loadable shim claiming reserved bytes: <https://sqlite.org/cksumvfs.html>
- SQLCipher and the backup interface: <https://groups.google.com/g/sqlcipher/c/r4BtWCxmLNU>
- SQLCipher community licence: <https://www.zetetic.net/sqlcipher/license/>
- SQLite3MultipleCiphers installation: <https://utelle.github.io/SQLite3MultipleCiphers/docs/installation/install_overview/>
- SQLite3MultipleCiphers on temporary files and multiple builds: <https://utelle.github.io/SQLite3MultipleCiphers/docs/faq/faq_overview/>
- SQLite Encryption Extension licence: <https://sqlite.org/com/see.html>
- Bun's `setCustomSQLite`: <https://bun.com/docs/runtime/sqlite>
- Expo SQLite and SQLCipher: <https://docs.expo.dev/versions/latest/sdk/sqlite/>
- MongoDB Atlas customer key management: <https://www.mongodb.com/docs/atlas/security-kms-encryption/>
- SQL Server transparent data encryption, including the scan, suspend and resume, and replication: <https://learn.microsoft.com/en-us/sql/relational-databases/security/encryption/transparent-data-encryption>
- MySQL InnoDB data-at-rest encryption: <https://dev.mysql.com/doc/refman/8.0/en/innodb-data-encryption.html>

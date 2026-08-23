# Keys are random, provided, and never in options or filenames

A developer turns encryption on with one option and Sirannon does the rest, so that option has to be safe on its own. The master key is 32 random bytes or a function that fetches them. It arrives through a key provider, and it never enters the options object or a filename.

## Decision

- Sirannon refuses a password. MongoDB's key file is the output of `openssl rand -base64 32`. MySQL's keyring generates a random 32-byte master key. pg_tde keeps its principal key in Vault or a KMIP server. Turso Database requires a random hex key and lists passphrases as future work. Neon, PlanetScale, and Supabase take a reference to a key the customer holds in a cloud key service and never take a password. A password is for unlocking a key store, and none of these engines lets one encrypt data. On a phone, the app generates a key once with a helper Sirannon provides and keeps it in the keychain, which is what Realm and PowerSync document.
- The master key wraps one data key per database. Master-key rotation re-wraps the key record and rewrites no page. MySQL's manual states this of `ROTATE INNODB MASTER KEY`, and MongoDB's states it of a KMIP master key. Changing the data key rewrites every page through the re-encryption job of ADR 0010, which is what MySQL's disable-and-re-enable and SQL Server's `ALTER DATABASE ENCRYPTION KEY` do.
- The developer supplies a key provider, and Sirannon provides one for an environment variable, one for a file, and one that wraps a caller's function for a key service or a keychain. A literal key in `DatabaseOptions` would be wrong, because Sirannon copies those options into the writer worker and keeps them so that a restore can reopen the database, so a key in them would go everywhere they go. A provider that does not answer at open stops the database from opening, which is what MongoDB Atlas does to a cluster that restarts without its key service.
- Sirannon hands the key to the VFS through an extension function called on a scratch connection, which registers the key against the file's path before the real connection opens. SQLite documents the same installation for its checksum shim. A URI parameter fails because Bun parses no `file:` URI and better-sqlite3 parses one only behind `SQLITE_USE_URI=1`, and a filename also appears in error messages and logs.
- The main thread calls the provider, sends the bytes once over the writer worker's message port, and zeroes its own copy. The worker holds the key for the life of its connection.

## Why this over the alternatives

- **A passphrase stretched by a key-derivation function** would be easy to use and easy to guess, and no production engine accepts one for data encryption.
- **One key for pages and wrapping** would make every rotation a full rewrite.
- **A key in `DatabaseOptions`** would go everywhere those options go.
- **Sirannon generating and storing a key beside the database** would protect nothing.

## Cost

A developer has to generate a key and keep it somewhere, and Sirannon's helper and its three providers are the whole of the help it gives. Only a holder of the master key can open the database or any backup made from it.

## Sources

- MongoDB encryption key file: <https://www.mongodb.com/docs/manual/tutorial/configure-encryption/>
- MongoDB key rotation: <https://www.mongodb.com/docs/manual/tutorial/rotate-encryption-key/>
- MongoDB Atlas and an unavailable key service: <https://www.mongodb.com/docs/atlas/security-kms-encryption/>
- MySQL keyring and master-key rotation: <https://dev.mysql.com/doc/refman/8.0/en/innodb-data-encryption.html>
- pg_tde key providers: <https://docs.percona.com/pg-tde/global-key-provider-configuration/index.html>
- SQL Server encryption hierarchy: <https://learn.microsoft.com/en-us/sql/relational-databases/security/encryption/encryption-hierarchy>
- Turso Database encryption: <https://docs.turso.tech/tursodb/encryption>
- Neon security overview: <https://neon.com/docs/security/security-overview>
- PlanetScale security: <https://planetscale.com/docs/security>
- Supabase security: <https://supabase.com/security>
- PowerSync data encryption: <https://docs.powersync.com/client-sdks/advanced/data-encryption>
- SQLite's checksum VFS shim, installed on a scratch connection: <https://sqlite.org/cksumvfs.html>
- Bun and `file:` URIs: <https://github.com/oven-sh/bun/issues/4202>

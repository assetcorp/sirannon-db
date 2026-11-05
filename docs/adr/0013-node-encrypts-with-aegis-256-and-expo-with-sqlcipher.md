# The Node driver encrypts with AEGIS-256, and the Expo driver encrypts with SQLCipher 4

Sirannon keeps an encrypted database, its log, and its backups as ciphertext under a 32-byte key that the caller supplies. The better-sqlite3 driver encrypts through SQLite3 Multiple Ciphers with AEGIS-256, compiled into the `@delali/sirannon-sqlite` build that [ADR 0010](0010-sirannon-builds-its-own-sqlite-to-count-rows-read.md) introduced. The Expo driver encrypts through the SQLCipher that expo-sqlite carries, with SQLCipher 4's default settings. Both drivers take the 32 bytes as a raw key, with no key derivation, because the caller's key function returns 32 bytes that the caller generates at random or takes from a key service.

## Why AEGIS-256 on Node

- **One pass over each page.** AEGIS-256 is an authenticated cipher built on the AES round function, so it encrypts a page and computes its 32-byte tag in one pass and uses the processor's AES instructions where they exist. SQLCipher 4 encrypts each page with AES-256 in CBC mode and then computes an HMAC-SHA512 over it, which is a second pass over the same bytes. On an x86-64 machine with AES instructions and with `synchronous = NORMAL`, the median insert took 14.1 µs under AEGIS-256 and 35.8 µs under SQLCipher 4, against 11.0 µs on a plaintext database, and the median transaction of 1,000 inserts took 2,123 µs under AEGIS-256 and 5,653 µs under SQLCipher 4, against 2,328 µs on plaintext. Each figure is the median of five runs, which alternated their order on one pinned core.
- **No cryptographic library outside the module.** SQLite3 Multiple Ciphers compiles its cipher code into the native module. A SQLCipher build for Node has to call a library for AES and HMAC: on Linux and Windows, that library is the OpenSSL inside the running Node process, which ties the module to the OpenSSL symbols that each Node release exports, and on macOS it is CommonCrypto.
- **The same build serves plaintext databases.** SQLite3 Multiple Ciphers passes the pages of an unencrypted database through unchanged, so a database opened without a key behaves as it does under better-sqlite3's own SQLite.

## Why SQLCipher on Expo

Sirannon publishes no native code for React Native, so the Expo driver opens databases through the SQLite that the app's expo-sqlite carries. expo-sqlite carries SQLCipher when the app turns on the `useSQLCipher` option of its config plugin. Building an encrypting SQLite for Expo would mean that Sirannon publishes and maintains a native module for iOS and Android beside expo-sqlite, so Sirannon uses the SQLCipher that is already there.

expo-sqlite first carries SQLCipher in 15.0.0, with SQLCipher 4.6.0, and `expo-file-system/legacy`, which the key change uses to read, delete, and move the database files, first exists in expo-file-system 19.0.0. Sirannon accepts expo-sqlite from 14.0.0 for plaintext databases, so the Expo driver checks the engine at an encrypted open and fails with `ENCRYPTION_UNSUPPORTED` below those versions.

## Consequences

- A file that one driver encrypts does not open under the other driver, because the two schemes lay out each page differently. The open fails with `ENCRYPTION_KEY_REJECTED`, and the message says that the file can come from the other scheme. `encryptionStatus()` reports `aegis-256` or `sqlcipher-4`, so an operator can tell which scheme a database uses.
- SQLCipher's `sqlcipher_export` renumbers the rows of a rowid table that has no index, because SQLite's transfer optimisation assigns new rowids there. On Expo, a key change therefore copies every rowid table that is not an alias of an `INTEGER PRIMARY KEY` again with its rowids after the export. On Node, Sirannon's patch adds `sirannon_export`, which runs the copy steps of `VACUUM INTO` and keeps every rowid.
- Sirannon maintains a second patch on the SQLite3 Multiple Ciphers amalgamation and reapplies it for each release that it adopts. The build disables every cipher except AEGIS.
- Tests read every encrypted file with a page decryptor that Sirannon's tests build on libsodium's AEGIS-256, which shares no code with SQLite3 Multiple Ciphers, and with a separate build of SQLite3 Multiple Ciphers as a second reader. The Expo tests use the SQLCipher amalgamation from the expo-sqlite tarballs, pinned by hash and compiled with Expo's Android flags, for 15.0.0, the oldest release that carries SQLCipher, and for 58.0.15.

## Considered options

- **SQLCipher on both drivers.** One scheme would let a file move between the two drivers. However, it would keep the second pass and the dependency on Node's OpenSSL on every Node host.
- **ChaCha20-Poly1305 from SQLite3 Multiple Ciphers.** It needs no AES instructions. However, on an x86-64 machine with AES instructions and with `synchronous = NORMAL`, it took a median of 18.7 µs per insert and 12.1 µs per update in an earlier comparison on the same machine, in which AEGIS-256 took 12.5 µs and 10.2 µs.
- **A key derived from a passphrase.** Both engines can derive the key from a passphrase, through Argon2 in SQLite3 Multiple Ciphers and through PBKDF2 in SQLCipher. Sirannon takes 32 bytes from the caller's key function, and when those bytes are random, a derivation adds its cost to every open and makes the key no harder to guess. A caller who starts from a passphrase can derive the 32 bytes before the function returns them.

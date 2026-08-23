# The encrypted file layout

An encrypted database has to stay a file that SQLite's stepped backup copies byte for byte, that Sirannon's restore can size without a key, and that Sirannon's change-piece capture can walk without a key. Every encrypted file follows the layout below, which is why a later change cannot undo it, so this record fixes it.

## Decision

- The first 100 bytes of the file stay plaintext. That is SQLite's file header, which holds the magic string, the page size, and the journal mode, and no row data. Turso Database and DuckDB leave the same header in the clear, and SQLCipher offers it as an option for iOS. Sirannon's restore and piece transfer read the page size from those bytes, and the `sqlite3` shell identifies the file.
- Every page reserves 128 bytes at its end, which is the reserved-bytes field SQLite keeps for a VFS shim and caps at 255. Of those, 12 hold a nonce and 16 hold an authentication tag. Page 1 also holds the key record, which is a version byte, a 16-byte salt, the 32-byte wrapped data key, the 12-byte nonce and 16-byte tag of that wrap, and a 16-byte master key name, 93 bytes in all and 121 with the page's own nonce and tag.
- Sirannon stores the key record inside the file. A byte copy of the file is therefore restorable by anyone who holds the master key, which is what MySQL does with its tablespace key, SQL Server with its database encryption key, and DuckDB with its header. With a sidecar file, pages would reserve 28 bytes in place of 128, and a copy, a backup, or a restore would have one more file to lose.
- A write-ahead log frame holds a ciphertext page and a plaintext frame header. SQLite computes each frame's checksum over the page as it holds it in memory, which is plaintext, and a VFS shim cannot change that. The checksum chain on disk therefore covers bytes that are not on disk. SQLite3MultipleCiphers computes its checksums over the ciphertext only because it edits SQLite's source, which a shim cannot do. Sirannon's change-piece capture therefore decrypts each frame's page in memory to verify the chain, and it sends the frame unchanged. The parser in `core/backup/wal-format.ts` already holds the frame layout, so the change is inside code Sirannon owns.

## Why this over the alternatives

- **Encrypting the header too** would force restore and piece transfer to hold the master key before reading a page size, and it would make the file unidentifiable to every SQLite tool.
- **A length-preserving cipher over the whole file** needs no reserved bytes, and it encrypts the write-ahead log's frame headers with everything else, so Sirannon's capture would see noise where it reads page numbers and checksums.
- **Ciphertext checksums in the log** would let the capture verify frames without a key, and only a replacement SQLite build can produce them.

## Cost

Reserved bytes are 128 of every page, which is 3.1% of a 4 KiB page and 0.2% of a 64 KiB page. SQLite can raise a database's reserved bytes only by rewriting every page, so the re-encryption job of ADR 0010 is the path that sets them. The capture holds the data key, which ADR 0012 already requires of the process that writes.

## Sources

- SQLite file format, including the header and reserved bytes: <https://www.sqlite.org/fileformat2.html>
- SQLite's `SQLITE_FCNTL_RESERVE_BYTES` file control: <https://www.sqlite.org/c3ref/c_fcntl_begin_atomic_write.html>
- SQLite write-ahead log format and frame checksums: <https://www.sqlite.org/walformat.html>
- SQLite3MultipleCiphers on log checksums over encrypted content and the source change behind it: <https://utelle.github.io/SQLite3MultipleCiphers/docs/architecture/arch_vfs/>
- Turso Database encryption, with nonce and tag in reserved space and a plaintext header: <https://turso.tech/blog/introducing-fast-native-encryption-in-turso-database>
- DuckDB data-at-rest encryption, with a plaintext main header: <https://duckdb.org/2025/11/19/encryption-in-duckdb>
- SQLCipher `cipher_plaintext_header_size`: <https://www.zetetic.net/sqlcipher/sqlcipher-api/>
- MySQL tablespace keys in the tablespace header: <https://dev.mysql.com/doc/refman/8.0/en/innodb-data-encryption.html>
- SQL Server's database encryption key in the boot record: <https://learn.microsoft.com/en-us/sql/relational-databases/security/encryption/transparent-data-encryption>

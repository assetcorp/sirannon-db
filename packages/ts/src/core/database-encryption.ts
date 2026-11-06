import { DatabaseBackups } from './database-backups.js'
import type { OpenOptions, SQLiteConnection } from './driver/types.js'
import type { EncryptionChange, EncryptionStatus } from './encryption/types.js'
import type { FileConnectionHolder } from './file-connections.js'

/**
 * Gives an open database its encryption methods: the status of its key, and the key change that encrypts it, gives it
 * a new key, or removes encryption while it stays open.
 *
 * Open a database through {@link Sirannon.open}, which returns a {@link Database} that extends this class.
 *
 * @public
 */
export class DatabaseEncryption extends DatabaseBackups {
  /**
   * Returns whether the database file holds ciphertext, the version of the encryption engine, the identifier that
   * came with the current key, the time of the last finished key change, and the progress of the current or last
   * key change. The status never contains a key.
   *
   * @returns The encryption status.
   */
  async encryptionStatus(): Promise<EncryptionStatus> {
    this.ensureNotClosed()
    return this.runtime.encryption.status()
  }

  /**
   * Encrypts a plaintext database, gives an encrypted one a new key, or, with `key` set to `null`, stores it as
   * plaintext, while the database stays open for reads and writes.
   *
   * Sirannon copies a snapshot to a new file under the target key, applies the writes committed since the snapshot in
   * rounds, holds writes only for the last round, and then replaces the file. Until the replacement, the database
   * keeps its current key. After it, every connection opens under the new key, so open the database under that key
   * from then on. When the database captures backups, the next capture starts a new chain with a full copy under the
   * new key.
   *
   * On the Expo driver, the change needs expo-file-system 19.0.0 or later. Once an app has called this method, keep
   * expo-file-system 19.0.0 or later installed in every later build, because the open that recovers from an
   * interrupted change reads the files beside the database through it. Without it, the open cannot see that a change
   * stopped part way.
   *
   * @param change - The function that returns the target key, or `null` to remove encryption.
   * @returns The encryption status once the change ends.
   * @throws An `EncryptionError` with `ENCRYPTION_UNSUPPORTED` on an Expo app without expo-file-system 19.0.0 or
   * later, before the change writes any file, with `ENCRYPTION_CHANGE_IN_PROGRESS` while another change runs, or with
   * `ENCRYPTION_CHANGE_ABORTED` when a schema change or a write that the change cannot apply exactly stops it, which
   * leaves the database under its current key, `READ_ONLY` on a read-only database, and a `ReplicationError` with
   * `REPLICATION_ACTIVE` while a replication engine writes to the database, which the caller stops before the change
   * and starts again afterwards.
   */
  async changeEncryption(change: EncryptionChange): Promise<EncryptionStatus> {
    this.ensureOpen()
    this.ensureWriterCanWait()
    return this.runtime.encryption.changeKey(change)
  }

  /**
   * Opens a connection to this database's file beyond the pool, under the database's current key, which waits while
   * a key change replaces the file. The caller closes the connection.
   *
   * @param options - The open settings, without a key.
   * @returns The connection.
   * @internal
   */
  openFileConnection(options: OpenOptions): Promise<SQLiteConnection> {
    this.ensureNotClosed()
    return this.runtime.fileConnections.open(options)
  }

  /**
   * Registers a part of the server that keeps a connection from {@link DatabaseEncryption.openFileConnection} open
   * between calls, so that a key change can close it before it replaces the file and reopen it afterwards.
   *
   * @param holder - The release and reopen calls.
   * @returns A function that removes the registration.
   * @internal
   */
  holdFileConnection(holder: FileConnectionHolder): () => void {
    return this.runtime.fileConnections.hold(holder)
  }

  /**
   * Returns whether the database file opens under a key, which a replication engine announces to its peers. A key
   * change can alter it only while no engine runs, so the value holds for an engine's whole run.
   *
   * @returns `true` for an encrypted database.
   * @internal
   */
  get encryptedFile(): boolean {
    return this.runtime.encryption.encrypted
  }

  /**
   * Records a replication engine that writes to this database through a separate writer connection, so that a key
   * change or a restore refuses to replace the file under that connection while the engine runs.
   *
   * @returns A function that removes the record, which the engine calls when it stops.
   * @throws `ENCRYPTION_CHANGE_IN_PROGRESS` while a key change runs, and `DATABASE_CLOSED` on a closed database.
   * @internal
   */
  registerReplicationEngine(): () => void {
    this.ensureNotClosed()
    return this.runtime.replicationEngines.register()
  }

  /**
   * Throws while a replication engine writes to this database, because replacing the file would leave the engine's
   * writer connection on the replaced file.
   *
   * @param operation - The operation that would replace the file, as the error message states it.
   * @throws A `ReplicationError` with `REPLICATION_ACTIVE` while an engine runs.
   * @internal
   */
  assertNoReplicationEngine(operation: string): void {
    this.runtime.replicationEngines.assertNone(operation)
  }
}

/**
 * The key that an {@link EncryptionOptions.key} function returns: 32 raw bytes, or those bytes with an identifier that
 * Sirannon stores in the database and reports through {@link EncryptionStatus.keyId}.
 *
 * @public
 */
export type EncryptionKey = Uint8Array | { key: Uint8Array; id?: string }

/**
 * Settings that encrypt every page of a database file, its write-ahead log, and every backup that Sirannon writes for
 * it.
 *
 * @public
 */
export interface EncryptionOptions {
  /**
   * Returns the 32-byte key. Sirannon calls it each time it opens the database's connections, which includes a
   * restarted writer worker and the reopen after a restore or a key change, so the key can come from a key service or
   * a keychain. Sirannon copies the bytes and wipes its copy once those connections are open, so the function can
   * return a buffer that it keeps.
   */
  key: () => EncryptionKey | Promise<EncryptionKey>
}

/**
 * The key change that {@link DatabaseEncryption.changeEncryption} makes: a `key` function encrypts a plaintext database or
 * changes the key of an encrypted one, and `null` removes encryption.
 *
 * @public
 */
export interface EncryptionChange {
  /** Returns the 32-byte key that the database takes, or `null` to store the database as plaintext. */
  key: (() => EncryptionKey | Promise<EncryptionKey>) | null
}

/**
 * The progress of the current key change, or the outcome of the last one.
 *
 * @public
 */
export interface EncryptionChangeStatus {
  /**
   * `idle` before any change, `copying` while Sirannon copies a snapshot to the new file, `catching-up` while it
   * applies the writes committed since that snapshot, `swapping` while it holds writes and replaces the file, and
   * `done` or `failed` once the change ends.
   */
  state: 'idle' | 'copying' | 'catching-up' | 'swapping' | 'done' | 'failed'
  /** Whether the change encrypts the file or stores it as plaintext. */
  target?: 'encrypted' | 'plaintext'
  /** The time at which the change started, in milliseconds since the Unix epoch. */
  startedAt?: number
  /** The time at which the change ended, in milliseconds since the Unix epoch. */
  finishedAt?: number
  /** The number of rows that the snapshot copy wrote to the new file. */
  rowsCopied?: number
  /** The number of rounds in which Sirannon applied the writes committed since the snapshot. */
  rounds?: number
  /** The code and message of the error that stopped the change. */
  error?: {
    /** The machine-readable code of the error. */
    code: string
    /** What went wrong, for a log or a person. */
    message: string
  }
  /**
   * The code and message of an error that came after the new file was in place, such as a key function that failed
   * when the connections reopened. The change is `done` and the file holds ciphertext under the new key, so keep that
   * key, and close and open the database again with it.
   */
  reopenError?: {
    /** The machine-readable code of the error. */
    code: string
    /** What went wrong, for a log or a person. */
    message: string
  }
}

/**
 * Reports whether a database is encrypted, under which key, and how its last key change went.
 *
 * @public
 */
export interface EncryptionStatus {
  /** Whether the database file holds ciphertext. */
  encrypted: boolean
  /**
   * The scheme that an encrypted file uses: `aegis-256` from the better-sqlite3 driver, or `sqlcipher-4` from the
   * Expo driver.
   */
  cipher?: 'aegis-256' | 'sqlcipher-4'
  /** The version of the encryption engine, which an encrypted database reports. */
  cipherVersion?: string
  /** The identifier that came with the current key. */
  keyId?: string
  /** The time at which the last key change finished, in milliseconds since the Unix epoch. */
  changedAt?: number
  /** The current key change, or the last one. */
  change: EncryptionChangeStatus
}

import { type EncryptionKeySource, type ResolvedEncryptionKey, resolveEncryptionKey, wipeEncryptionKey } from './key.js'

/**
 * The settings that an open passes to the driver for an encrypted database.
 *
 * @internal
 */
export interface KeyedOpenOptions {
  encryption?: { key: Uint8Array }
}

/**
 * Holds the key function of one open database, and calls it each time the database opens connections, so that no key
 * bytes stay in memory between opens.
 *
 * @internal
 */
export class EncryptionKeyring {
  private lastKeyId: string | undefined

  constructor(private source: EncryptionKeySource | null) {}

  /** `true` when the database opens under a key. */
  get encrypted(): boolean {
    return this.source !== null
  }

  /** The identifier that came with the key that the last open used, or `undefined` when it came with none. */
  get keyId(): string | undefined {
    return this.lastKeyId
  }

  /** The key function that the database currently opens under, or `null` for a plaintext database. */
  get keySource(): EncryptionKeySource | null {
    return this.source
  }

  /**
   * Makes the database open every later connection under a new key function, or as plaintext.
   *
   * @param source - The key function of the key that the file now holds, or `null` for a plaintext file.
   */
  replace(source: EncryptionKeySource | null): void {
    this.source = source
    this.lastKeyId = undefined
  }

  /**
   * Calls the key function once, passes the key to the opens, and wipes Sirannon's copy of the key once they settle.
   *
   * @param open - Opens the connections with the settings that carry the key.
   * @returns What `open` returns.
   */
  async openWith<T>(
    open: (options: KeyedOpenOptions, key: ResolvedEncryptionKey | undefined) => Promise<T>,
  ): Promise<T> {
    if (this.source === null) return open({}, undefined)
    const key = await resolveEncryptionKey(this.source)
    this.lastKeyId = key.id
    try {
      return await open({ encryption: { key: key.bytes } }, key)
    } finally {
      wipeEncryptionKey(key)
    }
  }

  /**
   * Calls the key function and returns a copy of the key bytes, which the caller hands to a writer worker and wipes.
   *
   * @returns The key bytes.
   */
  async freshKeyBytes(): Promise<Uint8Array> {
    if (this.source === null) throw new Error('A plaintext database has no key to hand to its writer worker')
    const key = await resolveEncryptionKey(this.source)
    this.lastKeyId = key.id
    return key.bytes
  }
}

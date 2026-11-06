import * as v from 'valibot'
import { EncryptionError } from './errors.js'
import type { EncryptionKey } from './types.js'

/**
 * The number of bytes in every key that Sirannon accepts, which the engine uses as the raw cipher key with no key
 * derivation.
 *
 * @internal
 */
export const ENCRYPTION_KEY_BYTES = 32

const RAW_KEY_SCHEMA = v.pipe(
  v.instance(Uint8Array),
  v.check(bytes => bytes.length === ENCRYPTION_KEY_BYTES),
)
const KEY_SCHEMA = v.union([RAW_KEY_SCHEMA, v.object({ key: RAW_KEY_SCHEMA, id: v.optional(v.string()) })])

/**
 * A function that returns a database key, such as {@link EncryptionOptions.key}.
 *
 * @internal
 */
export type EncryptionKeySource = () => EncryptionKey | Promise<EncryptionKey>

/**
 * A key that Sirannon copied out of a key function's result, which the caller wipes once the connections that need it
 * are open.
 *
 * @internal
 */
export interface ResolvedEncryptionKey {
  /** A copy of the 32 key bytes that no other code holds. */
  bytes: Uint8Array
  /** The identifier that came with the key. */
  id?: string
}

/**
 * Calls a key function and returns a copy of its key, after checking that the key is exactly 32 bytes.
 *
 * @param source - The key function.
 * @returns The copied key bytes and the identifier that came with them.
 * @throws An `EncryptionError` with `ENCRYPTION_KEY_UNAVAILABLE` when the function throws or rejects, carrying that
 * failure as its cause, and with `INVALID_ENCRYPTION_KEY` when it returns anything but 32 bytes.
 * @internal
 */
export async function resolveEncryptionKey(source: EncryptionKeySource): Promise<ResolvedEncryptionKey> {
  let returned: unknown
  try {
    returned = await source()
  } catch (err) {
    throw new EncryptionError(
      'The encryption key function threw or rejected, so Sirannon has no key to open the database with',
      'ENCRYPTION_KEY_UNAVAILABLE',
      err,
    )
  }
  const parsed = v.safeParse(KEY_SCHEMA, returned)
  if (!parsed.success) {
    throw new EncryptionError(
      `The encryption key function returned no ${ENCRYPTION_KEY_BYTES}-byte key. Return ${ENCRYPTION_KEY_BYTES} random bytes as a Uint8Array, alone or as { key, id }; Sirannon accepts no password and no string`,
      'INVALID_ENCRYPTION_KEY',
    )
  }
  const value = parsed.output
  if (value instanceof Uint8Array) return { bytes: Uint8Array.from(value) }
  return { bytes: Uint8Array.from(value.key), ...(value.id === undefined ? {} : { id: value.id }) }
}

/**
 * Overwrites a resolved key's bytes with zeros, so that the copy holds no key once the connections are open.
 *
 * @param key - The key to wipe, or `undefined` for a plaintext database.
 * @internal
 */
export function wipeEncryptionKey(key: ResolvedEncryptionKey | undefined): void {
  key?.bytes.fill(0)
}

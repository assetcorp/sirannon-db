import type { EncryptionKeySource } from './encryption/key.js'
import type { DatabaseOptions, SirannonOptions } from './types.js'

/**
 * Records the key that a finished key change left a database under, so that the reopen after a restore opens it under
 * that key.
 *
 * @param opened - The path and options that the registry reopens the database with.
 * @param key - The key function of the new key, or `null` for a plaintext file.
 * @internal
 */
export function rememberEncryptionKey(
  opened: { path: string; options?: DatabaseOptions } | undefined,
  key: EncryptionKeySource | null,
): void {
  if (opened === undefined) return
  const { encryption: _replaced, ...rest } = opened.options ?? {}
  opened.options = key === null ? rest : { ...rest, encryption: { key } }
}

export function withRegistryDefaults(
  registry: SirannonOptions,
  options?: DatabaseOptions,
): DatabaseOptions | undefined {
  const defaults: DatabaseOptions = {}
  if (registry.writerWorker !== undefined && options?.writerWorker === undefined) {
    defaults.writerWorker = registry.writerWorker
  }
  if (registry.cdcRetention !== undefined && options?.cdcRetention === undefined) {
    defaults.cdcRetention = registry.cdcRetention
  }
  if (registry.deviceCursorRetention !== undefined && options?.deviceCursorRetention === undefined) {
    defaults.deviceCursorRetention = registry.deviceCursorRetention
  }
  if (registry.maxChangesHeldForDevice !== undefined && options?.maxChangesHeldForDevice === undefined) {
    defaults.maxChangesHeldForDevice = registry.maxChangesHeldForDevice
  }

  if (Object.keys(defaults).length === 0) return options
  return { ...options, ...defaults }
}

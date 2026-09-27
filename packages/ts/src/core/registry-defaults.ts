import type { DatabaseOptions, SirannonOptions } from './types.js'

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

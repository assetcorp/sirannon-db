import { SirannonError } from '../errors.js'
import type { SQLiteDriver } from './types.js'

/**
 * Returns a frozen {@link SQLiteDriver} that holds your implementation's capabilities, its `open` function, and only the optional members that Sirannon supports.
 *
 * @param config - The driver implementation.
 * @returns The driver, which you can pass to a `Sirannon` registry.
 *
 * @public
 */
export function defineDriver(config: SQLiteDriver): SQLiteDriver {
  if (!config.capabilities || typeof config.open !== 'function') {
    throw new SirannonError('Driver must define capabilities and open()', 'INVALID_DRIVER')
  }
  if (config.worker !== undefined && typeof config.worker.specifier !== 'string') {
    throw new SirannonError('Driver worker entry must define a string specifier', 'INVALID_DRIVER')
  }
  return Object.freeze({
    capabilities: Object.freeze({ ...config.capabilities }),
    open: config.open,
    ...(config.worker ? { worker: Object.freeze({ ...config.worker }) } : {}),
    ...(config.startWriterHost ? { startWriterHost: config.startWriterHost } : {}),
    ...(config.createWriterContext ? { createWriterContext: config.createWriterContext } : {}),
    ...(config.createBackupEngine ? { createBackupEngine: config.createBackupEngine } : {}),
    ...(config.resolveExtensionPath ? { resolveExtensionPath: config.resolveExtensionPath } : {}),
  })
}

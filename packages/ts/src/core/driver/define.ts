import { SirannonError } from '../errors.js'
import type { SQLiteDriver } from './types.js'

/**
 * Returns a frozen {@link SQLiteDriver} that holds your implementation's capabilities, its `open` function, and only the optional members that Sirannon supports.
 *
 * Sirannon binds each of those functions to `config`, so that a method of a class instance can read the instance's own fields through `this`.
 *
 * @param config - The driver implementation, which can be a plain object or an instance of a class.
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
    open: config.open.bind(config),
    ...(config.worker ? { worker: Object.freeze({ ...config.worker }) } : {}),
    ...(config.startWriterHost ? { startWriterHost: config.startWriterHost.bind(config) } : {}),
    ...(config.createWriterContext ? { createWriterContext: config.createWriterContext.bind(config) } : {}),
    ...(config.createBackupEngine ? { createBackupEngine: config.createBackupEngine.bind(config) } : {}),
    ...(config.resolveExtensionPath ? { resolveExtensionPath: config.resolveExtensionPath.bind(config) } : {}),
  })
}

import { SirannonError } from '../errors.js'
import type { SQLiteDriver } from './types.js'

/**
 * Builds a {@link SQLiteDriver} from an implementation, copying across only the optional members Sirannon knows about.
 *
 * @param driver - The driver implementation.
 * @returns The driver, ready to pass to a `Sirannon` registry.
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

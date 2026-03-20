import { SirannonError } from '../errors.js'
import type { SQLiteDriver } from './types.js'

export function defineDriver(config: SQLiteDriver): SQLiteDriver {
  if (!config.capabilities || typeof config.open !== 'function') {
    throw new SirannonError('Driver must define capabilities and open()', 'INVALID_DRIVER')
  }
  return Object.freeze({
    capabilities: Object.freeze({ ...config.capabilities }),
    open: config.open,
  })
}

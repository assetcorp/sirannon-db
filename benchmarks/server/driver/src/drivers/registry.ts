// Map an engine name to its driver.

import type { Config } from '../config.ts'
import type { Driver } from './driver.ts'
import { PostgresDriver } from './postgres.ts'
import { SirannonDriver } from './sirannon.ts'

export function buildDriver(engine: string, config: Config, durability: string): Driver {
  if (engine === 'sirannon') {
    return new SirannonDriver(config.sirannon.baseUrl, config.sirannon.databaseId, durability, config.requestTimeoutMs)
  }
  if (engine === 'postgres') {
    return new PostgresDriver(config.postgres, durability)
  }
  throw new Error(`unknown engine ${JSON.stringify(engine)}; expected 'sirannon' or 'postgres'`)
}

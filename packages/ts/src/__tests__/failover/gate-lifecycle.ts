import { afterAll, afterEach, beforeAll, beforeEach } from 'vitest'
import {
  beginScenario,
  cleanupEnvironment,
  endScenario,
  type GateEnvironment,
  startEnvironment,
} from './gate-environment.js'

export function useFailoverEnvironment(): () => GateEnvironment {
  let environment: GateEnvironment | null = null

  beforeAll(async () => {
    environment = await startEnvironment()
  }, 240_000)

  beforeEach(async () => {
    if (environment) await beginScenario(environment)
  }, 60_000)

  afterEach(async () => {
    if (environment) await endScenario(environment)
  }, 60_000)

  afterAll(async () => {
    if (environment) await cleanupEnvironment(environment)
    environment = null
  }, 120_000)

  return () => {
    if (!environment) throw new Error('The failover environment did not start')
    return environment
  }
}

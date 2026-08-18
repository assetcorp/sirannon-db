import { describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../better-sqlite3/index.js'
import { bunSqlite } from '../bun/index.js'
import { nodeSqlite } from '../node/index.js'

const driversDeclaringExtensionSupport = [
  ['better-sqlite3', betterSqlite3()],
  ['node:sqlite', nodeSqlite()],
  ['bun:sqlite', bunSqlite()],
] as const

describe('extension support across the drivers that declare it', () => {
  for (const [label, driver] of driversDeclaringExtensionSupport) {
    it(`declares extension support and resolves a bare name to an absolute path on ${label}`, () => {
      expect(driver.capabilities.extensions).toBe(true)
      expect(typeof driver.resolveExtensionPath).toBe('function')
      expect(driver.resolveExtensionPath?.('probe.so')).toMatch(/^([/\\]|[A-Za-z]:)/)
    })
  }
})

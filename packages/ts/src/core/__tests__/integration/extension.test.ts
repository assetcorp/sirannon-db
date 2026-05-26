import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { SirannonError } from '../../errors.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integration-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('Extension loading via Database', () => {
  it('throws ExtensionError for nonexistent extension path', async () => {
    const db = await Database.create('test', join(tempDir, 'ext.db'), testDriver)

    await expect(db.loadExtension('/nonexistent/extension.so')).rejects.toThrow(SirannonError)
    await expect(db.loadExtension('/nonexistent/extension.so')).rejects.toThrow('Failed to load extension')

    await db.close()
  })
})

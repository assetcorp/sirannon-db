import { mkdirSync, readdirSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { createTenantResolver } from '../../../core/lifecycle/tenant.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { freshDirectory, readTotals } from './sqlite-oracle.js'

const FILE_NAME_LIMIT = 255
const SQLITE_JOURNAL_SUFFIX = '-journal'
const LONGEST_FILE_NAME_WITH_ROOM_FOR_JOURNAL = FILE_NAME_LIMIT - SQLITE_JOURNAL_SUFFIX.length
const DEFAULT_EXTENSION = '.db'
const LONG_EXTENSION = '.sqlite3'
const SQLITE_SIDE_FILE = /-(wal|shm|journal)$/

let dir: string
let tenantsDir: string
const registries: Sirannon[] = []

function tenantRegistry(extension?: string): Sirannon {
  const sirannon = new Sirannon({
    driver: betterSqlite3(),
    lifecycle: { autoOpen: { resolver: createTenantResolver({ basePath: tenantsDir, extension }) } },
  })
  registries.push(sirannon)
  return sirannon
}

function databaseFiles(directory: string): string[] {
  return readdirSync(directory)
    .filter(name => !SQLITE_SIDE_FILE.test(name))
    .sort()
}

function idFillingFileName(fileNameLength: number, extension: string): string {
  return 'a'.repeat(fileNameLength - extension.length)
}

beforeEach(() => {
  dir = freshDirectory('tenants')
  tenantsDir = join(dir, 'tenants')
  mkdirSync(tenantsDir)
})

afterEach(async () => {
  for (const sirannon of registries.splice(0)) await sirannon.shutdown()
  rmSync(dir, { recursive: true, force: true })
})

describe('tenant identifiers that reach the resolver', () => {
  it('opens no database and creates no file for an identifier that could escape the folder or misname a file, even among valid ones arriving at once', async () => {
    const sirannon = tenantRegistry()
    const hostile = [
      '',
      '.',
      '..',
      '../escape',
      '../../etc/passwd',
      'acme/../escape',
      'acme/orders',
      'acme\\orders',
      '..\\escape',
      `/${'escape'}`,
      '%2e%2e%2fescape',
      'acme.db',
      'acme.',
      'acme ',
      ' acme',
      'ac me',
      'acme\n',
      'acme\u0000',
      'acme\u0000/../escape',
      'acme​',
      '-acme',
      '_acme',
      'café',
      'ａｃｍｅ',
      'acme:orders',
      'acme*',
      '$(touch escape)',
      idFillingFileName(FILE_NAME_LIMIT + 1, DEFAULT_EXTENSION),
      'a'.repeat(1_000_000),
    ]
    const valid = [
      'acme',
      '7eleven',
      'globex-east_2',
      idFillingFileName(LONGEST_FILE_NAME_WITH_ROOM_FOR_JOURNAL, DEFAULT_EXTENSION),
    ]

    const answers = await Promise.all([...hostile, ...valid].map(id => sirannon.resolve(id)))

    hostile.forEach((id, at) => {
      expect(answers[at], JSON.stringify(id.slice(0, 40))).toBeUndefined()
      expect(sirannon.has(id), JSON.stringify(id.slice(0, 40))).toBe(false)
    })
    expect(answers.slice(hostile.length).map(db => db?.closed)).toEqual(valid.map(() => false))
    expect(databaseFiles(tenantsDir)).toEqual(valid.map(id => `${id}${DEFAULT_EXTENSION}`).sort())
    expect(readdirSync(dir)).toEqual(['tenants'])
  })

  it('counts the extension toward the 255-character file name limit', async () => {
    const sirannon = tenantRegistry(LONG_EXTENSION)
    const opening = idFillingFileName(LONGEST_FILE_NAME_WITH_ROOM_FOR_JOURNAL, LONG_EXTENSION)
    const pastLimit = idFillingFileName(FILE_NAME_LIMIT + 1, LONG_EXTENSION)

    const opened = await sirannon.resolve(opening)
    const refused = await sirannon.resolve(pastLimit)

    expect(opened?.closed).toBe(false)
    expect(refused).toBeUndefined()
    expect(`${pastLimit}${LONG_EXTENSION}`).toHaveLength(FILE_NAME_LIMIT + 1)
    expect(databaseFiles(tenantsDir)).toEqual([`${opening}${LONG_EXTENSION}`])
  })

  it('refuses an identifier with a capital letter, so that Acme and acme can never open the same file on a case-insensitive file system', async () => {
    const sirannon = tenantRegistry()
    const acme = await sirannon.resolve('acme')
    await acme?.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')
    await acme?.execute('INSERT INTO orders (total) VALUES (1)')

    const variants = await Promise.all(['Acme', 'ACME', 'acmE', 'Globex'].map(id => sirannon.resolve(id)))

    expect(variants).toEqual([undefined, undefined, undefined, undefined])
    expect([...sirannon.databases().keys()]).toEqual(['acme'])
    expect(databaseFiles(tenantsDir)).toEqual(['acme.db'])
    expect(readTotals(join(tenantsDir, 'acme.db'))).toEqual([1])
  })
})

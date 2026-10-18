import { execFile } from 'node:child_process'
import { cpSync, existsSync, mkdirSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { freshDirectory, readTotals } from './sqlite-oracle.js'

const run = promisify(execFile)

const PACKAGE_ROOT = fileURLToPath(new URL('../../../../', import.meta.url))
const BUILT_CORE_ENTRY = join(PACKAGE_ROOT, 'dist', 'core', 'index.mjs')
const INSTALLED_DRIVER = 'better-sqlite3'
const CHILD_DEADLINE_MS = 30_000
const TYPE_DECLARATION = /\.d\.ts$/

interface PackageManifest {
  name: string
  peerDependenciesMeta: Record<string, { optional?: boolean }>
}

const manifest = JSON.parse(readFileSync(join(PACKAGE_ROOT, 'package.json'), 'utf8')) as PackageManifest
const absentPeers = Object.entries(manifest.peerDependenciesMeta)
  .filter(([name, meta]) => meta.optional === true && name !== INSTALLED_DRIVER)
  .map(([name]) => name)

const CHILD_SCRIPT = `
const absent = JSON.parse(process.env.SIRANNON_ABSENT_PEERS)
const unresolved = []
for (const peer of absent) {
  try {
    await import(peer)
  } catch (err) {
    if (err.code === 'ERR_MODULE_NOT_FOUND') unresolved.push(peer)
  }
}
const { Sirannon } = await import('${manifest.name}')
const { betterSqlite3 } = await import('${manifest.name}/driver/better-sqlite3')
const sirannon = new Sirannon({ driver: betterSqlite3() })
const db = await sirannon.open('orders', process.env.SIRANNON_DATABASE_PATH)
await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')
await db.execute('INSERT INTO orders (total) VALUES (?)', [42])
const rows = await db.query('SELECT total FROM orders')
await sirannon.shutdown()
process.stdout.write(JSON.stringify({ unresolved, rows }))
`

let project: string

function installBuiltPackage(): void {
  const installed = join(project, 'node_modules', ...manifest.name.split('/'))
  mkdirSync(installed, { recursive: true })
  cpSync(join(PACKAGE_ROOT, 'dist'), join(installed, 'dist'), {
    recursive: true,
    filter: source => !TYPE_DECLARATION.test(source),
  })
  writeFileSync(join(installed, 'package.json'), readFileSync(join(PACKAGE_ROOT, 'package.json')))
  symlinkSync(
    realpathSync(join(PACKAGE_ROOT, 'node_modules', INSTALLED_DRIVER)),
    join(project, 'node_modules', INSTALLED_DRIVER),
    'junction',
  )
  writeFileSync(join(project, 'package.json'), JSON.stringify({ name: 'shop', private: true, type: 'module' }))
}

beforeEach(() => {
  project = freshDirectory('peers')
})

afterEach(() => {
  rmSync(project, { recursive: true, force: true })
})

describe.skipIf(!existsSync(BUILT_CORE_ENTRY))('the published package in a project without its optional peers', () => {
  it('imports the core entry and opens, writes, and reads a database with only the better-sqlite3 driver installed', async () => {
    installBuiltPackage()
    const databasePath = join(project, 'orders.db')
    const { NODE_OPTIONS: _nodeOptions, NODE_PATH: _nodePath, ...inherited } = process.env

    const { stdout } = await run(process.execPath, ['--input-type=module', '--eval', CHILD_SCRIPT], {
      cwd: project,
      timeout: CHILD_DEADLINE_MS,
      env: {
        ...inherited,
        SIRANNON_ABSENT_PEERS: JSON.stringify(absentPeers),
        SIRANNON_DATABASE_PATH: databasePath,
      },
    })

    expect(absentPeers.length).toBeGreaterThan(0)
    expect(JSON.parse(stdout)).toEqual({ unresolved: absentPeers, rows: [{ total: 42 }] })
    expect(readTotals(databasePath)).toEqual([42])
  })
})

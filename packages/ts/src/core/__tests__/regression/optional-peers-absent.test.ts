import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const AUDITED_OPTIONAL_PEERS = [
  'croner',
  'etcd3',
  '@grpc/grpc-js',
  'grpc-health-check',
  'uWebSockets.js',
  'wa-sqlite',
  'expo-sqlite',
  '@bufbuild/protobuf',
]

describe('core entry import with optional peers absent', () => {
  let dir = ''

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-no-optional-peers-'))
  })

  afterEach(() => {
    for (const peer of AUDITED_OPTIONAL_PEERS) {
      vi.doUnmock(peer)
    }
    vi.resetModules()
    rmSync(dir, { recursive: true, force: true })
  })

  it('imports the core entry and runs create/execute/query/close without any optional peer', async () => {
    for (const peer of AUDITED_OPTIONAL_PEERS) {
      vi.doMock(peer, () => {
        throw new Error(`Cannot find package '${peer}'`)
      })
    }
    vi.resetModules()

    const { Database } = await import('../../index.js')
    const { betterSqlite3 } = await import('../../../drivers/better-sqlite3/index.js')

    const db = await Database.create('regression', join(dir, 'regression.db'), betterSqlite3())
    try {
      await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)')
      const insert = await db.execute('INSERT INTO items (label) VALUES (?)', ['first'])
      expect(insert.changes).toBe(1)

      const rows = await db.query<{ label: string }>('SELECT label FROM items ORDER BY id')
      expect(rows).toEqual([{ label: 'first' }])
    } finally {
      await db.close()
    }
  })
})

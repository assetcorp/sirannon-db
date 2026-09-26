import type { DatabaseSync } from 'node:sqlite'
import { describe, expect, it, vi } from 'vitest'

vi.mock('node:sqlite', async importOriginal => {
  const actual = await importOriginal<typeof import('node:sqlite')>()
  return { ...actual, backup: undefined }
})

describe('Node driver copy on a Node.js build without node:sqlite backup', () => {
  it('fails with BACKUP_UNSUPPORTED', async () => {
    const { copyDatabaseWithNodeSqlite } = await import('../node/copy.js')
    const db = { isTransaction: false } as unknown as DatabaseSync

    await expect(copyDatabaseWithNodeSqlite(db, { destPath: '/tmp/copy.db', pagesPerStep: 100 })).rejects.toMatchObject(
      {
        code: 'BACKUP_UNSUPPORTED',
      },
    )
  })
})

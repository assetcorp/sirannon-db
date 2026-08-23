import { existsSync, mkdirSync, statSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { BackupError, SirannonError } from '../../errors.js'
import { testDriver } from '../helpers/test-driver.js'
import { createTestDb, tempDirPerTest } from './shared.js'

const temp = tempDirPerTest()

describe('BackupManager', () => {
  const manager = new BackupManager()

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('backup', () => {
    it('creates a valid SQLite backup file', async () => {
      const conn = await createTestDb(temp.path)
      const destPath = join(temp.path, 'backup.db')

      await manager.backup(conn, destPath)

      expect(existsSync(destPath)).toBe(true)

      const backupConn = await testDriver.open(destPath, { readonly: true, walMode: false })
      const stmt = await backupConn.prepare('SELECT * FROM users ORDER BY id')
      const rows = (await stmt.all()) as {
        id: number
        name: string
        age: number
      }[]
      expect(rows).toHaveLength(2)
      expect(rows[0].name).toBe('Alice')
      expect(rows[1].name).toBe('Bob')
      await backupConn.close()
      await conn.close()
    })

    it('reports the file it wrote, the pages it moved, and how long that took', async () => {
      const conn = await createTestDb(temp.path)
      const destPath = join(temp.path, 'reported.db')

      const report = await manager.backup(conn, destPath)

      expect(report.destPath).toBe(destPath)
      expect(report.runId).toMatch(/^[0-9a-f]{16}$/)
      expect(report.pageCount).toBeGreaterThan(0)
      expect(report.pageSize).toBeGreaterThan(0)
      expect(report.byteLength).toBe(statSync(destPath).size)
      expect(report.byteLength).toBe(report.pageCount * report.pageSize)
      expect(report.restarts).toBe(0)
      expect(report.finishedAt).toBeGreaterThanOrEqual(report.startedAt)
      expect(report.durationMs).toBe(report.finishedAt - report.startedAt)
      await conn.close()
    })

    it('gives every copy a run identifier of its own', async () => {
      const conn = await createTestDb(temp.path)

      const first = await manager.backup(conn, join(temp.path, 'first.db'))
      const second = await manager.backup(conn, join(temp.path, 'second.db'))

      expect(first.runId).not.toBe(second.runId)
      await conn.close()
    })

    it('reads the page size before it starts copying, so a copy it finishes is never discarded', async () => {
      const conn = await createTestDb(temp.path)
      const destPath = join(temp.path, 'page-size-failure.db')
      let copies = 0
      const failing = {
        ...conn,
        copyDatabase: async (options: unknown) => {
          copies++
          return (conn.copyDatabase as (o: unknown) => Promise<unknown>)(options)
        },
        prepare: async (sql: string) => {
          if (sql.includes('page_size')) throw new Error('the page size query failed')
          return conn.prepare(sql)
        },
      } as unknown as SQLiteConnection

      await expect(manager.backup(failing, destPath)).rejects.toThrow(BackupError)

      expect(copies).toBe(0)
      expect(existsSync(destPath)).toBe(false)
      await conn.close()
    })

    it('reports a stalled copy without waiting for that copy to stop', async () => {
      vi.useFakeTimers()
      try {
        const destPath = join(temp.path, 'stalled.db')
        let stopCopy: (() => void) | undefined
        const hanging = {
          async prepare() {
            return { get: async () => ({ page_size: 4096 }) }
          },
          copyDatabase() {
            writeFileSync(destPath, 'half a database')
            return new Promise<never>((_, reject) => {
              stopCopy = () => reject(new Error('the copy gave up'))
            })
          },
        } as unknown as SQLiteConnection

        const settled = manager.backup(hanging, destPath).then(
          () => new Error('the copy reported success'),
          (err: Error) => err,
        )
        await vi.advanceTimersByTimeAsync(30_000)

        const failure = await settled
        expect(failure).toBeInstanceOf(SirannonError)
        if (failure instanceof SirannonError) expect(failure.code).toBe('BACKUP_STALLED')
        expect(failure.message).toContain('moved no pages')
        expect(existsSync(destPath)).toBe(true)

        stopCopy?.()
        await vi.advanceTimersByTimeAsync(0)
        expect(existsSync(destPath)).toBe(false)
      } finally {
        vi.useRealTimers()
      }
    })

    it('leaves no rejection loose when the copy it stopped waiting on fails', async () => {
      vi.useFakeTimers()
      const loose: unknown[] = []
      const collect = (reason: unknown) => loose.push(reason)
      process.on('unhandledRejection', collect)
      try {
        const destPath = join(temp.path, 'loose.db')
        let stopCopy: (() => void) | undefined
        const hanging = {
          async prepare() {
            return { get: async () => ({ page_size: 4096 }) }
          },
          copyDatabase() {
            writeFileSync(destPath, 'half a database')
            return new Promise<never>((_, reject) => {
              stopCopy = () => reject(new Error('the copy gave up'))
            })
          },
        } as unknown as SQLiteConnection

        const settled = manager.backup(hanging, destPath).then(
          () => undefined,
          () => undefined,
        )
        await vi.advanceTimersByTimeAsync(30_000)
        await settled
        stopCopy?.()
        await vi.advanceTimersByTimeAsync(0)

        expect(loose).toEqual([])
        expect(existsSync(destPath)).toBe(false)
      } finally {
        process.off('unhandledRejection', collect)
        vi.useRealTimers()
      }
    })

    it('preserves all rows and schema in the backup', async () => {
      const conn = await createTestDb(temp.path)
      await conn.exec('CREATE TABLE products (sku TEXT PRIMARY KEY, price REAL)')
      await conn.exec("INSERT INTO products (sku, price) VALUES ('WIDGET-01', 9.99)")

      const destPath = join(temp.path, 'full-backup.db')
      await manager.backup(conn, destPath)

      const backupConn = await testDriver.open(destPath, { readonly: true, walMode: false })
      const usersStmt = await backupConn.prepare('SELECT count(*) as cnt FROM users')
      const users = (await usersStmt.get()) as { cnt: number }
      const productsStmt = await backupConn.prepare('SELECT * FROM products')
      const products = (await productsStmt.all()) as { sku: string; price: number }[]
      expect(users.cnt).toBe(2)
      expect(products).toHaveLength(1)
      expect(products[0].sku).toBe('WIDGET-01')
      await backupConn.close()
      await conn.close()
    })

    it('creates parent directories when they do not exist', async () => {
      const conn = await createTestDb(temp.path)
      const nested = join(temp.path, 'a', 'b', 'c')
      const destPath = join(nested, 'backup.db')

      await manager.backup(conn, destPath)

      expect(existsSync(destPath)).toBe(true)
      await conn.close()
    })

    it('throws BackupError with BACKUP_ERROR code when destination already exists', async () => {
      const conn = await createTestDb(temp.path)
      const destPath = join(temp.path, 'existing.db')
      writeFileSync(destPath, '')

      try {
        await manager.backup(conn, destPath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).code).toBe('BACKUP_ERROR')
        expect((err as BackupError).message).toContain('already exists')
      }
      await conn.close()
    })

    it('throws BackupError with BACKUP_ERROR code when database is closed', async () => {
      const conn = await createTestDb(temp.path)
      await conn.close()
      const destPath = join(temp.path, 'closed-backup.db')

      try {
        await manager.backup(conn, destPath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).code).toBe('BACKUP_ERROR')
      }
    })

    it('throws BackupError when backing up to the source database path', async () => {
      const conn = await createTestDb(temp.path)
      const sourcePath = join(temp.path, 'source.db')

      try {
        await manager.backup(conn, sourcePath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).code).toBe('BACKUP_ERROR')
        expect((err as BackupError).message).toContain('already exists')
      }
      await conn.close()
    })

    it('handles paths with spaces', async () => {
      const conn = await createTestDb(temp.path)
      const spacedDir = join(temp.path, 'dir with spaces')
      mkdirSync(spacedDir, { recursive: true })
      const destPath = join(spacedDir, 'backup file.db')

      await manager.backup(conn, destPath)

      expect(existsSync(destPath)).toBe(true)
      const backupConn = await testDriver.open(destPath, { readonly: true, walMode: false })
      const stmt = await backupConn.prepare('SELECT * FROM users')
      const rows = await stmt.all()
      expect(rows).toHaveLength(2)
      await backupConn.close()
      await conn.close()
    })

    it('handles paths with single quotes', async () => {
      const conn = await createTestDb(temp.path)
      const quotedDir = join(temp.path, "it's a dir")
      mkdirSync(quotedDir, { recursive: true })
      const destPath = join(quotedDir, 'backup.db')

      await manager.backup(conn, destPath)

      expect(existsSync(destPath)).toBe(true)
      const backupConn = await testDriver.open(destPath, { readonly: true, walMode: false })
      const stmt = await backupConn.prepare('SELECT * FROM users')
      const rows = await stmt.all()
      expect(rows).toHaveLength(2)
      await backupConn.close()
      await conn.close()
    })

    it('cleans up partial files on failure', async () => {
      const conn = await createTestDb(temp.path)
      await conn.close()
      const destPath = join(temp.path, 'partial.db')

      await expect(manager.backup(conn, destPath)).rejects.toThrow(BackupError)
      expect(existsSync(destPath)).toBe(false)
    })

    it('rejects backup paths with control characters', async () => {
      const conn = await createTestDb(temp.path)
      const badPath = `${join(temp.path, 'bad')}.db`

      await expect(manager.backup(conn, badPath)).rejects.toThrow(BackupError)
      await conn.close()
    })

    it('throws when backup directory creation fails', async () => {
      const conn = await createTestDb(temp.path)
      const blocked = join(temp.path, 'blocked')
      writeFileSync(blocked, 'not-a-directory')
      const destPath = join(blocked, 'nested', 'backup.db')

      await expect(manager.backup(conn, destPath)).rejects.toThrow(BackupError)
      await conn.close()
    })

    it('formats non-Error values thrown during backup execution', async () => {
      const fakeConn = {
        async prepare() {
          return { get: async () => ({ page_size: 4096 }) }
        },
        async copyDatabase() {
          throw 'string copy failure'
        },
      } as unknown as SQLiteConnection
      const destPath = join(temp.path, 'non-error-exec.db')

      try {
        await manager.backup(fakeConn, destPath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).message).toContain('string copy failure')
      }
    })

    it('formats non-Error values thrown while creating destination directory', async () => {
      vi.resetModules()
      const fsActual = await vi.importActual<typeof import('node:fs')>('node:fs')

      try {
        vi.doMock('node:fs', () => ({
          ...fsActual,
          existsSync: fsActual.existsSync,
          lstatSync: fsActual.lstatSync,
          readdirSync: fsActual.readdirSync,
          rmSync: fsActual.rmSync,
          mkdirSync: () => {
            throw 'string mkdir failure'
          },
        }))

        const { BackupManager: MockedBackupManager } = await import('../../backup/backup.js')
        const mockedManager = new MockedBackupManager()
        const conn = await createTestDb(temp.path)
        const destPath = join(temp.path, 'nested', 'backup.db')

        try {
          await mockedManager.backup(conn, destPath)
          expect.unreachable('should have thrown')
        } catch (err) {
          expect((err as Error).message).toContain('string mkdir failure')
        } finally {
          await conn.close()
        }
      } finally {
        vi.doUnmock('node:fs')
        vi.resetModules()
      }
    })
  })
})

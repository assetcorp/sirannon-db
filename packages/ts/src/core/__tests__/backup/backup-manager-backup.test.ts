import { existsSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { BackupError } from '../../errors.js'
import { testDriver } from '../helpers/test-driver.js'
import { createTestDb, useTempDir } from './shared.js'

const temp = useTempDir()

describe('BackupManager', () => {
  const manager = new BackupManager()

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
        async exec() {
          throw 'string exec failure'
        },
      } as unknown as SQLiteConnection
      const destPath = join(temp.path, 'non-error-exec.db')

      try {
        await manager.backup(fakeConn, destPath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).message).toContain('string exec failure')
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

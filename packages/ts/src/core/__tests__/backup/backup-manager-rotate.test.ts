import { mkdirSync, readdirSync, utimesSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import { BackupError } from '../../errors.js'
import { useTempDir } from './shared.js'

const temp = useTempDir()

describe('BackupManager', () => {
  const manager = new BackupManager()

  describe('rotate', () => {
    it('deletes oldest files beyond maxFiles', () => {
      const backupDir = join(temp.path, 'backups')
      mkdirSync(backupDir)

      const now = Date.now()
      for (let i = 0; i < 5; i++) {
        const filePath = join(backupDir, `backup-file-${i}.db`)
        writeFileSync(filePath, `data-${i}`)
        const time = new Date(now - (4 - i) * 2000)
        utimesSync(filePath, time, time)
      }

      manager.rotate(backupDir, 3)

      const remaining = readdirSync(backupDir).filter(f => f.endsWith('.db'))
      expect(remaining).toHaveLength(3)
    })

    it('keeps the most recent files and removes the oldest', () => {
      const backupDir = join(temp.path, 'backups')
      mkdirSync(backupDir)

      const now = Date.now()
      const files = [
        { name: 'backup-old.db', age: 3000 },
        { name: 'backup-mid.db', age: 2000 },
        { name: 'backup-new.db', age: 1000 },
      ]
      for (const f of files) {
        const filePath = join(backupDir, f.name)
        writeFileSync(filePath, 'data')
        const time = new Date(now - f.age)
        utimesSync(filePath, time, time)
      }

      manager.rotate(backupDir, 2)

      const remaining = readdirSync(backupDir)
      expect(remaining).toContain('backup-new.db')
      expect(remaining).toContain('backup-mid.db')
      expect(remaining).not.toContain('backup-old.db')
    })

    it('does nothing when file count is at or below maxFiles', () => {
      const backupDir = join(temp.path, 'backups')
      mkdirSync(backupDir)
      writeFileSync(join(backupDir, 'backup-a.db'), 'data')
      writeFileSync(join(backupDir, 'backup-b.db'), 'data')

      manager.rotate(backupDir, 5)

      const remaining = readdirSync(backupDir).filter(f => f.endsWith('.db'))
      expect(remaining).toHaveLength(2)
    })

    it('ignores files that do not match the backup naming convention', () => {
      const backupDir = join(temp.path, 'backups')
      mkdirSync(backupDir)

      const now = Date.now()
      writeFileSync(join(backupDir, 'backup-a.db'), 'data')
      utimesSync(join(backupDir, 'backup-a.db'), new Date(now - 2000), new Date(now - 2000))
      writeFileSync(join(backupDir, 'backup-b.db'), 'data')
      utimesSync(join(backupDir, 'backup-b.db'), new Date(now), new Date(now))
      writeFileSync(join(backupDir, 'other-file.db'), 'data')
      writeFileSync(join(backupDir, 'notes.txt'), 'data')

      manager.rotate(backupDir, 1)

      const remaining = readdirSync(backupDir)
      expect(remaining).toContain('other-file.db')
      expect(remaining).toContain('notes.txt')
      expect(remaining.filter(f => f.startsWith('backup-'))).toHaveLength(1)
      expect(remaining).toContain('backup-b.db')
    })

    it('handles a non-existent directory without throwing', () => {
      expect(() => manager.rotate(join(temp.path, 'nonexistent'), 3)).not.toThrow()
    })

    it('is a no-op when maxFiles is zero', () => {
      const backupDir = join(temp.path, 'backups')
      mkdirSync(backupDir)
      writeFileSync(join(backupDir, 'backup-a.db'), 'data')

      manager.rotate(backupDir, 0)

      expect(readdirSync(backupDir)).toHaveLength(1)
    })

    it('is a no-op when maxFiles is negative', () => {
      const backupDir = join(temp.path, 'backups')
      mkdirSync(backupDir)
      writeFileSync(join(backupDir, 'backup-a.db'), 'data')

      manager.rotate(backupDir, -1)

      expect(readdirSync(backupDir)).toHaveLength(1)
    })

    it('throws when backup directory cannot be read', () => {
      const filePath = join(temp.path, 'not-a-directory')
      writeFileSync(filePath, 'x')

      expect(() => manager.rotate(filePath, 2)).toThrow(BackupError)
    })

    it('formats non-Error values thrown while listing backup files', async () => {
      vi.resetModules()
      const fsActual = await vi.importActual<typeof import('node:fs')>('node:fs')

      try {
        vi.doMock('node:fs', () => ({
          ...fsActual,
          existsSync: () => true,
          readdirSync: () => {
            throw 'string readdir failure'
          },
        }))

        const { BackupError: MockedBackupError } = await import('../../errors.js')
        const { BackupManager: MockedBackupManager } = await import('../../backup/backup.js')
        const mockedManager = new MockedBackupManager()

        try {
          mockedManager.rotate(temp.path, 1)
          expect.unreachable('should have thrown')
        } catch (err) {
          expect(err).toBeInstanceOf(MockedBackupError)
          expect((err as InstanceType<typeof MockedBackupError>).message).toContain('string readdir failure')
        }
      } finally {
        vi.doUnmock('node:fs')
        vi.resetModules()
      }
    })
  })
})

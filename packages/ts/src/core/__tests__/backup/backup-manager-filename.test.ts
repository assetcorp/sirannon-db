import { describe, expect, it } from 'vitest'
import { BackupManager } from '../../backup/backup.js'

describe('BackupManager', () => {
  const manager = new BackupManager()

  describe('generateFilename', () => {
    it('returns a filename with backup prefix and .db extension', () => {
      const filename = manager.generateFilename()
      expect(filename).toMatch(/^backup-.*\.db$/)
    })

    it('includes an ISO-style timestamp with hyphens replacing colons and dots', () => {
      const filename = manager.generateFilename()
      expect(filename).toMatch(/^backup-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z\.db$/)
    })

    it('generates unique filenames across calls separated by time', async () => {
      const first = manager.generateFilename()
      await new Promise(r => setTimeout(r, 5))
      const second = manager.generateFilename()
      expect(first).not.toBe(second)
    })
  })
})

import { writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { copyLogRange } from '../../backup/wal-log.js'
import type { SirannonError } from '../../errors.js'
import { tempDirPerTest } from './shared.js'

const temp = tempDirPerTest()

describe('copying a run of bytes out of a log', () => {
  it('copies the whole run where the log holds it', async () => {
    const logPath = join(temp.path, 'full.wal')
    await writeFile(logPath, Buffer.alloc(256, 7))

    expect(await copyLogRange(logPath, 32, 256, join(temp.path, 'capture.wal'))).toBe(224)
  })

  it('fails where the log ends before the run of bytes the capture needs', async () => {
    const logPath = join(temp.path, 'short.wal')
    await writeFile(logPath, Buffer.alloc(64, 7))

    const error = (await copyLogRange(logPath, 0, 256, join(temp.path, 'capture.wal')).catch(
      (err: unknown) => err,
    )) as SirannonError

    expect(error.code).toBe('BACKUP_LOG_REWOUND')
    expect(error.message).toContain('ends at byte 64')
  })
})

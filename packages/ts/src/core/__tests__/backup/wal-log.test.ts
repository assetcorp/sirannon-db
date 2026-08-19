import { readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { copyLogRange } from '../../backup/wal-log.js'
import type { SirannonError } from '../../errors.js'
import { tempDirPerTest } from './shared.js'

const temp = tempDirPerTest()

describe('copying a run of bytes out of a log', () => {
  it('copies the whole run where the log holds it, byte for byte', async () => {
    const logPath = join(temp.path, 'full.wal')
    const capturePath = join(temp.path, 'capture.wal')
    const log = Buffer.from(Array.from({ length: 256 }, (_, at) => (at * 37) % 256))
    await writeFile(logPath, log)

    expect(await copyLogRange(logPath, 32, 256, capturePath)).toBe(224)
    expect(await readFile(capturePath)).toEqual(log.subarray(32, 256))
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

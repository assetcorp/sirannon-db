import { open, rename, rm } from 'node:fs/promises'
import type { DatabaseFileSystem } from '../core/driver/file-system.js'

const NO_SUCH_FILE_CODES: ReadonlySet<unknown> = new Set(['ENOENT', 'ENAMETOOLONG'])

function missingFile(err: unknown): boolean {
  return typeof err === 'object' && err !== null && NO_SUCH_FILE_CODES.has((err as { code?: unknown }).code)
}

/**
 * The file operations that the Node drivers supply for a key change and its recovery.
 *
 * @internal
 */
export const nodeDatabaseFiles: DatabaseFileSystem = {
  async readStart(path, length) {
    let handle: Awaited<ReturnType<typeof open>>
    try {
      handle = await open(path, 'r')
    } catch (err) {
      if (missingFile(err)) return null
      throw err
    }
    try {
      const buffer = new Uint8Array(length)
      const { bytesRead } = await handle.read(buffer, 0, length, 0)
      return buffer.subarray(0, bytesRead)
    } finally {
      await handle.close()
    }
  },
  async remove(path) {
    try {
      await rm(path, { force: true })
    } catch (err) {
      if (!missingFile(err)) throw err
    }
  },
  async rename(from, to) {
    await rename(from, to)
  },
}

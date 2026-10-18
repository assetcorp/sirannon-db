import { mkdir, readdir, readFile, stat, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import type { BackupDestination, BackupPiece } from '../../core/backup/destination.js'

export function directoryDestination(root: string): BackupDestination {
  const folderFor = (name: string) => join(root, encodeURIComponent(name))

  return {
    async writePiece(name, index, bytes) {
      await mkdir(folderFor(name), { recursive: true })
      await writeFile(join(folderFor(name), String(index)), bytes)
    },
    async writePieceIfAbsent(name, index, bytes) {
      await mkdir(folderFor(name), { recursive: true })
      try {
        await writeFile(join(folderFor(name), String(index)), bytes, { flag: 'wx' })
        return true
      } catch (err: unknown) {
        if ((err as NodeJS.ErrnoException).code === 'EEXIST') return false
        throw err
      }
    },
    async readPiece(name, index) {
      return new Uint8Array(await readFile(join(folderFor(name), String(index))))
    },
    async listPieces(name): Promise<BackupPiece[]> {
      let entries: string[]
      try {
        entries = await readdir(folderFor(name))
      } catch (err: unknown) {
        if ((err as NodeJS.ErrnoException).code === 'ENOENT') return []
        throw err
      }
      const pieces: BackupPiece[] = []
      for (const entry of entries) {
        const index = Number(entry)
        if (!Number.isSafeInteger(index)) continue
        pieces.push({ index, byteLength: (await stat(join(folderFor(name), entry))).size })
      }
      return pieces.sort((left, right) => right.index - left.index)
    },
  }
}

export async function storedBackupNames(root: string): Promise<string[]> {
  try {
    return (await readdir(root)).map(entry => decodeURIComponent(entry)).sort()
  } catch (err: unknown) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return []
    throw err
  }
}

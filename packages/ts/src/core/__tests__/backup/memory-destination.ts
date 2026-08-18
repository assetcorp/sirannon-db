import type { BackupDestination, BackupPiece } from '../../backup/destination.js'

export interface MemoryDestination extends BackupDestination {
  names(): string[]
  bytesFor(name: string): Uint8Array
  refusePiece(index: number): void
  refuseName(name: string | null): void
}

export function memoryDestination(): MemoryDestination {
  const files = new Map<string, Map<number, Uint8Array>>()
  let refusedIndex: number | null = null
  let refusedName: string | null = null

  const pieces = (name: string): Map<number, Uint8Array> => {
    const existing = files.get(name)
    if (existing) return existing
    const created = new Map<number, Uint8Array>()
    files.set(name, created)
    return created
  }

  return {
    async writePiece(name, index, bytes) {
      if (index === refusedIndex) throw new Error(`refusing piece ${index}`)
      if (name === refusedName) throw new Error(`refusing every piece of '${name}'`)
      pieces(name).set(index, bytes.slice())
    },
    async readPiece(name, index) {
      const piece = pieces(name).get(index)
      if (!piece) throw new Error(`no piece ${index} of '${name}'`)
      return piece
    },
    async listPieces(name): Promise<BackupPiece[]> {
      const listed = [...pieces(name).entries()].map(([index, bytes]) => ({
        index,
        byteLength: bytes.byteLength,
      }))
      return listed.sort((a, b) => b.index - a.index)
    },
    names() {
      return [...files.keys()]
    },
    bytesFor(name) {
      const stored = [...pieces(name).entries()].sort(([left], [right]) => left - right)
      const total = stored.reduce((sum, [, bytes]) => sum + bytes.byteLength, 0)
      const joined = new Uint8Array(total)
      let offset = 0
      for (const [, bytes] of stored) {
        joined.set(bytes, offset)
        offset += bytes.byteLength
      }
      return joined
    },
    refusePiece(index) {
      refusedIndex = index
    },
    refuseName(name) {
      refusedName = name
    },
  }
}

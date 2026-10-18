import { type ChildProcess, fork } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const READER_SCRIPT = fileURLToPath(new URL('./oracle-reader.mjs', import.meta.url))

interface Reply {
  id: number
  rows?: Record<string, unknown>[]
  error?: string
}

interface Waiter {
  resolve: (rows: Record<string, unknown>[]) => void
  reject: (error: Error) => void
}

class SeparateProcessReader {
  private readonly waiting = new Map<number, Waiter>()
  private nextId = 0

  constructor(private readonly child: ChildProcess) {
    child.on('message', (reply: Reply) => {
      const waiter = this.waiting.get(reply.id)
      if (waiter === undefined) return
      this.waiting.delete(reply.id)
      if (reply.error !== undefined) waiter.reject(new Error(`The oracle process failed to read: ${reply.error}`))
      else waiter.resolve(reply.rows ?? [])
    })
    child.on('exit', code => {
      for (const waiter of this.waiting.values()) waiter.reject(new Error(`The oracle process exited with ${code}`))
      this.waiting.clear()
    })
    child.unref()
    child.channel?.unref()
  }

  read<T = Record<string, unknown>>(path: string, sql: string, params: readonly unknown[] = []): Promise<T[]> {
    const id = this.nextId++
    return new Promise<T[]>((resolve, reject) => {
      this.waiting.set(id, { resolve: rows => resolve(rows as T[]), reject })
      this.child.send({ id, path, sql, params })
    })
  }
}

let reader: SeparateProcessReader | null = null

export function readInSeparateProcess<T = Record<string, unknown>>(
  path: string,
  sql: string,
  params: readonly unknown[] = [],
): Promise<T[]> {
  reader ??= new SeparateProcessReader(fork(READER_SCRIPT, [], { serialization: 'advanced', stdio: 'inherit' }))
  return reader.read<T>(path, sql, params)
}

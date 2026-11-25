import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const HELD_READ_MARKER = '/* sirannon_held_read */'

export function heldReadSql(sql: string): string {
  return `${sql} ${HELD_READ_MARKER}`
}

export interface HeldReads {
  readonly reached: Promise<void>
  release(): void
}

export interface ConnectionCountingDriver {
  readonly driver: SQLiteDriver
  openReaders(): number
  openWriters(): number
  writerOpens(): number
  holdReads(): HeldReads
}

interface ReadGate {
  reach(): void
  readonly released: Promise<void>
}

export function connectionCountingDriver(): ConnectionCountingDriver {
  const base = betterSqlite3()
  let openReaders = 0
  let openWriters = 0
  let writerOpens = 0
  let gate: ReadGate | null = null
  return {
    driver: defineDriver({
      ...base,
      open: async (path, options) => {
        const conn = await base.open(path, options)
        const isReader = options?.readonly === true
        if (isReader) openReaders++
        else {
          openWriters++
          writerOpens++
        }
        const close = conn.close.bind(conn)
        let closed = false
        conn.close = async () => {
          await close()
          if (closed) return
          closed = true
          if (isReader) openReaders--
          else openWriters--
        }
        if (!isReader) return conn
        const prepare = conn.prepare.bind(conn)
        conn.prepare = async (sql: string) => {
          const waiting = gate
          if (waiting !== null && sql.includes(HELD_READ_MARKER)) {
            waiting.reach()
            await waiting.released
          }
          return prepare(sql)
        }
        return conn
      },
    }),
    openReaders: () => openReaders,
    openWriters: () => openWriters,
    writerOpens: () => writerOpens,
    holdReads: () => {
      let reach = (): void => {}
      let release = (): void => {}
      const reached = new Promise<void>(resolve => {
        reach = resolve
      })
      const released = new Promise<void>(resolve => {
        release = resolve
      })
      gate = { reach, released }
      return {
        reached,
        release: () => {
          gate = null
          release()
        },
      }
    },
  }
}

import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { WSConnection, WSSendOutcome } from '../ws-connection.js'

export interface MockWSConnection extends WSConnection {
  messages: string[]
  closed: boolean
  closeCode?: number
  closeReason?: string
  sendOutcome: WSSendOutcome
}

export function createMockConnection(): MockWSConnection {
  const conn: MockWSConnection = {
    messages: [],
    closed: false,
    closeCode: undefined,
    closeReason: undefined,
    sendOutcome: 'sent',
    send(data: string): WSSendOutcome {
      if (conn.sendOutcome === 'dropped') {
        return 'dropped'
      }
      conn.messages.push(data)
      return conn.sendOutcome
    },
    close(code?: number, reason?: string) {
      conn.closed = true
      conn.closeCode = code
      conn.closeReason = reason
    },
  }
  return conn
}

export function createTestSirannon(): Sirannon {
  const driver = betterSqlite3()
  return new Sirannon({ driver })
}

export function parseMessages(conn: MockWSConnection): Record<string, unknown>[] {
  return conn.messages.map(m => JSON.parse(m))
}

export function lastMessage(conn: MockWSConnection): Record<string, unknown> {
  return JSON.parse(conn.messages[conn.messages.length - 1])
}

export function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

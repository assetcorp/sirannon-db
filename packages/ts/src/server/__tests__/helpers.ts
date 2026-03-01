import type { WSConnection } from '../ws-handler.js'

export interface MockWSConnection extends WSConnection {
  messages: string[]
  closed: boolean
  closeCode?: number
  closeReason?: string
}

export function createMockConnection(): MockWSConnection {
  const conn: MockWSConnection = {
    messages: [],
    closed: false,
    closeCode: undefined,
    closeReason: undefined,
    send(data: string) {
      conn.messages.push(data)
    },
    close(code?: number, reason?: string) {
      conn.closed = true
      conn.closeCode = code
      conn.closeReason = reason
    },
  }
  return conn
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

import type { WSConnection } from './ws-connection.js'

const MAX_MESSAGES_WHILE_OPENING = 256

const PAYLOADS_HELD_WHILE_OPENING = 4

interface HeldMessages {
  messages: string[]
  bytes: number
}

/**
 * Holds the messages that arrive on a WebSocket connection while the server still opens its database, so that the
 * handler answers them in order once the connection is ready, as a client may send its first request as soon as the
 * upgrade completes.
 *
 * @internal
 */
export class OpeningMessages {
  private readonly held = new Map<WSConnection, HeldMessages>()

  /**
   * Creates a store that holds at most 256 messages, and four times `maxPayloadLength` bytes of UTF-8, for each
   * connection.
   *
   * @param maxPayloadLength - The largest message, in bytes, that the handler accepts.
   */
  constructor(private readonly maxPayloadLength: number) {}

  /**
   * Starts holding the messages of a connection whose database the handler is opening.
   *
   * @param conn - The connection that the server has just upgraded.
   */
  begin(conn: WSConnection): void {
    this.held.set(conn, { messages: [], bytes: 0 })
  }

  /**
   * Reports whether the handler is still opening the database of a connection that has not closed.
   *
   * @param conn - The connection to look up.
   * @returns True between {@link OpeningMessages.begin} and {@link OpeningMessages.finish} for an open connection.
   */
  opening(conn: WSConnection): boolean {
    return this.held.has(conn)
  }

  /**
   * Holds one message of a connection that is still opening.
   *
   * @param conn - The connection that the message arrived on.
   * @param data - The text of the message.
   * @returns `held` when the store keeps the message, `full` when the connection has sent more than the store holds,
   * and `ready` when the connection is not opening, so the handler processes the message itself.
   */
  hold(conn: WSConnection, data: string): 'held' | 'full' | 'ready' {
    const entry = this.held.get(conn)
    if (!entry) return 'ready'
    const bytes = entry.bytes + Buffer.byteLength(data)
    if (
      entry.messages.length >= MAX_MESSAGES_WHILE_OPENING ||
      bytes > this.maxPayloadLength * PAYLOADS_HELD_WHILE_OPENING
    ) {
      this.held.delete(conn)
      return 'full'
    }
    entry.messages.push(data)
    entry.bytes = bytes
    return 'held'
  }

  /**
   * Stops holding the messages of a connection and returns them in the order that they arrived.
   *
   * @param conn - The connection whose database the handler has opened, or failed to open.
   * @returns The messages held for the connection, which is empty once the connection has closed.
   */
  finish(conn: WSConnection): string[] {
    const entry = this.held.get(conn)
    this.held.delete(conn)
    return entry?.messages ?? []
  }
}

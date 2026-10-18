import { SirannonError } from '../core/errors.js'
import type { ChangeEvent } from '../core/types.js'
import { clientErrorMessage } from './client-error.js'
import type { WSServerMessage } from './protocol.js'
import type { WSConnection, WSSendOutcome } from './ws-connection.js'
import { wireChangeEvent } from './ws-device-frames.js'

/**
 * Writes the server's messages to WebSocket connections, and passes each connection that refuses a frame to
 * `onDropped`, so that the handler closes that connection.
 *
 * @internal
 */
export class WSFrameWriter {
  /**
   * @param onDropped - Receives each connection that refused a frame at the backpressure bound, or a message that
   * cannot be serialised.
   */
  constructor(private readonly onDropped: (conn: WSConnection) => void) {}

  /**
   * Serialises one message and sends it as one frame.
   *
   * @param conn - The connection to write to.
   * @param msg - The message to send.
   * @returns Whether the connection sent the frame, buffered it, or refused it.
   */
  send(conn: WSConnection, msg: WSServerMessage): WSSendOutcome {
    let data: string
    try {
      data = JSON.stringify(msg)
    } catch {
      this.onDropped(conn)
      return 'dropped'
    }
    return this.sendText(conn, data)
  }

  /**
   * Sends one frame of text that the caller has already serialised.
   *
   * @param conn - The connection to write to.
   * @param data - The serialised message.
   * @returns Whether the connection sent the frame, buffered it, or refused it.
   */
  sendText(conn: WSConnection, data: string): WSSendOutcome {
    const outcome = conn.send(data)
    if (outcome === 'dropped') {
      this.onDropped(conn)
    }
    return outcome
  }

  /**
   * Sends an `error` message that answers one request.
   *
   * @param conn - The connection to write to.
   * @param id - The identifier of the request that failed, or an empty string for an error that answers no request.
   * @param code - The error code.
   * @param message - The error message.
   */
  sendError(conn: WSConnection, id: string, code: string, message: string): void {
    this.send(conn, { type: 'error', id, error: { code, message } })
  }

  /**
   * Sends an `error` message that carries the code and the message of a `SirannonError`, or `INTERNAL_ERROR` for any
   * other value, so that the reply reveals nothing about an unexpected failure.
   *
   * @param conn - The connection to write to.
   * @param id - The identifier of the request that failed.
   * @param err - The value that the request threw.
   */
  sendSirannonError(conn: WSConnection, id: string, err: unknown): void {
    const code = err instanceof SirannonError ? err.code : 'INTERNAL_ERROR'
    const message = err instanceof SirannonError ? clientErrorMessage(err) : 'An unexpected error occurred'
    this.sendError(conn, id, code, message)
  }

  /**
   * Sends one change event to a subscription.
   *
   * @param conn - The connection to write to.
   * @param subscriptionId - The identifier of the subscription.
   * @param event - The change event to send.
   * @returns Whether the connection sent the frame, buffered it, or refused it.
   */
  sendChange(conn: WSConnection, subscriptionId: string, event: ChangeEvent): WSSendOutcome {
    return this.send(conn, { type: 'change', id: subscriptionId, event: wireChangeEvent(event) })
  }
}

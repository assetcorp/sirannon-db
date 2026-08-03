/**
 * Result of writing a frame to a WebSocket connection.
 *
 * - `sent`: delivered with no backpressure.
 * - `buffered`: accepted into the socket's outbound buffer under the
 *   backpressure limit; it will drain over time and must not be resent.
 * - `dropped`: rejected because it would exceed the backpressure limit, so
 *   the frame was not delivered. The caller must fail loud rather than treat
 *   the request as answered.
 */
export type WSSendOutcome = 'sent' | 'buffered' | 'dropped'

/**
 * One open WebSocket, as the handler sends over it.
 *
 * @internal
 */
export interface WSConnection {
  /**
   * Sends one frame and reports whether it went out, was buffered, or was dropped.
   */
  send(data: string): WSSendOutcome
  /**
   * Closes the connection with a code and reason.
   */
  close(code?: number, reason?: string): void
}

export const WS_CLOSE_OVERLOADED = 4290

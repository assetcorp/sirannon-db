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
   * Reports how many bytes the socket still holds unsent.
   *
   * A `buffered` outcome promises no later drain notification, because a send that
   * queues only the tail of a frame can flush that tail without the socket ever
   * becoming writable again. A sender that paused itself reads this to find out
   * whether it is still waiting on anything.
   */
  bufferedAmount(): number
  /**
   * Writes a control frame so the socket flushes what it is holding.
   *
   * uWebSockets keeps a partial write queued until the next write on that socket,
   * so a sender with nothing left to send calls this to move the remainder.
   */
  flush(): void
  /**
   * Closes the connection with a code and reason.
   */
  close(code?: number, reason?: string): void
}

export const WS_CLOSE_OVERLOADED = 4290

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

export interface WSConnection {
  send(data: string): WSSendOutcome
  close(code?: number, reason?: string): void
}

/**
 * Application close code sent when a connection is torn down because its
 * outbound buffer exceeded the backpressure limit. Chosen from the private
 * 4000-4999 range and echoing HTTP 429 so clients can recognise an overload.
 */
export const WS_CLOSE_OVERLOADED = 4290

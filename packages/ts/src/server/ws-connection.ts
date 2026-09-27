/**
 * The outcome of writing one frame to a WebSocket connection.
 *
 * - `sent`: uWebSockets writes the whole frame with no backpressure.
 * - `buffered`: uWebSockets queues the frame in the socket's outbound buffer,
 *   under the backpressure limit, and sends it later, so the caller must not send it again.
 * - `dropped`: uWebSockets discards the frame, because the socket is closed or the
 *   frame would push its buffer past the backpressure limit, so the client never
 *   receives it. The caller must then report the request as failed.
 */
export type WSSendOutcome = 'sent' | 'buffered' | 'dropped'

/**
 * The methods that the handler uses to write to one open WebSocket.
 *
 * @internal
 */
export interface WSConnection {
  /**
   * Sends one frame and returns whether uWebSockets sent it, buffered it, or dropped it.
   */
  send(data: string): WSSendOutcome
  /**
   * Returns the number of bytes that the socket has buffered and not yet sent.
   *
   * After a `buffered` outcome, uWebSockets may never fire a drain event, because
   * when it queues only the tail of a frame, it can flush that tail without the
   * socket becoming writable again. A paused device stream calls this method to
   * check whether the socket still has bytes left to send.
   */
  bufferedAmount(): number
  /**
   * Sends a ping frame so that uWebSockets flushes the bytes that the socket has buffered.
   *
   * uWebSockets keeps a partial write queued until the next write on that socket,
   * so a caller with nothing left to send calls this method to send the rest.
   */
  flush(): void
  /**
   * Closes the connection with a close code and a reason.
   */
  close(code?: number, reason?: string): void
}

export const WS_CLOSE_OVERLOADED = 4290

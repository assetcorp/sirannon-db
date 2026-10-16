import type { WSConnection } from './ws-connection.js'
import { WS_CLOSE_GOING_AWAY } from './ws-connection.js'

const FLUSH_CHECK_INTERVAL_MS = 10

/**
 * The close reason that a server sends with close code 1001 while it shuts down.
 *
 * @internal
 */
export const SHUTTING_DOWN_REASON = 'Server shutting down'

/**
 * The message of the `HANDLER_CLOSED` error that answers a request arriving after the server began to close.
 *
 * @internal
 */
export const SHUTTING_DOWN_MESSAGE =
  'The server is shutting down, so it refuses every new request on this connection; send the request again once the client reconnects'

/**
 * Counts the requests that one WebSocket connection has in progress, so that closing the server can wait for each
 * request to send its reply before the server closes the connection.
 *
 * @internal
 */
export class InFlightRequests {
  private count = 0
  private onIdle: (() => void) | null = null
  private readonly settle = (): void => {
    this.count--
    if (this.count === 0) this.release()
  }

  /**
   * Counts one request until the promise of its handler settles.
   *
   * @param request - The promise that the message handler returned.
   */
  track(request: Promise<void>): void {
    this.count++
    request.then(this.settle, this.settle)
  }

  /**
   * Waits until no request is in progress.
   *
   * @param deadline - The signal that ends the wait when the close deadline passes.
   * @returns A promise that resolves once no request is in progress, once the connection closes, or once `deadline` aborts.
   */
  idle(deadline: AbortSignal): Promise<void> {
    if (this.count === 0 || deadline.aborted) return Promise.resolve()
    return new Promise(resolve => {
      const finish = (): void => {
        deadline.removeEventListener('abort', finish)
        this.onIdle = null
        resolve()
      }
      this.onIdle = finish
      deadline.addEventListener('abort', finish, { once: true })
    })
  }

  /** Ends the wait of {@link InFlightRequests.idle}, which the handler calls when the connection closes. */
  release(): void {
    const idle = this.onIdle
    this.onIdle = null
    idle?.()
  }
}

function untilFlushed(conn: WSConnection, deadline: AbortSignal): Promise<void> {
  if (deadline.aborted) return Promise.resolve()
  return new Promise(resolve => {
    let timer: ReturnType<typeof setTimeout> | undefined
    let previous = -1
    const finish = (): void => {
      clearTimeout(timer)
      deadline.removeEventListener('abort', finish)
      resolve()
    }
    const check = (): void => {
      const buffered = conn.bufferedAmount()
      if (buffered === 0) {
        finish()
        return
      }
      if (buffered === previous) conn.flush()
      previous = buffered
      timer = setTimeout(check, FLUSH_CHECK_INTERVAL_MS)
    }
    deadline.addEventListener('abort', finish, { once: true })
    check()
  })
}

/**
 * Closes one WebSocket connection with close code 1001 once its requests have replied and its outbound buffer has
 * emptied, so that the client receives every reply and the close frame before the server ends the socket.
 *
 * The function sends the close frame without waiting further once `deadline` aborts.
 *
 * @param conn - The connection to close.
 * @param requests - The requests that the connection has in progress.
 * @param deadline - The signal that aborts when the close deadline passes.
 * @returns A promise that resolves once the function has sent the close frame.
 * @internal
 */
export async function closeGoingAway(
  conn: WSConnection,
  requests: InFlightRequests,
  deadline: AbortSignal,
): Promise<void> {
  await requests.idle(deadline)
  await untilFlushed(conn, deadline)
  conn.close(WS_CLOSE_GOING_AWAY, SHUTTING_DOWN_REASON)
}

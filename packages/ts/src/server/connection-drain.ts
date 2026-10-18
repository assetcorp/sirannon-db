import type { HttpRequest, HttpResponse, TemplatedApp } from 'uWebSockets.js'

type RouteHandler = (res: HttpResponse, req: HttpRequest) => void | Promise<void>

/**
 * The route methods of a uWebSockets.js app.
 *
 * @internal
 */
export type RouteRegistrar = Pick<TemplatedApp, 'get' | 'post' | 'options' | 'any'>

const OWNING_DRAIN = Symbol('sirannon.owningDrain')

type DrainedResponse = HttpResponse & { [OWNING_DRAIN]?: ConnectionDrain | undefined }

/**
 * Records that a response has ended or that its client went away, so that a server waiting to close stops waiting for it.
 *
 * @param res - The response that ended or that uWebSockets.js aborted.
 * @internal
 */
export function settleResponse(res: HttpResponse): void {
  const drained = res as DrainedResponse
  const drain = drained[OWNING_DRAIN]
  if (drain === undefined) return
  drained[OWNING_DRAIN] = undefined
  drain.settle(res)
}

/**
 * Writes a whole response in one corked batch and then records that the response ended, so that a server waiting to
 * close stops waiting for it.
 *
 * Every route ends its response through this function, because a response that ends without that record holds a closing
 * server open until the close deadline passes.
 *
 * @param res - The response to write.
 * @param write - Writes the status, the headers, and the body, and ends the response, or upgrades it to a WebSocket.
 * @internal
 */
export function endResponse(res: HttpResponse, write: () => void): void {
  res.cork(write)
  settleResponse(res)
}

/**
 * Records whether the body of a request is still arriving, so that closing the server can cut a stalled upload.
 *
 * @param res - The response of the request whose body is arriving.
 * @param arriving - `true` while chunks of the body are still due, and `false` once the last one arrived.
 * @internal
 */
export function markBodyArriving(res: HttpResponse, arriving: boolean): void {
  ;(res as DrainedResponse)[OWNING_DRAIN]?.markBody(res, arriving)
}

/**
 * Tracks the HTTP requests that a server is answering, so that closing the server can let every request whose body has arrived finish before it ends the connections.
 *
 * @internal
 */
export class ConnectionDrain {
  private closing = false
  private pending = 0
  private readonly uploading = new Set<HttpResponse>()
  private onDrained: (() => void) | null = null

  /**
   * Records a request until its response ends or its client goes away, or closes its connection when the request arrives
   * after closing began.
   *
   * @param res - The response of the request that arrived.
   * @returns `true` when the server answers the request, and `false` when it closed the connection instead.
   */
  admit(res: HttpResponse): boolean {
    if (this.closing) {
      res.close()
      return false
    }
    this.pending++
    ;(res as DrainedResponse)[OWNING_DRAIN] = this
    return true
  }

  /**
   * Returns a route handler that passes each request through {@link ConnectionDrain.admit} before it runs `handler`.
   *
   * @param handler - The route handler to record requests for.
   * @returns The handler to register with uWebSockets.js.
   */
  track(handler: RouteHandler): RouteHandler {
    return (res, req) => (this.admit(res) ? handler(res, req) : undefined)
  }

  /**
   * Returns route methods that register each handler on `app` through {@link ConnectionDrain.track}.
   *
   * @param app - The app whose routes the server registers.
   * @returns The route methods to register every HTTP route with.
   */
  routesOf(app: TemplatedApp): RouteRegistrar {
    return {
      get: (pattern, handler) => app.get(pattern, this.track(handler)),
      post: (pattern, handler) => app.post(pattern, this.track(handler)),
      options: (pattern, handler) => app.options(pattern, this.track(handler)),
      any: (pattern, handler) => app.any(pattern, this.track(handler)),
    }
  }

  /**
   * Stops accepting requests, closes the connection of every request whose body is still arriving, and waits for every other request to finish.
   *
   * @param deadline - The signal that ends the wait when the close deadline passes.
   * @returns A promise that resolves once no request is in progress, or once `deadline` aborts.
   */
  close(deadline: AbortSignal): Promise<void> {
    this.closing = true
    for (const res of [...this.uploading]) res.close()
    if (this.pending === 0 || deadline.aborted) return Promise.resolve()
    return new Promise(resolve => {
      const finish = (): void => {
        deadline.removeEventListener('abort', finish)
        this.onDrained = null
        resolve()
      }
      this.onDrained = finish
      deadline.addEventListener('abort', finish, { once: true })
    })
  }

  /**
   * Records whether a tracked request's body is still arriving.
   *
   * @param res - The response of the tracked request.
   * @param arriving - Whether chunks of the body are still due.
   */
  markBody(res: HttpResponse, arriving: boolean): void {
    if (arriving) this.uploading.add(res)
    else this.uploading.delete(res)
  }

  /**
   * Stops tracking a request whose response ended or whose client went away.
   *
   * @param res - The response of the request.
   */
  settle(res: HttpResponse): void {
    this.uploading.delete(res)
    this.pending--
    if (this.pending === 0 && this.onDrained) {
      const drained = this.onDrained
      this.onDrained = null
      drained()
    }
  }
}

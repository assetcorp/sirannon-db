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
   * Returns a route handler that records each request until its response ends or its client goes away, and that closes the connection of a request that arrives after closing began.
   *
   * @param handler - The route handler to record requests for.
   * @returns The handler to register with uWebSockets.js.
   */
  track(handler: RouteHandler): RouteHandler {
    return (res, req) => {
      if (this.closing) {
        res.close()
        return
      }
      this.pending++
      ;(res as DrainedResponse)[OWNING_DRAIN] = this
      return handler(res, req)
    }
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
   * @returns A promise that resolves once no request is in progress.
   */
  close(): Promise<void> {
    this.closing = true
    for (const res of [...this.uploading]) res.close()
    if (this.pending === 0) return Promise.resolve()
    return new Promise(resolve => {
      this.onDrained = resolve
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

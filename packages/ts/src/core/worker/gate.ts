import { WriteOverloadError } from '../errors.js'

/**
 * Bounds writes in flight so a slow worker disk cannot let the queue grow
 * without limit while the event loop keeps accepting. Zero disables the gate.
 */
export class WriteGate {
  private inFlight = 0

  constructor(
    private readonly limit: number,
    private readonly retryAfterMs: number,
  ) {}

  run<T>(op: () => Promise<T>): Promise<T> {
    if (this.limit > 0 && this.inFlight >= this.limit) {
      return Promise.reject(new WriteOverloadError(this.limit, this.retryAfterMs))
    }
    this.inFlight++
    return op().finally(() => {
      this.inFlight--
    })
  }
}

import { WriteOverloadError } from '../errors.js'

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

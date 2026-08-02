import { RemoteError } from '../types.js'

interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  timer: ReturnType<typeof setTimeout> | undefined
}

export class PendingRequests {
  private readonly requests = new Map<string, PendingRequest>()

  constructor(private readonly timeoutMs: number) {}

  start<T>(id: string, send: () => void): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timer =
        this.timeoutMs > 0
          ? setTimeout(() => {
              this.requests.delete(id)
              reject(new RemoteError('TIMEOUT', `Request timed out after ${this.timeoutMs}ms`))
            }, this.timeoutMs)
          : undefined

      this.requests.set(id, { resolve: resolve as (value: unknown) => void, reject, timer })
      send()
    })
  }

  resolve(id: string, value: unknown): boolean {
    const pending = this.take(id)
    if (pending === undefined) return false
    pending.resolve(value)
    return true
  }

  reject(id: string, error: Error): boolean {
    const pending = this.take(id)
    if (pending === undefined) return false
    pending.reject(error)
    return true
  }

  rejectAll(error: Error): void {
    for (const [, pending] of this.requests) {
      clearTimeout(pending.timer)
      pending.reject(error)
    }
    this.requests.clear()
  }

  private take(id: string): PendingRequest | undefined {
    const pending = this.requests.get(id)
    if (pending === undefined) return undefined
    clearTimeout(pending.timer)
    this.requests.delete(id)
    return pending
  }
}

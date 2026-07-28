import type { Subscription } from '../types.js'

export type LiveQueryState<T> =
  | { status: 'pending' }
  | { status: 'ready'; rows: readonly T[]; revalidating: boolean }
  | { status: 'error'; error: Error }

export interface LiveQuery<T = Record<string, unknown>> {
  getState(): LiveQueryState<T>
  subscribe(listener: () => void): () => void
  close(): void
}

export interface LiveQueryDeps<T> {
  tables: readonly string[]
  read: () => Promise<T[]>
  watch: (table: string, onChange: () => void) => Subscription
}

export function createLiveQuery<T>(deps: LiveQueryDeps<T>): LiveQuery<T> {
  return new PolledLiveQuery(deps)
}

class PolledLiveQuery<T> implements LiveQuery<T> {
  private state: LiveQueryState<T> = { status: 'pending' }
  private readonly listeners = new Set<() => void>()
  private readonly subscriptions: Subscription[] = []
  private closed = false
  private reading = false
  private changed = false

  constructor(private readonly deps: LiveQueryDeps<T>) {
    for (const table of deps.tables) {
      this.subscriptions.push(deps.watch(table, () => this.onChange()))
    }
    void this.refresh()
  }

  getState(): LiveQueryState<T> {
    return this.state
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  close(): void {
    if (this.closed) return
    this.closed = true
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe()
    }
    this.subscriptions.length = 0
    this.listeners.clear()
  }

  private onChange(): void {
    if (this.closed) return
    this.changed = true
    const current = this.state
    if (current.status === 'ready' && !current.revalidating) {
      this.publish({ status: 'ready', rows: current.rows, revalidating: true })
    }
    void this.refresh()
  }

  private async refresh(): Promise<void> {
    if (this.reading || this.closed) return
    this.reading = true
    try {
      do {
        this.changed = false
        const rows = await this.deps.read()
        if (this.closed) return
        this.publish({ status: 'ready', rows, revalidating: this.changed })
      } while (this.changed && !this.closed)
    } catch (err) {
      if (!this.closed) {
        this.publish({ status: 'error', error: err instanceof Error ? err : new Error(String(err)) })
      }
    } finally {
      this.reading = false
    }
  }

  private publish(next: LiveQueryState<T>): void {
    this.state = next
    for (const listener of this.listeners) {
      try {
        listener()
      } catch {}
    }
  }
}

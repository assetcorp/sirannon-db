import type { LiveQuery, LiveQueryState } from '../core/live/types.js'

const PENDING: LiveQueryState<never> = { status: 'pending' }

export type OpenLiveQuery<Row> = () => Promise<LiveQuery<Row>>

export interface LiveQueryStore<Row> {
  readonly subscribe: (listener: () => void) => () => void
  readonly getSnapshot: () => LiveQueryState<Row>
}

export function createLiveQueryStore<Row>(open: OpenLiveQuery<Row> | null): LiveQueryStore<Row> {
  const listeners = new Set<() => void>()
  let snapshot: LiveQueryState<Row> = PENDING
  let query: LiveQuery<Row> | null = null
  let detach: (() => void) | null = null
  let generation = 0

  function publish(next: LiveQueryState<Row>): void {
    if (next === snapshot) return
    snapshot = next
    for (const listener of [...listeners]) listener()
  }

  function start(): void {
    if (open === null) return
    const opened = generation
    open().then(
      next => {
        if (opened !== generation) {
          void next.close().catch(discard)
          return
        }
        query = next
        detach = next.subscribe(() => publish(next.getState()))
        publish(next.getState())
      },
      (reason: unknown) => {
        if (opened !== generation) return
        publish({ status: 'error', error: reason instanceof Error ? reason : new Error(String(reason)) })
      },
    )
  }

  function stop(): void {
    generation++
    detach?.()
    detach = null
    const closing = query
    query = null
    snapshot = PENDING
    void closing?.close().catch(discard)
  }

  return {
    subscribe(listener: () => void): () => void {
      listeners.add(listener)
      if (listeners.size === 1) start()
      return () => {
        listeners.delete(listener)
        if (listeners.size === 0) stop()
      }
    },
    getSnapshot(): LiveQueryState<Row> {
      return snapshot
    },
  }
}

export function getServerSnapshot(): LiveQueryState<never> {
  return PENDING
}

function discard(): void {}

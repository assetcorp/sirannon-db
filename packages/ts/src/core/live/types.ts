export type LiveQueryState<T> =
  | { status: 'pending' }
  | { status: 'ready'; rows: readonly T[]; revalidating: boolean }
  | { status: 'error'; error: Error }

export type ResultOp<T> =
  | { op: 'insert'; index: number; row: T }
  | { op: 'update'; index: number; row: T }
  | { op: 'delete'; index: number }

export type LiveUpdate<T> =
  | { kind: 'rows' }
  | { kind: 'ops'; ops: readonly ResultOp<T>[] }
  | { kind: 'revalidating' }
  | { kind: 'error' }

export interface LiveQuery<T = Record<string, unknown>> {
  getState(): LiveQueryState<T>
  subscribe(listener: (update: LiveUpdate<T>) => void): () => void
  close(): Promise<void>
}

export interface LiveQueryOptions {
  rereadJitterMs?: number
  maxTransactionChanges?: number
}

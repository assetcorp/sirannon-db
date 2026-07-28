export type LiveQueryState<T> =
  | { status: 'pending' }
  | { status: 'ready'; rows: readonly T[]; revalidating: boolean }
  | { status: 'error'; error: Error }

export interface LiveQuery<T = Record<string, unknown>> {
  getState(): LiveQueryState<T>
  subscribe(listener: () => void): () => void
  close(): Promise<void>
}

export interface LiveQueryOptions {
  rereadJitterMs?: number
  maxTransactionChanges?: number
}

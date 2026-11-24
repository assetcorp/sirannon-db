import { AsyncLocalStorage } from 'node:async_hooks'
import type { WriterContext } from '../core/driver/types.js'

let heldWriters: AsyncLocalStorage<ReadonlySet<symbol>> | undefined

/**
 * Returns a writer context that marks the async work inside a held write through `AsyncLocalStorage`, so that a
 * write which a transaction's function makes through its database joins that transaction and a new caller waits
 * for the writer. Every database shares the async storage, so detaching background work clears every inherited
 * writer hold. Node and Bun both provide `AsyncLocalStorage` through `node:async_hooks`.
 *
 * @returns The context through which a driver tracks its write state.
 * @internal
 */
export function asyncLocalWriterContext(): WriterContext {
  heldWriters ??= new AsyncLocalStorage<ReadonlySet<symbol>>()
  const held = heldWriters
  const writer = Symbol()
  return {
    run: operation => held.run(new Set([...(held.getStore() ?? []), writer]), operation),
    isActive: () => held.getStore()?.has(writer) === true,
    exit: operation => held.exit(operation),
  }
}

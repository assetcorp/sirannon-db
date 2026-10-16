import { AsyncLocalStorage } from 'node:async_hooks'
import type { WriterContext } from '../core/driver/types.js'

/**
 * Returns a writer context that marks the async work inside a held write through `AsyncLocalStorage`, so that a
 * write which a transaction's function makes through its database joins that transaction and a new caller waits
 * for the writer. Node and Bun both provide `AsyncLocalStorage` through `node:async_hooks`.
 *
 * @returns The context through which a driver tracks its write state.
 * @internal
 */
export function asyncLocalWriterContext(): WriterContext {
  const held = new AsyncLocalStorage<true>()
  return {
    run: operation => held.run(true, operation),
    isActive: () => held.getStore() === true,
    exit: operation => held.exit(operation),
  }
}

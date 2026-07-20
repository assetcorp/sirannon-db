import { AsyncLocalStorage } from 'node:async_hooks'
import { resolve } from 'node:path'
import { BackupManager } from '../core/backup/backup.js'
import { BackupScheduler } from '../core/backup/scheduler.js'
import type { BackupEngine, WriterContext } from '../core/driver/types.js'

/**
 * The capabilities a Node-like runtime can back but a browser cannot. Only
 * drivers for those runtimes import this, which is what keeps `node:` modules
 * out of a bundle built for one that lacks them.
 */
export function nodeWriterContext(): WriterContext {
  const held = new AsyncLocalStorage<true>()
  return {
    run: operation => held.run(true, operation),
    isActive: () => held.getStore() === true,
    exit: operation => held.exit(operation),
  }
}

export function nodeResolveExtensionPath(extensionPath: string): string {
  return resolve(extensionPath)
}

export function nodeBackupEngine(): BackupEngine {
  const manager = new BackupManager()
  const scheduler = new BackupScheduler(manager)
  return {
    backup: (conn, destPath) => manager.backup(conn, destPath),
    schedule: (conn, options, runExclusive) => scheduler.schedule(conn, options, runExclusive),
  }
}

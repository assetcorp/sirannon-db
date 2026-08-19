import { AsyncLocalStorage } from 'node:async_hooks'
import { resolve } from 'node:path'
import { BackupManager } from '../core/backup/backup.js'
import { createBackupCycle } from '../core/backup/cycle.js'
import { BackupScheduler } from '../core/backup/scheduler.js'
import type { BackupStreamingSupport } from '../core/backup/streamed-copy.js'
import type { BackupEngine, SQLiteDriver, WriterContext } from '../core/driver/types.js'
import { resolveVfsExtensionPath } from './vfs-library.js'

/** What one Node driver tells the backup engine about streaming a copy.
 * @internal
 */
export interface NodeStreamingOptions {
  /** Driver the engine opens its own connection through. */
  driver: SQLiteDriver
  /** Whether SQLite parses URI file names in this runtime, which is how Sirannon names the destination. */
  uriFilenames: boolean
  /** Extension the operator named, which replaces the binary the install fetched. */
  extensionPath?: string
}

/**
 * Tracks which callers hold the writer, so a driver can tell work scheduled
 * from inside a write apart from a fresh caller.
 *
 * @returns The context a Node driver reports its write state through.
 *
 * @internal
 */
export function nodeWriterContext(): WriterContext {
  const held = new AsyncLocalStorage<true>()
  return {
    run: operation => held.run(true, operation),
    isActive: () => held.getStore() === true,
    exit: operation => held.exit(operation),
  }
}

/**
 * Returns an extension path in absolute form, because SQLite resolves a
 * relative path against the process's working directory rather than the
 * caller's.
 *
 * @param extensionPath - Path the caller gave, absolute or relative.
 * @returns The same file as an absolute path.
 *
 * @internal
 */
export function nodeResolveExtensionPath(extensionPath: string): string {
  return resolve(extensionPath)
}

/**
 * Works out whether a full copy can reach the destination without a local
 * file. It can once this host carries a compiled extension and the runtime
 * parses URI file names, because Sirannon names its virtual file system on the
 * copy through a URI parameter.
 *
 * @param options - Driver, runtime facts, and any extension path the operator named.
 * @returns What a streamed copy needs, or undefined where this runtime takes the staged route.
 *
 * @internal
 */
export function nodeStreamingSupport(options: NodeStreamingOptions): BackupStreamingSupport | undefined {
  if (!options.uriFilenames) return undefined
  const named = options.extensionPath ?? resolveVfsExtensionPath()
  if (!named) return undefined
  return {
    extensionPath: nodeResolveExtensionPath(named),
    openConnection: () => options.driver.open(':memory:', { walMode: false }),
  }
}

/**
 * Builds the backup engine both Node drivers run their copies through, together
 * with the scheduler that starts a copy on a timetable.
 *
 * @param streaming - What a streamed copy needs, where this runtime can carry one.
 * @returns The engine a driver hands to every database it opens.
 *
 * @internal
 */
export function nodeBackupEngine(streaming?: BackupStreamingSupport): BackupEngine {
  const manager = new BackupManager(streaming)
  const scheduler = new BackupScheduler(manager)
  return {
    backup: (conn, destPath, onFirstStep) => manager.backup(conn, destPath, onFirstStep),
    copyToDestination: (conn, request) => manager.copyToDestination(conn, request),
    streamsToDestination: () => manager.streamsToDestination(),
    createCycle: request => createBackupCycle(request),
    schedule: (conn, options, runExclusive) => scheduler.schedule(conn, options, runExclusive),
  }
}

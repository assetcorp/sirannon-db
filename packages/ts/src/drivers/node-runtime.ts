import { resolve } from 'node:path'
import { BackupManager } from '../core/backup/backup.js'
import { createBackupCycle } from '../core/backup/cycle-factory.js'
import { BackupScheduler } from '../core/backup/scheduler.js'
import type { BackupStreamingSupport } from '../core/backup/streamed-copy.js'
import type { BackupEngine, SQLiteDriver } from '../core/driver/types.js'
import { resolveVfsExtensionPath } from './vfs-library.js'

/** The settings that a Node driver passes to the backup engine for a streamed copy.
 * @internal
 */
export interface NodeStreamingOptions {
  /** The driver through which the engine opens its own connection. */
  driver: SQLiteDriver
  /** `true` when SQLite parses URI file names in this runtime, since Sirannon names the destination through a URI. */
  uriFilenames: boolean
  /** The extension path that the operator sets, which Sirannon uses in place of the installed platform binary. */
  extensionPath?: string
}

/**
 * Returns an extension path in absolute form, resolved against the process's
 * working directory at the time of the call.
 *
 * @param extensionPath - The path that the caller passes, absolute or relative.
 * @returns The same file as an absolute path.
 *
 * @internal
 */
export function nodeResolveExtensionPath(extensionPath: string): string {
  return resolve(extensionPath)
}

/**
 * Returns the support for streaming a full copy to its destination without a
 * local file. Sirannon streams a copy only when this host has a compiled
 * extension and the runtime parses URI file names, because it selects its
 * virtual file system on the copy through a URI parameter.
 *
 * @param options - The driver, the runtime's URI support, and any extension path that the operator sets.
 * @returns The extension path and connection opener for a streamed copy, or `undefined` when the copy has to go through a local file.
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
 * Returns the backup engine that both Node drivers use for their copies,
 * including a scheduler that starts a copy on a timetable.
 *
 * @param streaming - The streaming support from {@link nodeStreamingSupport}, when this runtime can stream a copy.
 * @returns The backup engine for the driver's databases.
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
    schedule: (conn, request) => scheduler.schedule(conn, request),
  }
}

import { type FSWatcher, realpathSync, watch } from 'node:fs'
import { stat } from 'node:fs/promises'
import { basename, dirname, join, resolve } from 'node:path'

interface FileWatchEntry {
  readonly onChange: () => void
  readonly onLost: () => void
  readonly filePaths: readonly string[]
  signature: string | null
}

interface DirectoryWatch {
  readonly watcher: FSWatcher
  readonly entriesByFileName: Map<string, Set<FileWatchEntry>>
  readonly entries: Set<FileWatchEntry>
  sweepTimer: ReturnType<typeof setTimeout> | null
}

const JOURNAL_SUFFIXES = ['-wal', '-journal'] as const
const WRITTEN_FILE_SUFFIXES = ['', ...JOURNAL_SUFFIXES] as const
const MISSED_EVENT_SWEEP_MS = 30_000
const ABSENT_FILE_SIGNATURE = '-'
const EMPTY_JOURNAL_SIGNATURE = ABSENT_FILE_SIGNATURE

const directoryWatches = new Map<string, DirectoryWatch>()

const foldsFileNameCase = process.platform === 'darwin' || process.platform === 'win32'

function fileNameKey(fileName: string): string {
  return foldsFileNameCase ? fileName.toLowerCase() : fileName
}

function resolvedDatabasePath(path: string): string | null {
  if (path === '' || path === ':memory:' || path.startsWith('file:')) return null
  const absolute = resolve(path)
  try {
    return realpathSync(absolute)
  } catch {
    return absolute
  }
}

function notifyEntry(entry: FileWatchEntry): void {
  try {
    entry.onChange()
  } catch {}
}

function isJournalFileName(fileName: string): boolean {
  const key = fileNameKey(fileName)
  return JOURNAL_SUFFIXES.some(suffix => key.endsWith(suffix))
}

function dispatchFileEvent(
  path: string,
  directory: DirectoryWatch,
  eventType: string,
  fileName: string | Buffer | null,
): void {
  if (fileName === null) {
    for (const entry of directory.entries) notifyEntry(entry)
    return
  }
  const name = fileName.toString()
  const entries = directory.entriesByFileName.get(fileNameKey(name))
  if (entries === undefined) return
  if (eventType === 'rename' && isJournalFileName(name)) {
    void notifyIfJournalExists(join(path, name), entries)
    return
  }
  for (const entry of entries) notifyEntry(entry)
}

async function notifyIfJournalExists(journalPath: string, entries: ReadonlySet<FileWatchEntry>): Promise<void> {
  try {
    await stat(journalPath)
  } catch {
    return
  }
  for (const entry of entries) notifyEntry(entry)
}

async function fileSignature(filePaths: readonly string[]): Promise<string> {
  const parts: string[] = []
  for (const filePath of filePaths) {
    try {
      const stats = await stat(filePath, { bigint: true })
      const emptyJournal = stats.size === 0n && isJournalFileName(basename(filePath))
      parts.push(emptyJournal ? EMPTY_JOURNAL_SIGNATURE : `${stats.ino}:${stats.size}:${stats.mtimeNs}`)
    } catch {
      parts.push(ABSENT_FILE_SIGNATURE)
    }
  }
  return parts.join('|')
}

async function recordSignature(
  directory: DirectoryWatch,
  entry: FileWatchEntry,
  notifyOnChange: boolean,
): Promise<void> {
  const signature = await fileSignature(entry.filePaths)
  if (!directory.entries.has(entry)) return
  const changed = entry.signature !== null && entry.signature !== signature
  entry.signature = signature
  if (changed && notifyOnChange) notifyEntry(entry)
}

async function sweepDirectory(path: string, directory: DirectoryWatch): Promise<void> {
  for (const entry of [...directory.entries]) {
    if (directoryWatches.get(path) !== directory) return
    await recordSignature(directory, entry, true)
  }
}

function armSweep(path: string, directory: DirectoryWatch): void {
  const timer = setTimeout(() => {
    directory.sweepTimer = null
    void sweepDirectory(path, directory).finally(() => {
      if (directoryWatches.get(path) === directory && directory.entries.size > 0) armSweep(path, directory)
    })
  }, MISSED_EVENT_SWEEP_MS) as ReturnType<typeof setTimeout> & { unref?: () => void }
  timer.unref?.()
  directory.sweepTimer = timer
}

function stopDirectory(path: string, directory: DirectoryWatch): void {
  directoryWatches.delete(path)
  if (directory.sweepTimer !== null) clearTimeout(directory.sweepTimer)
  directory.sweepTimer = null
  try {
    directory.watcher.close()
  } catch {}
}

function loseDirectory(path: string, directory: DirectoryWatch): void {
  if (directoryWatches.get(path) !== directory) return
  stopDirectory(path, directory)
  const lost = [...directory.entries]
  directory.entries.clear()
  directory.entriesByFileName.clear()
  for (const entry of lost) {
    try {
      entry.onLost()
    } catch {}
  }
}

function openDirectoryWatch(path: string): DirectoryWatch | null {
  let watcher: FSWatcher
  try {
    watcher = watch(path, { persistent: false })
  } catch {
    return null
  }
  const directory: DirectoryWatch = { watcher, entriesByFileName: new Map(), entries: new Set(), sweepTimer: null }
  watcher.on('change', (eventType, fileName) => dispatchFileEvent(path, directory, eventType, fileName))
  watcher.on('error', () => loseDirectory(path, directory))
  watcher.on('close', () => loseDirectory(path, directory))
  directoryWatches.set(path, directory)
  armSweep(path, directory)
  return directory
}

function removeEntry(path: string, directory: DirectoryWatch, keys: readonly string[], entry: FileWatchEntry): void {
  directory.entries.delete(entry)
  for (const key of keys) {
    const entries = directory.entriesByFileName.get(key)
    if (entries === undefined) continue
    entries.delete(entry)
    if (entries.size === 0) directory.entriesByFileName.delete(key)
  }
  if (directory.entries.size > 0 || directoryWatches.get(path) !== directory) return
  stopDirectory(path, directory)
}

/**
 * Watches a database file, its write-ahead log, and its rollback journal for writes, through one `fs.watch` on the
 * directory that holds them, which every database in that directory shares.
 *
 * Watching the directory follows a file that a key change or a restore replaces with a rename, and a directory holding
 * many databases costs one watch, which keeps a server of many databases under the per-user inotify watch limit. The
 * watch reports writes from every process, including this one. It skips the removal of a journal file, which SQLite
 * does when the last connection closes and which carries no write, so that a database which closes its idle
 * connections receives no read for it. Every 30 seconds the watch also compares the size, modification time, and
 * inode of each watched file with what it recorded, counting an empty journal as an absent one, and reports a write
 * for each database whose files differ, which catches a write whose event the kernel dropped when its event queue
 * overflowed.
 *
 * @param path - The database file.
 * @param onChange - Called after each write to one of the files, and must return at once.
 * @param onLost - Called once when the watch fails, after which `onChange` receives no more calls.
 * @returns The function that stops the watch, or `null` when the runtime cannot watch the directory, for an in-memory
 * database, and for a URI path.
 * @internal
 */
export function watchNodeDatabaseFile(path: string, onChange: () => void, onLost: () => void): (() => void) | null {
  const databasePath = resolvedDatabasePath(path)
  if (databasePath === null) return null
  const directoryPath = dirname(databasePath)
  const directory = directoryWatches.get(directoryPath) ?? openDirectoryWatch(directoryPath)
  if (directory === null) return null

  const fileName = basename(databasePath)
  const entry: FileWatchEntry = {
    onChange,
    onLost,
    filePaths: WRITTEN_FILE_SUFFIXES.map(suffix => `${databasePath}${suffix}`),
    signature: null,
  }
  const keys = WRITTEN_FILE_SUFFIXES.map(suffix => fileNameKey(`${fileName}${suffix}`))
  directory.entries.add(entry)
  for (const key of keys) {
    let entries = directory.entriesByFileName.get(key)
    if (entries === undefined) {
      entries = new Set()
      directory.entriesByFileName.set(key, entries)
    }
    entries.add(entry)
  }
  void recordSignature(directory, entry, false)

  let stopped = false
  return () => {
    if (stopped) return
    stopped = true
    removeEntry(directoryPath, directory, keys, entry)
  }
}

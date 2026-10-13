import { createHash } from 'node:crypto'
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { readdir, readFile, rename, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import type { BackupDestination, BackupPiece } from '../../../core/backup/destination.js'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'

const WAIT_STEP_MS = 5
const WAIT_LIMIT_MS = 8_000
const WRITING_SUFFIX = '.writing'

export const CHAIN_LIST_NAME = 'sirannon-backup-chain'
export const ORDER_PADDING = 200

export interface Placement {
  label: string
  driver: () => SQLiteDriver
  writerWorker: boolean
}

export const PLACEMENTS: Placement[] = [
  { label: 'better-sqlite3 with writes on the calling thread', driver: () => betterSqlite3(), writerWorker: false },
  { label: 'better-sqlite3 with writes on the writer worker', driver: () => betterSqlite3(), writerWorker: true },
  { label: 'node:sqlite with writes on the calling thread', driver: () => nodeSqlite(), writerWorker: false },
  { label: 'node:sqlite with writes on the writer worker', driver: () => nodeSqlite(), writerWorker: true },
]

export interface DestinationFaults {
  refuseWrite?: (name: string, index: number) => boolean
  refuseRead?: (name: string, index: number) => boolean
  refuseList?: (name: string) => boolean
  holdWrite?: (name: string, index: number) => Promise<void> | undefined
  holdRead?: (name: string, index: number) => Promise<void> | undefined
  holdList?: (name: string) => Promise<void> | undefined
  claimTaken?: (name: string, index: number) => boolean
  afterRead?: (name: string, index: number) => void
}

export interface DirectoryDestination extends BackupDestination {
  readonly root: string
  readonly faults: DestinationFaults
  readonly writes: { name: string; index: number }[]
  names(): string[]
  indices(name: string): number[]
  stored(name: string, index: number): Buffer
  replace(name: string, index: number, bytes: Uint8Array): void
  remove(name: string, index: number): void
}

function nameDirectory(root: string, name: string): string {
  return join(root, encodeURIComponent(name))
}

export function directoryDestination(root: string, options: { claims?: boolean } = {}): DirectoryDestination {
  mkdirSync(root, { recursive: true })
  const faults: DestinationFaults = {}
  const writes: { name: string; index: number }[] = []
  const pieceFile = (name: string, index: number) => join(nameDirectory(root, name), String(index))

  const store = async (name: string, index: number, bytes: Uint8Array) => {
    await faults.holdWrite?.(name, index)
    if (faults.refuseWrite?.(name, index)) throw new Error(`the bucket refused piece ${index} of '${name}'`)
    mkdirSync(nameDirectory(root, name), { recursive: true })
    const target = pieceFile(name, index)
    await writeFile(`${target}${WRITING_SUFFIX}`, bytes)
    await rename(`${target}${WRITING_SUFFIX}`, target)
    writes.push({ name, index })
  }

  const destination: DirectoryDestination = {
    root,
    faults,
    writes,
    writePiece: store,
    async readPiece(name, index) {
      await faults.holdRead?.(name, index)
      if (faults.refuseRead?.(name, index)) throw new Error(`the bucket refused to return piece ${index} of '${name}'`)
      const bytes = new Uint8Array(await readFile(pieceFile(name, index)))
      faults.afterRead?.(name, index)
      return bytes
    },
    async listPieces(name): Promise<BackupPiece[]> {
      await faults.holdList?.(name)
      if (faults.refuseList?.(name)) throw new Error(`the bucket refused to list '${name}'`)
      const directory = nameDirectory(root, name)
      const entries = await readdir(directory).catch(() => [] as string[])
      const listed: BackupPiece[] = []
      for (const entry of entries) {
        if (entry.endsWith(WRITING_SUFFIX)) continue
        listed.push({ index: Number(entry), byteLength: (await stat(join(directory, entry))).size })
      }
      return listed.sort((left, right) => right.index - left.index)
    },
    names() {
      if (!existsSync(root)) return []
      return readdirSync(root)
        .map(entry => decodeURIComponent(entry))
        .sort()
    },
    indices(name) {
      const directory = nameDirectory(root, name)
      if (!existsSync(directory)) return []
      return readdirSync(directory)
        .filter(entry => !entry.endsWith(WRITING_SUFFIX))
        .map(Number)
        .sort((left, right) => left - right)
    },
    stored(name, index) {
      return readFileSync(pieceFile(name, index))
    },
    replace(name, index, bytes) {
      mkdirSync(nameDirectory(root, name), { recursive: true })
      writeFileSync(pieceFile(name, index), bytes)
    },
    remove(name, index) {
      rmSync(pieceFile(name, index), { force: true })
    },
  }

  if (options.claims !== false) {
    destination.writePieceIfAbsent = async (name, index, bytes) => {
      await faults.holdWrite?.(name, index)
      if (faults.refuseWrite?.(name, index)) throw new Error(`the bucket refused piece ${index} of '${name}'`)
      if (faults.claimTaken?.(name, index)) return false
      mkdirSync(nameDirectory(root, name), { recursive: true })
      try {
        await writeFile(pieceFile(name, index), bytes, { flag: 'wx' })
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code === 'EEXIST') return false
        throw err
      }
      writes.push({ name, index })
      return true
    }
  }
  return destination
}

export interface Workspace {
  readonly dir: string
  path(name: string): string
  registry(driver?: SQLiteDriver): Sirannon
  open(name: string, options?: DatabaseOptions, driver?: SQLiteDriver): Promise<Database>
  destination(name?: string, options?: { claims?: boolean }): DirectoryDestination
  dispose(): Promise<void>
}

export function createWorkspace(label: string): Workspace {
  const dir = mkdtempSync(join(tmpdir(), `sirannon-backups-${label}-`))
  const registries: Sirannon[] = []
  const registry = (driver?: SQLiteDriver): Sirannon => {
    const sirannon = new Sirannon({ driver: driver ?? betterSqlite3() })
    registries.push(sirannon)
    return sirannon
  }
  return {
    dir,
    path: name => join(dir, name),
    registry,
    open: (name, options, driver) => registry(driver).open(name, join(dir, `${name}.db`), options),
    destination: (name = 'bucket', options) => directoryDestination(join(dir, name), options),
    dispose: async () => {
      for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
      rmSync(dir, { recursive: true, force: true })
    },
  }
}

export function readRaw<T = Record<string, unknown>>(path: string, sql: string, params: unknown[] = []): T[] {
  const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    return raw.prepare(sql).all(...params) as T[]
  } finally {
    raw.close()
  }
}

export function integrityOf(path: string): string {
  const [row] = readRaw<{ integrity_check: string }>(path, 'PRAGMA integrity_check')
  return row?.integrity_check ?? 'no answer'
}

export function orderLabels(path: string): string[] {
  return readRaw<{ label: string }>(path, 'SELECT label FROM orders ORDER BY id').map(row => row.label)
}

export function writeRaw(path: string, sql: string, params: unknown[] = []): void {
  const raw = new BetterSqlite3(path, { fileMustExist: true })
  try {
    raw.pragma('busy_timeout = 5000')
    if (params.length === 0) raw.exec(sql)
    else raw.prepare(sql).run(...params)
  } finally {
    raw.close()
  }
}

export const CREATE_ORDERS = 'CREATE TABLE orders (id INTEGER PRIMARY KEY, label TEXT NOT NULL, note TEXT NOT NULL)'
export const INSERT_ORDER = 'INSERT INTO orders (label, note) VALUES (?, ?)'

export function orderRows(prefix: string, count: number, padding = 0): [string, string][] {
  return Array.from({ length: count }, (_, at) => [`${prefix}-${at}`, ''.padEnd(padding, 'x')])
}

export async function seedOrders(db: Database, prefix: string, count: number, padding = 0): Promise<string[]> {
  await db.execute(CREATE_ORDERS)
  if (count > 0) await db.bulkLoad(INSERT_ORDER, orderRows(prefix, count, padding))
  return orderRows(prefix, count).map(([label]) => label)
}

export function sha256(bytes: Uint8Array): string {
  return createHash('sha256').update(bytes).digest('hex')
}

export function joinPieces(destination: DirectoryDestination, name: string, pieceBytes: number): Buffer {
  const indices = destination.indices(name)
  const last = indices.at(-1)
  if (last === undefined) return Buffer.alloc(0)
  const tail = destination.stored(name, last)
  const joined = Buffer.alloc(last * pieceBytes + tail.byteLength)
  for (const index of indices) destination.stored(name, index).copy(joined, index * pieceBytes)
  return joined
}

export function storedRecord<T = Record<string, unknown>>(
  destination: DirectoryDestination,
  name: string,
  index: number,
): T {
  return JSON.parse(destination.stored(name, index).toString('utf8')) as T
}

export interface StoredHead {
  chainId: string
  startedAt: number
  previousChainId?: string
}

export interface StoredRecord {
  kind: 'full' | 'change'
  chainId: string
  name: string
  sequence?: number
  pieceBytes: number
  pieceCount: number
  bytesWritten: number
  frameCount?: number
  capturedAt?: number
  finishedAt?: number
  fingerprint?: string
  position?: { logSequence: number; salt1: number; salt2: number; firstFrame: number; lastFrame: number }
}

export function storedHeads(destination: DirectoryDestination, chainName = CHAIN_LIST_NAME): StoredHead[] {
  return destination.indices(chainName).map(index => storedRecord<StoredHead>(destination, chainName, index))
}

export function storedChain(
  destination: DirectoryDestination,
  chainId: string,
  chainName = CHAIN_LIST_NAME,
): StoredRecord[] {
  const name = `${chainName}.${chainId}`
  return destination.indices(name).map(index => storedRecord<StoredRecord>(destination, name, index))
}

function sameLogRun(left: StoredRecord, right: StoredRecord): boolean {
  const a = left.position
  const b = right.position
  return a !== undefined && b !== undefined && a.salt1 === b.salt1 && a.salt2 === b.salt2
}

export function replayStoredChain(
  destination: DirectoryDestination,
  records: readonly StoredRecord[],
  outPath: string,
): void {
  const base = records.find(record => record.kind === 'full')
  if (!base) throw new Error('the chain holds no full copy to rebuild from')
  writeFileSync(outPath, joinPieces(destination, base.name, base.pieceBytes))
  const changes = records
    .filter(record => record.kind === 'change')
    .sort((left, right) => (left.sequence ?? 0) - (right.sequence ?? 0))
  let run: StoredRecord[] = []
  const replay = () => {
    if (run.length === 0) return
    const log = Buffer.concat(run.map(record => joinPieces(destination, record.name, record.pieceBytes)))
    rmSync(`${outPath}-shm`, { force: true })
    writeFileSync(`${outPath}-wal`, log)
    const raw = new BetterSqlite3(outPath)
    try {
      raw.pragma('wal_checkpoint(TRUNCATE)')
    } finally {
      raw.close()
    }
    run = []
  }
  for (const change of changes) {
    const previous = run.at(-1)
    if (previous && !sameLogRun(previous, change)) replay()
    run.push(change)
  }
  replay()
  rmSync(`${outPath}-wal`, { force: true })
  rmSync(`${outPath}-shm`, { force: true })
}

const WAL_HEADER_BYTES = 32
const WAL_FRAME_HEADER_BYTES = 24

export interface LogHeader {
  pageSize: number
  logSequence: number
  salt1: number
  salt2: number
}

export function logHeaderOf(bytes: Uint8Array): LogHeader {
  const view = Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  return {
    pageSize: view.readUInt32BE(8),
    logSequence: view.readUInt32BE(12),
    salt1: view.readUInt32BE(16),
    salt2: view.readUInt32BE(20),
  }
}

export function walBytes(path: string): number {
  const wal = `${path}-wal`
  return existsSync(wal) ? statSync(wal).size : 0
}

export function framesIn(logBytes: number, pageSize: number): number {
  if (logBytes < WAL_HEADER_BYTES) return 0
  return (logBytes - WAL_HEADER_BYTES) / (pageSize + WAL_FRAME_HEADER_BYTES)
}

export function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export async function waitFor(condition: () => boolean, what: string, limitMs = WAIT_LIMIT_MS): Promise<void> {
  const deadline = Date.now() + limitMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await pause(WAIT_STEP_MS)
  }
}

export interface Gate {
  promise: Promise<void>
  open(): void
}

export function gate(): Gate {
  let open: () => void = () => {}
  const promise = new Promise<void>(resolve => {
    open = resolve
  })
  return { promise, open }
}

export interface RejectionWatch {
  reasons: unknown[]
  stop(): void
}

export function watchUnhandledRejections(): RejectionWatch {
  const reasons: unknown[] = []
  const record = (reason: unknown) => reasons.push(reason)
  process.on('unhandledRejection', record)
  return { reasons, stop: () => process.off('unhandledRejection', record) }
}

export function codeOf(err: unknown): string | undefined {
  return (err as { code?: string } | null)?.code
}

export async function failureOf(promise: Promise<unknown>): Promise<Error & { code?: string }> {
  try {
    await promise
  } catch (err) {
    return err as Error & { code?: string }
  }
  throw new Error('the call succeeded where it had to fail')
}

export function filesIn(path: string): string[] {
  return existsSync(path) ? readdirSync(path).sort() : []
}

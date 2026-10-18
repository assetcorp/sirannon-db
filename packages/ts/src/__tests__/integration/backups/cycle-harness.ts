import { DatabaseSync } from 'node:sqlite'
import BetterSqlite3 from 'better-sqlite3'
import { expect } from 'vitest'
import { restoreBackup } from '../../../backup/index.js'
import type { BackupCycleOptions } from '../../../core/backup/cycle-options.js'
import type { BackupGroupMembership, BackupGroupSource, BackupSkip } from '../../../core/backup/preferred-node.js'
import type { BackupRunReport } from '../../../core/backup/report.js'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import {
  CHAIN_LIST_NAME,
  CREATE_ORDERS,
  type DirectoryDestination,
  INSERT_ORDER,
  integrityOf,
  orderLabels,
  replayStoredChain,
  type StoredRecord,
  storedChain,
  storedHeads,
  type Workspace,
} from './harness.js'

export interface CycleNode {
  db: Database
  path: string
  stagingDir: string
  destination: DirectoryDestination
  runs: BackupRunReport[]
  errors: (Error & { code?: string })[]
  skips: BackupSkip[]
}

export interface CycleNodeOptions {
  destination?: DirectoryDestination
  driver?: SQLiteDriver
  writerWorker?: boolean
  stagingDir?: string
  backups?: Partial<BackupCycleOptions>
}

export async function openCycleNode(
  workspace: Workspace,
  name: string,
  options: CycleNodeOptions = {},
): Promise<CycleNode> {
  const destination = options.destination ?? workspace.destination()
  const stagingDir = options.stagingDir ?? workspace.path(`${name}-staging`)
  const runs: BackupRunReport[] = []
  const errors: (Error & { code?: string })[] = []
  const skips: BackupSkip[] = []
  const path = workspace.path(`${name}.db`)
  const db = await workspace.registry(options.driver ?? betterSqlite3()).open(name, path, {
    writerWorker: options.writerWorker ?? false,
    backups: {
      destination,
      intervalMs: 0,
      stagingDir,
      onRun: report => runs.push(report),
      onError: err => errors.push(err),
      onSkip: skip => skips.push(skip),
      ...options.backups,
    },
  })
  return { db, path, stagingDir, destination, runs, errors, skips }
}

export async function addOrders(db: Database, labels: readonly string[]): Promise<void> {
  for (const label of labels) await db.execute(INSERT_ORDER, [label, ''])
}

export async function createOrders(db: Database): Promise<void> {
  await db.execute(CREATE_ORDERS)
}

export function onlyChainId(destination: DirectoryDestination, chainName = CHAIN_LIST_NAME): string {
  const heads = storedHeads(destination, chainName)
  expect(heads).toHaveLength(1)
  const head = heads[0]
  if (!head) throw new Error('the destination lists no chain')
  return head.chainId
}

export function newestChainId(destination: DirectoryDestination, chainName = CHAIN_LIST_NAME): string {
  const head = storedHeads(destination, chainName).at(-1)
  if (!head) throw new Error('the destination lists no chain')
  return head.chainId
}

export function changeRecords(records: readonly StoredRecord[]): StoredRecord[] {
  return records.filter(record => record.kind === 'change')
}

export async function expectRestoresTo(
  workspace: Workspace,
  destination: DirectoryDestination,
  expected: readonly string[],
  label: string,
  chainName = CHAIN_LIST_NAME,
): Promise<void> {
  const chainId = newestChainId(destination, chainName)
  const replayed = workspace.path(`${label}-replayed.db`)
  replayStoredChain(destination, storedChain(destination, chainId, chainName), replayed)
  expect(integrityOf(replayed), label).toBe('ok')
  expect(orderLabels(replayed), label).toEqual(expected)

  const restored = workspace.path(`${label}-restored.db`)
  const report = await restoreBackup({ destination, driver: betterSqlite3(), destPath: restored, chainName })
  expect(report.chainId, label).toBe(chainId)
  expect(integrityOf(restored), label).toBe('ok')
  expect(orderLabels(restored), label).toEqual(expected)
}

export interface HeldReader {
  release(): void
}

export type SqliteBuild = 'better-sqlite3' | 'node:sqlite'

export function holdReadTransaction(path: string, build: SqliteBuild = 'better-sqlite3'): HeldReader {
  const reader =
    build === 'node:sqlite'
      ? new DatabaseSync(path, { readOnly: true })
      : new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  reader.exec('BEGIN')
  reader.prepare('SELECT count(*) FROM orders').get()
  return {
    release: () => {
      reader.exec('COMMIT')
      reader.close()
    },
  }
}

export function truncateLogFromOutside(path: string): void {
  const raw = new BetterSqlite3(path, { fileMustExist: true })
  try {
    raw.pragma('busy_timeout = 5000')
    raw.pragma('wal_checkpoint(TRUNCATE)')
  } finally {
    raw.close()
  }
}

export interface ControlledGroup extends BackupGroupSource {
  membership: BackupGroupMembership
  failure: Error | null
  reads: number
}

export function controlledGroup(nodeId: string, membership: BackupGroupMembership): ControlledGroup {
  const group: ControlledGroup = {
    nodeId,
    membership,
    failure: null,
    reads: 0,
    async readMembership() {
      group.reads++
      if (group.failure) throw group.failure
      return { primaryNodeId: group.membership.primaryNodeId, nodeIds: [...group.membership.nodeIds] }
    },
  }
  return group
}

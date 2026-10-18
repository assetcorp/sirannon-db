import { join } from 'node:path'
import type { BackupChain } from '../../../core/backup/chain.js'
import type { BackupCycleOptions } from '../../../core/backup/cycle-options.js'
import type { BackupCycleStatus } from '../../../core/backup/cycle-status.js'
import type { BackupRunReport } from '../../../core/backup/report.js'
import type { Database } from '../../../core/database.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { BackupRestoreStatus } from '../../../server/backup-restore-runs.js'
import {
  CREATE_ORDERS,
  type DirectoryDestination,
  directoryDestination,
  INSERT_ORDER,
  integrityOf,
  orderLabels,
  replayStoredChain,
  storedChain,
  storedHeads,
} from '../../integration/backups/harness.js'
import { sendRaw, waitUntil, within } from '../lib/served-registry.js'

export interface BackedUpDatabase {
  db: Database
  path: string
  destination: DirectoryDestination
  runs: BackupRunReport[]
  errors: (Error & { code?: string })[]
}

export async function openBackedUp(
  sirannon: Sirannon,
  dir: string,
  id: string,
  backups: Partial<BackupCycleOptions> = {},
): Promise<BackedUpDatabase> {
  const destination = directoryDestination(join(dir, `${id}-bucket`))
  const runs: BackupRunReport[] = []
  const errors: (Error & { code?: string })[] = []
  const path = join(dir, `${id}.db`)
  const db = await sirannon.open(id, path, {
    backups: {
      destination,
      intervalMs: 0,
      stagingDir: join(dir, `${id}-staging`),
      onRun: report => runs.push(report),
      onError: err => errors.push(err),
      ...backups,
    },
  })
  await waitUntil(() => runs.length + errors.length > 0 && !db.backupStatus().running, 'the opening full copy')
  await db.execute(CREATE_ORDERS)
  return { db, path, destination, runs, errors }
}

export async function addLabels(db: Database, labels: readonly string[]): Promise<void> {
  for (const label of labels) await db.execute(INSERT_ORDER, [label, ''])
}

export async function backupStatus(base: string, id: string): Promise<BackupCycleStatus> {
  const answer = await sendRaw(base, 'GET', `/db/${id}/backup`)
  if (answer.status !== 200) throw new Error(`the status route answered ${answer.status}: ${answer.text}`)
  return answer.body as BackupCycleStatus
}

export async function triggerAndSettle(base: string, backedUp: BackedUpDatabase, id: string): Promise<BackupRunReport> {
  const runsBefore = backedUp.runs.length
  const errorsBefore = backedUp.errors.length
  const answer = await sendRaw(base, 'POST', `/db/${id}/backup`)
  if (answer.status !== 202) throw new Error(`the trigger answered ${answer.status}: ${answer.text}`)
  await waitUntil(
    () => backedUp.runs.length > runsBefore || backedUp.errors.length > errorsBefore,
    'the triggered turn finishing',
  )
  await cycleIdle(base, id)
  const report = backedUp.runs.at(-1)
  if (!report || backedUp.errors.length > errorsBefore) {
    throw new Error(`the turn failed: ${backedUp.errors.at(-1)?.message}`)
  }
  return report
}

export async function cycleIdle(base: string, id: string): Promise<BackupCycleStatus> {
  let status = await backupStatus(base, id)
  await within(
    (async () => {
      while (status.running) {
        await new Promise(resolve => setTimeout(resolve, 5))
        status = await backupStatus(base, id)
      }
    })(),
    'the cycle going idle',
  )
  return status
}

export async function restoreSettled(base: string, id: string): Promise<BackupRestoreStatus> {
  let status: BackupRestoreStatus = { state: 'idle' }
  await within(
    (async () => {
      for (;;) {
        status = (await sendRaw(base, 'GET', `/db/${id}/backup/restore`)).body as BackupRestoreStatus
        if (status.state === 'done' || status.state === 'failed') return
        await new Promise(resolve => setTimeout(resolve, 10))
      }
    })(),
    'the restore finishing',
    20_000,
  )
  return status
}

export async function chainsOf(base: string, id: string): Promise<BackupChain[]> {
  const answer = await sendRaw(base, 'GET', `/db/${id}/backup/chain`)
  if (answer.status !== 200) throw new Error(`the chain route answered ${answer.status}: ${answer.text}`)
  return (answer.body as { chains: BackupChain[] }).chains
}

export function replayNewestChain(destination: DirectoryDestination, outPath: string): string[] {
  const head = storedHeads(destination).at(-1)
  if (!head) throw new Error('the destination lists no chain')
  replayStoredChain(destination, storedChain(destination, head.chainId), outPath)
  if (integrityOf(outPath) !== 'ok') throw new Error('the replayed chain is not a sound database')
  return orderLabels(outPath)
}

export function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

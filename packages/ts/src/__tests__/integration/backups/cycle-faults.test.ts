import { spawnSync } from 'node:child_process'
import { existsSync, rmSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { restoreBackup } from '../../../backup/index.js'
import { defineDriver } from '../../../core/driver/define.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import {
  addOrders,
  changeRecords,
  createOrders,
  expectRestoresTo,
  holdReadTransaction,
  newestChainId,
  onlyChainId,
  openCycleNode,
  truncateLogFromOutside,
} from './cycle-harness.js'
import {
  CHAIN_LIST_NAME,
  codeOf,
  createWorkspace,
  directoryDestination,
  failureOf,
  filesIn,
  storedChain,
  storedHeads,
  storedRecord,
  type Workspace,
  waitFor,
  walBytes,
  watchUnhandledRejections,
} from './harness.js'
import {
  CAPTURED_BEFORE_THE_KILL,
  KILLED_WRITER_ENV,
  KILLED_WRITER_READY,
  type KilledWriterMode,
  WRITTEN_BEFORE_THE_KILL,
} from './killed-writer.js'

const BUSY_TIMEOUT_MS = 20
const KILLED_WRITER_SCRIPT = join(dirname(fileURLToPath(import.meta.url)), 'killed-writer.ts')
const PACKAGE_ROOT = join(dirname(fileURLToPath(import.meta.url)), '..', '..', '..', '..')

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('cycle-faults')
})

afterEach(async () => {
  await workspace.dispose()
})

function stagedCaptures(stagingDir: string): string[] {
  return filesIn(stagingDir).filter(name => name.endsWith('.wal'))
}

describe('settings the backup cycle refuses', () => {
  it('refuses a database whose log it could never read, or a driver with no backup engine, before it creates any file', async () => {
    const destination = workspace.destination()
    const sirannon = workspace.registry()
    const base = betterSqlite3()
    const engineless = defineDriver({ capabilities: base.capabilities, open: base.open })

    const rollback = await failureOf(
      sirannon.open('rollback', workspace.path('rollback.db'), { walMode: false, backups: { destination } }),
    )
    const memory = await failureOf(sirannon.open('memory', ':memory:', { backups: { destination } }))
    const noEngine = await failureOf(
      workspace.registry(engineless).open('engineless', workspace.path('engineless.db'), { backups: { destination } }),
    )

    expect([codeOf(rollback), codeOf(memory), codeOf(noEngine)]).toEqual([
      'BACKUP_UNSUPPORTED',
      'BACKUP_UNSUPPORTED',
      'BACKUP_UNSUPPORTED',
    ])
    expect(rollback.message).toContain('write-ahead logging mode')
    expect(noEngine.message).toContain('no backup engine')
    expect(existsSync(workspace.path('rollback.db'))).toBe(false)
    expect(existsSync(workspace.path('engineless.db'))).toBe(false)
    expect([...sirannon.databases().keys()]).toEqual([])
    expect(destination.names()).toEqual([])
  })

  it('refuses a destination deadline no timer holds, leaves the file closed, and opens it with a good one', async () => {
    const destination = workspace.destination()
    const sirannon = workspace.registry()
    const path = workspace.path('deadline.db')

    for (const destinationTimeoutMs of [-1, Number.NaN, 2_147_483_648]) {
      const error = await failureOf(sirannon.open('deadline', path, { backups: { destination, destinationTimeoutMs } }))
      expect(codeOf(error), `${destinationTimeoutMs}`).toBe('BACKUP_ERROR')
      expect(sirannon.has('deadline')).toBe(false)
      expect(existsSync(`${path}-wal`), `${destinationTimeoutMs}`).toBe(false)
    }
    const db = await sirannon.open('deadline', path, {
      backups: { destination, intervalMs: 0, destinationTimeoutMs: 0 },
    })
    await createOrders(db)
    await addOrders(db, ['opened-at-last'])
    await db.close()

    await expectRestoresTo(workspace, destination, ['opened-at-last'], 'deadline')
  })
})

describe('a backup cycle whose destination refuses, hangs, or loses what it held', () => {
  it('sends a capture the destination refused on the next turn, at the same place in the chain', async () => {
    const node = await openCycleNode(workspace, 'refused')
    await createOrders(node.db)
    await node.db.captureBackupChanges()
    await addOrders(node.db, ['refused-once'])
    node.destination.faults.refuseWrite = name => name.endsWith('-000002.wal')

    const refused = await failureOf(node.db.captureBackupChanges())

    expect(codeOf(refused)).toBe('BACKUP_DESTINATION_ERROR')
    expect(refused.message).toContain('the bucket refused piece 0')
    expect(walBytes(node.path)).toBe(0)
    expect(stagedCaptures(node.stagingDir)).toHaveLength(1)
    expect(node.errors.map(codeOf)).toEqual(['BACKUP_DESTINATION_ERROR'])

    node.destination.faults.refuseWrite = undefined
    await addOrders(node.db, ['after-the-refusal'])
    await node.db.captureBackupChanges()

    const changes = changeRecords(storedChain(node.destination, onlyChainId(node.destination)))
    expect(changes.map(record => record.sequence)).toEqual([1, 2, 3])
    expect(stagedCaptures(node.stagingDir)).toEqual([])
    await expectRestoresTo(workspace, node.destination, ['refused-once', 'after-the-refusal'], 'refused')
  })

  it('stops a turn whose destination hangs once destinationTimeoutMs passes, and sends that capture later', async () => {
    const node = await openCycleNode(workspace, 'hung', { backups: { destinationTimeoutMs: 150 } })
    await createOrders(node.db)
    await node.db.captureBackupChanges()
    await addOrders(node.db, ['held-up'])
    node.destination.faults.holdWrite = name => (name.endsWith('.wal') ? new Promise<void>(() => {}) : undefined)

    const startedAt = Date.now()
    const hung = await failureOf(node.db.captureBackupChanges())

    expect(codeOf(hung)).toBe('BACKUP_DESTINATION_ERROR')
    expect(hung.message).toContain('within 150ms')
    expect(Date.now() - startedAt).toBeLessThan(5_000)
    node.destination.faults.holdWrite = undefined
    await node.db.captureBackupChanges()
    await expectRestoresTo(workspace, node.destination, ['held-up'], 'hung')

    const claiming = workspace.destination('claim-bucket')
    claiming.faults.holdWrite = name => (name === CHAIN_LIST_NAME ? new Promise<void>(() => {}) : undefined)
    const unclaimed = await openCycleNode(workspace, 'unclaimed', {
      destination: claiming,
      backups: { destinationTimeoutMs: 150 },
    })
    await createOrders(unclaimed.db)
    await addOrders(unclaimed.db, ['claimed-at-last'])
    await waitFor(() => unclaimed.errors.length > 0, 'the claim on the chain list to time out')
    expect(unclaimed.errors[0]?.message).toBe(
      `The destination did not claim piece 0 of '${CHAIN_LIST_NAME}' within 150ms, so the run stopped`,
    )
    expect(storedHeads(claiming)).toEqual([])
    claiming.faults.holdWrite = undefined
    await unclaimed.db.captureBackupChanges()
    await expectRestoresTo(workspace, claiming, ['claimed-at-last'], 'unclaimed')
  })

  it('reports BACKUP_LOG_REWOUND when another connection empties the log it was reading, and starts a fresh chain', async () => {
    const node = await openCycleNode(workspace, 'rewound', { driver: betterSqlite3({ busyTimeout: BUSY_TIMEOUT_MS }) })
    await createOrders(node.db)
    await addOrders(node.db, ['before-the-reader'])
    const reader = holdReadTransaction(node.path)
    await node.db.captureBackupChanges()
    reader.release()
    const broken = onlyChainId(node.destination)
    truncateLogFromOutside(node.path)
    await addOrders(node.db, ['after-the-outside-checkpoint'])

    const rewound = await failureOf(node.db.captureBackupChanges())

    expect(codeOf(rewound)).toBe('BACKUP_LOG_REWOUND')
    expect(rewound.message).toContain('Another connection checkpointed the log')
    expect(node.errors.map(codeOf)).toEqual(['BACKUP_LOG_REWOUND'])
    const heads = storedHeads(node.destination)
    expect(heads.map(head => head.chainId)[0]).toBe(broken)
    expect(heads).toHaveLength(2)
    expect(heads[1]?.previousChainId).toBe(broken)
    expect(node.db.backupStatus().lastError).toMatchObject({ code: 'BACKUP_LOG_REWOUND', chainId: broken })
    await expectRestoresTo(
      workspace,
      node.destination,
      ['before-the-reader', 'after-the-outside-checkpoint'],
      'rewound',
    )
  })

  it('drops a chain that the list no longer names, says which, and starts a fresh chain', async () => {
    for (const loss of ['overwritten', 'deleted'] as const) {
      const node = await openCycleNode(workspace, `lost-${loss}`, {
        destination: workspace.destination(`bucket-${loss}`),
      })
      await createOrders(node.db)
      await addOrders(node.db, ['before-the-loss'])
      await waitFor(() => node.runs.some(run => run.kind === 'full'), 'the full copy that starts the chain')
      const lost = onlyChainId(node.destination)
      if (loss === 'overwritten') {
        node.destination.replace(
          CHAIN_LIST_NAME,
          0,
          Buffer.from(JSON.stringify({ chainId: 'another-node', startedAt: 1 })),
        )
      } else {
        node.destination.remove(CHAIN_LIST_NAME, 0)
      }
      await addOrders(node.db, ['after-the-loss'])

      const replaced = await node.db.captureBackupChanges()

      expect(replaced?.kind, loss).toBe('full')
      expect(node.errors.map(codeOf), loss).toEqual(['BACKUP_CHAIN_BROKEN'])
      expect(node.errors[0]?.message, loss).toContain(lost)
      expect(
        storedHeads(node.destination).map(head => head.chainId),
        loss,
      ).not.toContain(lost)
      await expectRestoresTo(workspace, node.destination, ['before-the-loss', 'after-the-loss'], `lost-${loss}`)
    }
  })

  it('checks its chain with one read of its own place in the list, however many chains the list holds', async () => {
    const node = await openCycleNode(workspace, 'one-read')
    await createOrders(node.db)
    await node.db.captureBackupChanges()
    await node.db.close()
    for (const [index, chainId] of [
      [1, 'other-one'],
      [2, 'other-two'],
    ] as const) {
      node.destination.replace(CHAIN_LIST_NAME, index, Buffer.from(JSON.stringify({ chainId, startedAt: 1 })))
    }
    const listReads: number[] = []
    const listings: string[] = []
    const destination = directoryDestination(node.destination.root)
    destination.faults.refuseRead = (name, index) => {
      if (name === CHAIN_LIST_NAME) listReads.push(index)
      return false
    }
    destination.faults.holdList = name => {
      if (name === CHAIN_LIST_NAME) listings.push(name)
      return undefined
    }

    const reopened = await openCycleNode(workspace, 'one-read', { destination, stagingDir: node.stagingDir })
    await addOrders(reopened.db, ['after-reopening'])
    await reopened.db.captureBackupChanges()

    expect(listReads).toEqual([0])
    expect(listings).toEqual([])
    expect(reopened.errors).toEqual([])
  })

  it('appends nothing to a chain it could not check, and takes that chain up once the destination answers', async () => {
    const node = await openCycleNode(workspace, 'unchecked')
    await createOrders(node.db)
    await addOrders(node.db, ['before-the-outage'])
    await node.db.captureBackupChanges()
    await node.db.close()
    const chainId = onlyChainId(node.destination)
    const recordsBefore = storedChain(node.destination, chainId).length
    const destination = directoryDestination(node.destination.root)
    destination.faults.refuseRead = name => name === CHAIN_LIST_NAME
    destination.faults.refuseList = name => name === CHAIN_LIST_NAME

    const reopened = await openCycleNode(workspace, 'unchecked', { destination, stagingDir: node.stagingDir })
    await addOrders(reopened.db, ['during-the-outage'])
    const refused = await failureOf(reopened.db.captureBackupChanges())

    expect(codeOf(refused)).toBe('BACKUP_DESTINATION_ERROR')
    expect(refused.message).toContain('refused to list')
    expect(storedChain(destination, chainId)).toHaveLength(recordsBefore)
    destination.faults.refuseRead = undefined
    destination.faults.refuseList = undefined
    const resumed = await reopened.db.captureBackupChanges()

    expect(resumed?.chainId).toBe(chainId)
    expect(storedHeads(destination)).toHaveLength(1)
    await expectRestoresTo(workspace, destination, ['before-the-outage', 'during-the-outage'], 'unchecked')
  })

  it('reports staged frames that someone deleted as BACKUP_CHAIN_BROKEN and starts a fresh chain', async () => {
    const node = await openCycleNode(workspace, 'wiped')
    await createOrders(node.db)
    await node.db.captureBackupChanges()
    await addOrders(node.db, ['staged-then-wiped'])
    node.destination.faults.refuseWrite = name => name.endsWith('.wal')
    await failureOf(node.db.captureBackupChanges())
    node.destination.faults.refuseWrite = undefined
    for (const staged of stagedCaptures(node.stagingDir)) rmSync(join(node.stagingDir, staged))

    const broken = await failureOf(node.db.captureBackupChanges())

    expect(codeOf(broken)).toBe('BACKUP_CHAIN_BROKEN')
    expect(broken.message).toContain('are no longer in')
    expect(storedHeads(node.destination)).toHaveLength(2)
    await expectRestoresTo(workspace, node.destination, ['staged-then-wiped'], 'wiped')
  })

  it('refuses chain records that are missing fields, run backwards, or are not records at all', async () => {
    const node = await openCycleNode(workspace, 'corrupt')
    await createOrders(node.db)
    await addOrders(node.db, ['recorded'])
    await node.db.captureBackupChanges()
    const chainId = onlyChainId(node.destination)
    const logName = `${CHAIN_LIST_NAME}.${chainId}`
    const original = node.destination.stored(logName, 1)
    const change = storedRecord<Record<string, unknown>>(node.destination, logName, 1)
    const corruptions: [string, string, string][] = [
      ['missing fields', JSON.stringify({ kind: 'change', chainId, name: change.name }), 'missing fields'],
      [
        'frames running backwards',
        JSON.stringify({ ...change, position: { ...(change.position as object), firstFrame: 4, lastFrame: 2 } }),
        'missing fields',
      ],
      ['bytes that are no record', 'this is not JSON', 'is not a record Sirannon wrote'],
    ]

    for (const [label, bytes, message] of corruptions) {
      node.destination.replace(logName, 1, Buffer.from(bytes))
      const listed = await failureOf(node.db.backupChain())
      const restored = await failureOf(
        restoreBackup({
          destination: node.destination,
          driver: betterSqlite3(),
          destPath: workspace.path(`${label}.db`),
        }),
      )
      expect([codeOf(listed), codeOf(restored)], label).toEqual([
        'BACKUP_DESTINATION_ERROR',
        'BACKUP_DESTINATION_ERROR',
      ])
      expect(listed.message, label).toContain(message)
      expect(existsSync(workspace.path(`${label}.db`)), label).toBe(false)
    }
    node.destination.replace(logName, 1, original)
    expect((await node.db.backupChain())[0]?.changes).toHaveLength(1)
  })

  it('starts a fresh chain when the state it left on disk is damaged, and drops staged frames a new destination lacks', async () => {
    const node = await openCycleNode(workspace, 'state')
    await createOrders(node.db)
    await addOrders(node.db, ['first-life'])
    await node.db.captureBackupChanges()
    await node.db.close()
    writeFileSync(
      join(node.stagingDir, 'cycle.json'),
      JSON.stringify({ chainId: onlyChainId(node.destination), records: 2 }),
    )

    const damaged = await openCycleNode(workspace, 'state', {
      destination: node.destination,
      stagingDir: node.stagingDir,
    })
    await damaged.db.captureBackupChanges()
    const heads = storedHeads(node.destination)
    expect(heads).toHaveLength(2)
    expect(heads[1]?.previousChainId).toBeUndefined()

    await addOrders(damaged.db, ['staged-for-the-old-destination'])
    damaged.destination.faults.refuseWrite = name => name.endsWith('.wal')
    await failureOf(damaged.db.captureBackupChanges())
    await damaged.db.close()
    expect(stagedCaptures(node.stagingDir)).toHaveLength(1)

    const moved = await openCycleNode(workspace, 'state', {
      destination: workspace.destination('new-bucket'),
      stagingDir: node.stagingDir,
    })
    await moved.db.captureBackupChanges()

    expect(stagedCaptures(node.stagingDir)).toEqual([])
    expect(moved.errors.map(codeOf)).toEqual(['BACKUP_CHAIN_BROKEN'])
    await expectRestoresTo(workspace, moved.destination, ['first-life', 'staged-for-the-old-destination'], 'moved')
  })
})

describe('a backup cycle whose callers misbehave', () => {
  it('finishes and records every turn when its callbacks throw or reject', async () => {
    const watch = watchUnhandledRejections()
    try {
      for (const failure of ['throws', 'rejects'] as const) {
        const fail =
          failure === 'throws'
            ? (): never => {
                throw new Error('the operator log went away')
              }
            : async (): Promise<never> => {
                throw new Error('the operator log went away')
              }
        const node = await openCycleNode(workspace, `callbacks-${failure}`, {
          destination: workspace.destination(`bucket-${failure}`),
          backups: { onRun: fail, onProgress: fail, onError: fail, onSkip: fail },
        })
        await createOrders(node.db)
        await addOrders(node.db, ['reported-badly'])

        const report = await node.db.captureBackupChanges()

        expect(report?.kind, failure).toBe('change')
        expect(node.db.backupStatus().lastRun?.kind, failure).toBe('change')
        expect(node.db.backupStatus().lastError, failure).toBeUndefined()
        await node.db.close()
        await expectRestoresTo(workspace, node.destination, ['reported-badly'], `callbacks-${failure}`)
      }
      expect(watch.reasons).toEqual([])
    } finally {
      watch.stop()
    }
  })

  it('captures every write exactly once when callers ask for turns at the same moment', async () => {
    const node = await openCycleNode(workspace, 'crowded')
    await createOrders(node.db)
    const expected: string[] = []
    const turns: Promise<unknown>[] = []
    for (let round = 0; round < 6; round++) {
      const label = `crowd-${round}`
      expected.push(label)
      await addOrders(node.db, [label])
      turns.push(node.db.captureBackupChanges(), node.db.captureBackupChanges())
    }

    await Promise.all(turns)
    await node.db.captureBackupChanges()

    const changes = changeRecords(storedChain(node.destination, onlyChainId(node.destination)))
    expect(changes.map(record => record.sequence)).toEqual(changes.map((_, at) => at + 1))
    expect(node.errors).toEqual([])
    await expectRestoresTo(workspace, node.destination, expected, 'crowded')
  })

  it.each<KilledWriterMode>([
    'unsent-writes',
    'refused-capture',
  ])('picks up after the process is killed between turns (%s), whatever limit the log is given', mode => {
    const path = workspace.path('killed.db')
    const bucket = workspace.path('bucket')
    const stagingDir = workspace.path('killed-staging')
    const child = spawnSync(
      process.execPath,
      ['--import', 'tsx', KILLED_WRITER_SCRIPT, path, bucket, stagingDir, mode],
      {
        cwd: PACKAGE_ROOT,
        encoding: 'utf8',
        env: { ...process.env, [KILLED_WRITER_ENV]: '1' },
        timeout: 60_000,
      },
    )
    expect(child.stdout, child.stderr).toContain(KILLED_WRITER_READY)
    expect(child.signal).toBe('SIGKILL')
    expect(walBytes(path)).toBeGreaterThan(0)
    expect(stagedCaptures(stagingDir)).toHaveLength(mode === 'refused-capture' ? 1 : 0)

    return (async () => {
      const destination = directoryDestination(bucket)
      const node = await openCycleNode(workspace, 'killed', {
        destination,
        stagingDir,
        backups: { maxUncapturedLogBytes: 1 },
      })
      await node.db.captureBackupChanges()

      expect(node.errors).toEqual([])
      expect(node.runs.map(run => run.kind)).not.toContain('full')
      expect(stagedCaptures(stagingDir)).toEqual([])
      expect(newestChainId(destination)).toBe(onlyChainId(destination))
      await expectRestoresTo(
        workspace,
        destination,
        [CAPTURED_BEFORE_THE_KILL, WRITTEN_BEFORE_THE_KILL],
        `killed-${mode}`,
      )
    })()
  }, 90_000)
})

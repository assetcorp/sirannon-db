import { existsSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import { createBackupCycle } from '../../backup/cycle-factory.js'
import type { BackupCycleRequest } from '../../backup/cycle-options.js'
import type { BackupDestination } from '../../backup/destination.js'
import type { BackupGroupMembership, BackupSkip } from '../../backup/preferred-node.js'
import type { SQLiteConnection } from '../../driver/types.js'
import type { SirannonError } from '../../errors.js'
import { testDriver } from '../helpers/test-driver.js'
import { type MemoryDestination, memoryDestination } from './memory-destination.js'
import { settleUntil, tempDirPerTest } from './shared.js'

const temp = tempDirPerTest()
const openConnections: SQLiteConnection[] = []

afterEach(async () => {
  for (const conn of openConnections.splice(0)) {
    await conn.close()
  }
})

interface Group {
  membership: BackupGroupMembership
  fail: Error | null
  reads: number
}

interface Harness {
  build: (overrides?: Partial<BackupCycleRequest>) => ReturnType<typeof createBackupCycle>
  destination: MemoryDestination
  conn: SQLiteConnection
  logPath: string
  group: Group
  skips: BackupSkip[]
  errors: Error[]
}

function heldOnChangePieces(destination: BackupDestination, gate: Promise<void>): BackupDestination {
  return {
    ...destination,
    async writePiece(pieceName, index, bytes) {
      if (pieceName.endsWith('.wal')) await gate
      await destination.writePiece(pieceName, index, bytes)
    },
  }
}

function claiming(destination: BackupDestination): BackupDestination {
  return {
    ...destination,
    async writePieceIfAbsent(name, index, bytes) {
      const taken = await destination.listPieces(name)
      if (taken.some(piece => piece.index === index)) return false
      await destination.writePiece(name, index, bytes)
      return true
    },
  }
}

async function harness(nodeId = 'node-b', name = 'source', shared?: MemoryDestination): Promise<Harness> {
  const dbPath = join(temp.path, `${name}.db`)
  const conn = await testDriver.open(dbPath, { walMode: true, walAutoCheckpoint: 0 })
  openConnections.push(conn)
  await conn.exec('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')

  const destination = shared ?? memoryDestination()
  const manager = new BackupManager()
  const group: Group = {
    membership: { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] },
    fail: null,
    reads: 0,
  }
  const skips: BackupSkip[] = []
  const errors: Error[] = []

  const request: BackupCycleRequest = {
    destination: claiming(destination),
    intervalMs: 0,
    databaseId: 'main',
    sourcePath: dbPath,
    stagingDir: join(temp.path, `${name}-staging`),
    replicationGroup: {
      nodeId,
      readMembership: async () => {
        group.reads++
        if (group.fail) throw group.fail
        return group.membership
      },
    },
    onSkip: skip => skips.push(skip),
    onError: err => errors.push(err),
    runExclusive: op => op(),
    acquireWriter: () => conn,
    fullCopy: options => manager.copyToDestination(conn, { ...options, databaseId: 'main', sourcePath: dbPath }),
  }

  return {
    build: overrides => createBackupCycle({ ...request, ...overrides }),
    destination,
    conn,
    logPath: `${dbPath}-wal`,
    group,
    skips,
    errors,
  }
}

async function insert(conn: SQLiteConnection, body: string): Promise<void> {
  await conn.exec(`INSERT INTO notes (body) VALUES ('${body}')`)
}

describe('a backup cycle that every node of a group carries', () => {
  it('takes the backup on the node the group names', async () => {
    const { build } = await harness()
    const cycle = build()
    await cycle.start()

    expect((await cycle.chains())[0]?.base?.kind).toBe('full')
  })

  it('lets one node of the group take the turn while the other takes none', async () => {
    const replica = await harness('node-b', 'replica')
    const primary = await harness('node-a', 'primary', replica.destination)
    const takingIt = replica.build()
    const standingDown = primary.build()
    await takingIt.start()
    await standingDown.start()

    expect(await takingIt.chains()).toHaveLength(1)
    expect(await standingDown.chains()).toHaveLength(1)
    expect(replica.skips).toEqual([])
    expect(primary.skips[0]?.preferredNodeId).toBe('node-b')
  })

  it('writes nothing from a node the group names no backup on, and says whose turn it was', async () => {
    const { build, destination, skips } = await harness('node-a')
    const cycle = build()
    await cycle.start()

    expect(destination.names()).toEqual([])
    expect(skips[0]?.reason).toBe('not-preferred')
    expect(skips[0]?.preferredNodeId).toBe('node-b')
  })

  it('empties the log of a node that takes none of the backups', async () => {
    const { build, conn, logPath } = await harness('node-a')
    const cycle = build()
    await cycle.start()
    await insert(conn, 'first')
    expect(statSync(logPath).size).toBeGreaterThan(0)

    await cycle.runOnce()

    expect(statSync(logPath).size).toBe(0)
  })

  it('holds the frames of a node that could not reach its group, and reports the cause', async () => {
    const { build, conn, group, logPath, skips } = await harness()
    const cycle = build()
    await cycle.start()
    await insert(conn, 'first')
    group.fail = new Error('etcd deadline exceeded')

    expect(await cycle.runOnce()).toBeUndefined()
    expect(skips[0]?.reason).toBe('group-unavailable')
    expect(skips[0]?.message).toContain('etcd deadline exceeded')
    expect(statSync(logPath).size).toBeGreaterThan(0)
  })

  it('counts the log a skipping node is holding, so an operator sees it grow before any limit bites', async () => {
    const { build, conn, group, logPath, skips } = await harness()
    const cycle = build()
    await cycle.start()
    await insert(conn, 'first')
    group.fail = new Error('etcd deadline exceeded')

    await cycle.runOnce()

    expect(skips.at(-1)?.uncapturedLogBytes).toBe(statSync(logPath).size)
  })

  it('warns once where a group shares a destination that cannot claim a place in the list', async () => {
    const { build, destination, errors } = await harness()
    const cycle = build({ destination })

    await cycle.start()
    await cycle.runOnce()

    expect(errors.filter(err => err.message.includes('writePieceIfAbsent'))).toHaveLength(1)
  })

  it('warns about no such destination where it can claim a place', async () => {
    const { build, errors } = await harness()

    await build().start()

    expect(errors.filter(err => err.message.includes('writePieceIfAbsent'))).toEqual([])
  })

  it('starts a fresh chain with a full copy once a failover brings the backups back to it', async () => {
    const { build, conn, group } = await harness()
    const cycle = build()
    await cycle.start()
    await insert(conn, 'first')
    await cycle.runOnce()
    const first = (await cycle.chains())[0]

    group.membership = { primaryNodeId: 'node-b', nodeIds: ['node-a', 'node-b'] }
    await cycle.runOnce()
    expect(existsSync(join(temp.path, 'source-staging', 'cycle.json'))).toBe(false)

    group.membership = { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] }
    await cycle.runOnce()

    const chains = await cycle.chains()
    expect(chains).toHaveLength(2)
    expect(chains[0]?.chainId).not.toBe(first?.chainId)
    expect(chains[0]?.base?.kind).toBe('full')
  })

  it('sends the capture it already read off the log before it lets go of the chain', async () => {
    const { build, conn, destination, group } = await harness()
    const cycle = build()
    await cycle.start()
    const chainId = (await cycle.chains())[0]?.chainId
    await insert(conn, 'first')

    destination.refuseName(`sirannon-backup-${chainId}-000001.wal`)
    await cycle.runOnce().catch(() => {})
    destination.refuseName(null)
    group.membership = { primaryNodeId: 'node-b', nodeIds: ['node-a', 'node-b'] }
    await cycle.runOnce()

    const chain = (await cycle.chains()).find(candidate => candidate.chainId === chainId)
    expect(chain?.changes.map(piece => piece.sequence)).toEqual([1])
  })

  it('keeps a capture the destination refused while it stands down, and sends it once the destination answers', async () => {
    const { build, conn, destination, group, logPath, errors } = await harness()
    const cycle = build()
    await cycle.start()
    const chainId = (await cycle.chains())[0]?.chainId
    await insert(conn, 'first')
    destination.refuseName(`sirannon-backup-${chainId}-000001.wal`)
    await cycle.runOnce().catch(() => {})
    const staged = join(temp.path, 'source-staging', 'capture-1.wal')
    group.membership = { primaryNodeId: 'node-a', nodeIds: ['node-a'] }

    await cycle.runOnce()

    expect(existsSync(staged)).toBe(true)
    expect(errors.at(-1)?.message).toContain('refusing')

    destination.refuseName(null)
    await cycle.runOnce()

    expect(existsSync(staged)).toBe(false)
    expect(statSync(logPath).size).toBe(0)
    const chain = (await cycle.chains()).find(held => held.chainId === chainId)
    expect(chain?.changes.map(piece => piece.sequence)).toEqual([1])
  })

  it('offers a refused capture once in the turn that lets its chain go, rather than waiting on it twice', async () => {
    const { build, conn, destination, group } = await harness()
    let offers = 0
    const counting: BackupDestination = {
      ...destination,
      async writePiece(pieceName, index, bytes) {
        if (pieceName.endsWith('.wal')) offers++
        await destination.writePiece(pieceName, index, bytes)
      },
    }
    const cycle = build({ destination: counting, maxUncapturedLogBytes: 1 })
    await cycle.start()
    await insert(conn, 'first')
    destination.refuseName(`sirannon-backup-${(await cycle.chains())[0]?.chainId}-000001.wal`)
    await cycle.runOnce().catch(() => {})
    await insert(conn, 'second')
    group.membership = { primaryNodeId: 'node-a', nodeIds: ['node-a'] }
    offers = 0

    await cycle.runOnce()

    expect(offers).toBe(1)
  })

  it('asks its group nothing where the operator pinned the backups to one node', async () => {
    const { build, group } = await harness()
    const cycle = build({ preferredNode: { nodeId: 'node-b' } })
    group.fail = new Error('etcd is down')
    await cycle.start()

    expect((await cycle.chains())[0]?.base?.kind).toBe('full')
    expect(group.reads).toBe(0)
  })

  it('takes every turn where the database belongs to no group', async () => {
    const { build } = await harness('node-a')
    const cycle = build({ replicationGroup: undefined })
    await cycle.start()

    expect((await cycle.chains())[0]?.base?.kind).toBe('full')
  })

  it('empties a log that grew past the limit while it could not reach its group, and names the loss', async () => {
    const { build, conn, group, logPath, errors } = await harness()
    const cycle = build({ maxUncapturedLogBytes: 4096 })
    await cycle.start()
    group.fail = new Error('etcd deadline exceeded')
    await insert(conn, 'first')

    await cycle.runOnce()

    expect(statSync(logPath).size).toBe(0)
    expect(errors[0]?.message).toContain('in no backup')
    expect((errors[0] as SirannonError).code).toBe('BACKUP_CHAIN_BROKEN')
    expect(existsSync(join(temp.path, 'source-staging', 'cycle.json'))).toBe(false)
  })

  it('starts a fresh chain with a full copy once it reaches its group again', async () => {
    const { build, conn, group } = await harness()
    const cycle = build({ maxUncapturedLogBytes: 4096 })
    await cycle.start()
    const first = (await cycle.chains())[0]?.chainId
    group.fail = new Error('etcd deadline exceeded')
    await insert(conn, 'first')
    await cycle.runOnce()

    group.fail = null
    await cycle.runOnce()

    const chains = await cycle.chains()
    expect(chains).toHaveLength(2)
    expect(chains[0]?.chainId).not.toBe(first)
    expect(chains[0]?.base?.kind).toBe('full')
  })

  it('holds a log that captures normally, whatever limit the operator set', async () => {
    const { build, conn, errors } = await harness()
    const cycle = build({ maxUncapturedLogBytes: 1 })
    await cycle.start()
    await insert(conn, 'first')

    expect((await cycle.runOnce())?.kind).toBe('change')
    expect(errors).toEqual([])
    expect(await cycle.chains()).toHaveLength(1)
  })

  it('drops a scheduled turn that finds the previous one still running, and says why', async () => {
    const { build, conn, destination, skips } = await harness()
    let release = (): void => {}
    const gate = new Promise<void>(resolve => {
      release = resolve
    })
    const cycle = build({ destination: heldOnChangePieces(destination, gate), intervalMs: 5 })
    await cycle.start()
    await insert(conn, 'first')

    const running = cycle.runOnce()
    await settleUntil(() => skips.some(skip => skip.reason === 'previous-run-active'))
    release()
    await running
    await cycle.stop()

    expect(skips.some(skip => skip.reason === 'previous-run-active')).toBe(true)
  })
})

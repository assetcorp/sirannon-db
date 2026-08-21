import { describe, expect, it } from 'vitest'
import type { BackupChain, BackupChainBase, BackupChainChange } from '../../backup/chain.js'
import type { BackupDestination } from '../../backup/destination.js'
import { verifyBackupRecord } from '../../backup/verify.js'
import { type MemoryDestination, memoryDestination } from './memory-destination.js'

const HELLO = new TextEncoder().encode('hello')
const WORLD = new TextEncoder().encode('world')
const HELLO_DIGEST = '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'
const HELLO_WORLD_DIGEST = '936a185caaa266bb9cbe981e9e05cb78cd732b0b3280eb944412bb6f8f8f07af'

function fullCopy(overrides?: Partial<BackupChainBase>): BackupChainBase {
  return {
    kind: 'full',
    chainId: 'chain-a',
    name: 'orders-full.db',
    runId: 'run-1',
    finishedAt: 1000,
    pieceCount: 1,
    pieceBytes: 5,
    bytesWritten: 5,
    fingerprint: HELLO_DIGEST,
    ...overrides,
  }
}

function changePiece(overrides?: Partial<BackupChainChange>): BackupChainChange {
  return {
    kind: 'change',
    chainId: 'chain-a',
    name: 'orders-1.wal',
    runId: 'run-2',
    sequence: 1,
    position: { logSequence: 1, salt1: 7, salt2: 8, firstFrame: 1, lastFrame: 2 },
    capturedAt: 2000,
    frameCount: 2,
    pieceCount: 2,
    pieceBytes: 5,
    bytesWritten: 10,
    checkpointed: true,
    fingerprint: HELLO_WORLD_DIGEST,
    ...overrides,
  }
}

function chainOf(base: BackupChainBase, changes: BackupChainChange[] = []): BackupChain[] {
  return [{ chainId: base.chainId, startedAt: 900, base, changes }]
}

async function storedDestination(): Promise<MemoryDestination> {
  const destination = memoryDestination()
  await destination.writePiece('orders-full.db', 0, HELLO)
  await destination.writePiece('orders-1.wal', 0, HELLO)
  await destination.writePiece('orders-1.wal', 1, WORLD)
  return destination
}

describe('verifying one backup record against the destination it is stored at', () => {
  it('reports the pieces and bytes of a full copy whose stored bytes match what the run recorded', async () => {
    const destination = await storedDestination()

    const result = await verifyBackupRecord(destination, chainOf(fullCopy()), 'orders-full.db')

    expect(result).toEqual({
      name: 'orders-full.db',
      chainId: 'chain-a',
      kind: 'full',
      pieceCount: 1,
      bytesRead: 5,
      fingerprint: HELLO_DIGEST,
    })
  })

  it('reads every piece of a change piece back and reports what they came to', async () => {
    const destination = await storedDestination()

    const result = await verifyBackupRecord(destination, chainOf(fullCopy(), [changePiece()]), 'orders-1.wal')

    expect(result.kind).toBe('change')
    expect(result.pieceCount).toBe(2)
    expect(result.bytesRead).toBe(10)
    expect(result.fingerprint).toBe(HELLO_WORLD_DIGEST)
  })

  it('answers for a record the run fingerprinted no copy of', async () => {
    const destination = await storedDestination()
    const chains = chainOf(fullCopy({ fingerprint: undefined }))

    const result = await verifyBackupRecord(destination, chains, 'orders-full.db')

    expect(result.fingerprint).toBeUndefined()
    expect(result.bytesRead).toBe(5)
  })

  it('refuses a record whose stored bytes no longer match the fingerprint the run recorded', async () => {
    const destination = await storedDestination()
    await destination.writePiece('orders-full.db', 0, WORLD)

    await expect(verifyBackupRecord(destination, chainOf(fullCopy()), 'orders-full.db')).rejects.toMatchObject({
      code: 'BACKUP_DESTINATION_ERROR',
    })
  })

  it('refuses a record the destination is missing a piece of', async () => {
    const destination = memoryDestination()
    await destination.writePiece('orders-1.wal', 0, HELLO)
    const chains = chainOf(fullCopy(), [changePiece()])

    await expect(verifyBackupRecord(destination, chains, 'orders-1.wal')).rejects.toMatchObject({
      code: 'BACKUP_DESTINATION_ERROR',
    })
  })

  it('refuses a name no record in the chains states', async () => {
    const destination: BackupDestination = await storedDestination()

    await expect(verifyBackupRecord(destination, chainOf(fullCopy()), 'orders-9.wal')).rejects.toMatchObject({
      code: 'BACKUP_CHAIN_BROKEN',
    })
  })
})

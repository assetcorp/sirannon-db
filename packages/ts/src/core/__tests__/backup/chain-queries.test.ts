import { describe, expect, it } from 'vitest'
import type { BackupChain, BackupChainBase, BackupChainChange } from '../../backup/chain.js'
import { backupPiecesSafeToDelete, planBackupRestore } from '../../backup/chain-queries.js'
import type { SirannonError } from '../../errors.js'

function base(chainId: string, finishedAt: number): BackupChainBase {
  return {
    kind: 'full',
    chainId,
    name: `${chainId}-full.db`,
    runId: chainId,
    finishedAt,
    pieceCount: 1,
    pieceBytes: 16,
    bytesWritten: 16,
  }
}

function change(chainId: string, sequence: number, capturedAt: number): BackupChainChange {
  return {
    kind: 'change',
    chainId,
    name: `${chainId}-${sequence}.wal`,
    runId: `${chainId}-${sequence}`,
    sequence,
    position: { logSequence: 0, salt1: 1, salt2: 2, firstFrame: sequence, lastFrame: sequence },
    capturedAt,
    frameCount: 1,
    pieceCount: 1,
    pieceBytes: 16,
    bytesWritten: 16,
    checkpointed: true,
  }
}

function chain(chainId: string, startedAt: number, changes: BackupChainChange[]): BackupChain {
  return { chainId, startedAt, base: base(chainId, startedAt + 1), changes }
}

const older = chain('older', 100, [change('older', 1, 200), change('older', 2, 300)])
const newer = chain('newer', 400, [change('newer', 1, 500), change('newer', 2, 600)])

describe('planBackupRestore', () => {
  it('reads the newest full copy taken at or before the moment', () => {
    const plan = planBackupRestore([newer, older], 550)

    expect(plan.chainId).toBe('newer')
    expect(plan.changes.map(piece => piece.sequence)).toEqual([1])
    expect(plan.restoresTo).toBe(500)
  })

  it('falls back to an older chain for a moment before the newer full copy', () => {
    const plan = planBackupRestore([newer, older], 350)

    expect(plan.chainId).toBe('older')
    expect(plan.changes).toHaveLength(2)
    expect(plan.restoresTo).toBe(300)
  })

  it('restores the full copy alone where no piece was taken by then', () => {
    const plan = planBackupRestore([newer, older], 150)

    expect(plan.chainId).toBe('older')
    expect(plan.changes).toHaveLength(0)
    expect(plan.restoresTo).toBe(101)
  })

  it('refuses a moment no full copy reaches back to, and names the earliest one that does', () => {
    const error = (() => {
      try {
        planBackupRestore([newer, older], 50)
      } catch (err) {
        return err as SirannonError
      }
    })()

    expect(error?.code).toBe('BACKUP_CHAIN_BROKEN')
    expect(error?.message).toContain(new Date(101).toISOString())
  })

  it('refuses a chain with a missing piece and names the piece', () => {
    const gapped = chain('gapped', 100, [change('gapped', 1, 200), change('gapped', 3, 400)])

    const error = (() => {
      try {
        planBackupRestore([gapped], 500)
      } catch (err) {
        return err as SirannonError
      }
    })()

    expect(error?.code).toBe('BACKUP_CHAIN_BROKEN')
    expect(error?.message).toContain('piece 2')
  })
})

describe('backupPiecesSafeToDelete', () => {
  it('reports nothing while every chain can still serve a restore', () => {
    expect(backupPiecesSafeToDelete([newer, older])).toEqual([])
  })

  it('reports every piece of a chain whose full copy the destination no longer holds', () => {
    const orphaned: BackupChain = { chainId: 'orphan', startedAt: 10, changes: [change('orphan', 1, 20)] }

    expect(backupPiecesSafeToDelete([orphaned]).map(record => record.name)).toEqual(['orphan-1.wal'])
  })

  it('reports the pieces after a gap, because no restore reaches past it', () => {
    const gapped = chain('gapped', 100, [change('gapped', 1, 200), change('gapped', 3, 400)])

    expect(backupPiecesSafeToDelete([gapped]).map(record => record.name)).toEqual(['gapped-3.wal'])
  })

  it('reports a whole chain a newer full copy has replaced across the window kept', () => {
    const discarded = backupPiecesSafeToDelete([newer, older], { restorableFrom: 450 })

    expect(discarded.map(record => record.name)).toEqual(['older-full.db', 'older-1.wal', 'older-2.wal'])
  })

  it('keeps the older chain while the newer full copy is younger than the window kept', () => {
    expect(backupPiecesSafeToDelete([newer, older], { restorableFrom: 250 })).toEqual([])
  })
})

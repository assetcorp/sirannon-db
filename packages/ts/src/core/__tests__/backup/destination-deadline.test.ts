import { afterEach, describe, expect, it, vi } from 'vitest'
import type { BackupDestination } from '../../backup/destination.js'
import { DEFAULT_DESTINATION_TIMEOUT_MS, destinationWithDeadline } from '../../backup/destination-deadline.js'
import { memoryDestination } from './memory-destination.js'

const SILENT: BackupDestination = {
  writePiece: () => new Promise<void>(() => {}),
  readPiece: () => new Promise<Uint8Array>(() => {}),
  listPieces: () => new Promise<never[]>(() => {}),
}

afterEach(() => {
  vi.useRealTimers()
})

async function expectDeadline(call: Promise<unknown>, action: string): Promise<void> {
  const settled = expect(call).rejects.toMatchObject({
    code: 'BACKUP_DESTINATION_ERROR',
    message: `The destination did not ${action} within ${DEFAULT_DESTINATION_TIMEOUT_MS}ms, so the run stopped`,
  })
  await vi.advanceTimersByTimeAsync(DEFAULT_DESTINATION_TIMEOUT_MS)
  await settled
}

describe('destinationWithDeadline', () => {
  it('passes on what a destination that answers in time returned', async () => {
    const bounded = destinationWithDeadline(memoryDestination(), DEFAULT_DESTINATION_TIMEOUT_MS)

    await bounded.writePiece('copy.db', 0, new Uint8Array([1, 2, 3]))

    expect(await bounded.readPiece('copy.db', 0)).toEqual(new Uint8Array([1, 2, 3]))
    expect(await bounded.listPieces('copy.db')).toEqual([{ index: 0, byteLength: 3 }])
  })

  it('stops a run whose destination never answers a write', async () => {
    vi.useFakeTimers()
    const bounded = destinationWithDeadline(SILENT, DEFAULT_DESTINATION_TIMEOUT_MS)

    await expectDeadline(bounded.writePiece('copy.db', 3, new Uint8Array(1)), "store piece 3 of 'copy.db'")
  })

  it('stops a run whose destination never answers a read', async () => {
    vi.useFakeTimers()
    const bounded = destinationWithDeadline(SILENT, DEFAULT_DESTINATION_TIMEOUT_MS)

    await expectDeadline(bounded.readPiece('copy.db', 2), "return piece 2 of 'copy.db'")
  })

  it('stops a run whose destination never answers a listing', async () => {
    vi.useFakeTimers()
    const bounded = destinationWithDeadline(SILENT, DEFAULT_DESTINATION_TIMEOUT_MS)

    await expectDeadline(bounded.listPieces('copy.db'), "list the pieces of 'copy.db'")
  })

  it('carries the claim of a destination that has one, so a chain still keeps its place', async () => {
    const destination = memoryDestination()
    const claiming: BackupDestination = {
      ...destination,
      writePieceIfAbsent: async (name, index, bytes) => {
        await destination.writePiece(name, index, bytes)
        return true
      },
    }
    const bounded = destinationWithDeadline(claiming, DEFAULT_DESTINATION_TIMEOUT_MS)

    expect(await bounded.writePieceIfAbsent?.('copy.db', 0, new Uint8Array([7]))).toBe(true)
    expect(await bounded.readPiece('copy.db', 0)).toEqual(new Uint8Array([7]))
  })

  it('offers no claim of its own where the destination has none', () => {
    const bounded = destinationWithDeadline(memoryDestination(), DEFAULT_DESTINATION_TIMEOUT_MS)

    expect(bounded.writePieceIfAbsent).toBeUndefined()
  })

  it('stops a run whose destination never answers a claim', async () => {
    vi.useFakeTimers()
    const silentClaim: BackupDestination = { ...SILENT, writePieceIfAbsent: () => new Promise<boolean>(() => {}) }
    const bounded = destinationWithDeadline(silentClaim, DEFAULT_DESTINATION_TIMEOUT_MS)

    await expectDeadline(
      bounded.writePieceIfAbsent?.('copy.db', 3, new Uint8Array(1)) ?? Promise.resolve(),
      "claim piece 3 of 'copy.db'",
    )
  })

  it('leaves the calls unbounded where the deadline is zero', () => {
    const destination = memoryDestination()

    expect(destinationWithDeadline(destination, 0)).toBe(destination)
  })

  it.each([-1, Number.NaN, Number.POSITIVE_INFINITY])('refuses a deadline of %s', deadline => {
    expect(() => destinationWithDeadline(memoryDestination(), deadline)).toThrow(
      expect.objectContaining({
        code: 'BACKUP_ERROR',
        message: `The destination deadline must be a number of milliseconds that is zero or above, and it was ${deadline}`,
      }),
    )
  })
})

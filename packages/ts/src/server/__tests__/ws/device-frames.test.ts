import { describe, expect, it } from 'vitest'
import type { ChangeEvent } from '../../../core/types.js'
import type { WSSendOutcome } from '../../ws-connection.js'
import { DeviceFramePacker, wireChangeEvent } from '../../ws-device-frames.js'

function change(seq: number, body: string): ChangeEvent {
  return {
    type: 'insert',
    table: 'notes',
    row: { id: seq, body },
    seq: BigInt(seq),
    timestamp: 0,
    rowId: String(seq),
    txId: 'tx-1',
    origin: 'a'.repeat(32),
  }
}

function makePacker(targetBytes: number, outcome: WSSendOutcome = 'sent') {
  const frames: string[] = []
  const packer = new DeviceFramePacker(
    'sub-1',
    data => {
      frames.push(data)
      return outcome
    },
    targetBytes,
  )
  return { packer, frames }
}

function frameSeqs(frame: string): number[] {
  const parsed = JSON.parse(frame) as { type: string; id: string; events: { seq: string }[] }
  expect(parsed.type).toBe('changes')
  expect(parsed.id).toBe('sub-1')
  return parsed.events.map(event => Number(event.seq))
}

describe('DeviceFramePacker', () => {
  it('packs events into one frame until the byte target is reached', () => {
    const { packer, frames } = makePacker(100_000)

    for (let seq = 1; seq <= 4; seq++) {
      expect(packer.append(change(seq, 'short'))).toBe('queued')
    }
    expect(frames).toHaveLength(0)

    expect(packer.flush()).toBe('sent')
    expect(frames).toHaveLength(1)
    expect(frameSeqs(frames[0])).toEqual([1, 2, 3, 4])
  })

  it('starts a new frame instead of crossing the byte target', () => {
    const single = JSON.stringify(wireChangeEvent(change(1, 'x'.repeat(64)))).length
    const { packer, frames } = makePacker(single * 2)

    packer.append(change(1, 'x'.repeat(64)))
    packer.append(change(2, 'x'.repeat(64)))
    packer.append(change(3, 'x'.repeat(64)))
    packer.flush()

    expect(frames).toHaveLength(2)
    expect(frameSeqs(frames[0])).toEqual([1, 2])
    expect(frameSeqs(frames[1])).toEqual([3])
  })

  it('sends an event larger than the whole target alone rather than splitting it', () => {
    const { packer, frames } = makePacker(256)

    packer.append(change(1, 'small'))
    const outcome = packer.append(change(2, 'y'.repeat(2_000)))

    expect(outcome).toBe('sent')
    expect(frames).toHaveLength(2)
    expect(frameSeqs(frames[0])).toEqual([1])
    expect(frameSeqs(frames[1])).toEqual([2])
    expect(packer.pendingEvents).toBe(0)
  })

  it('propagates the send outcome of a flush it performs', () => {
    const { packer } = makePacker(64, 'buffered')

    expect(packer.append(change(1, 'z'.repeat(200)))).toBe('buffered')
  })

  it('clears pending events without sending them', () => {
    const { packer, frames } = makePacker(100_000)

    packer.append(change(1, 'held'))
    packer.clear()
    expect(packer.flush()).toBe('queued')
    expect(frames).toHaveLength(0)
  })
})

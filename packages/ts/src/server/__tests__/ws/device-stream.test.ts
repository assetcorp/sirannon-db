import { describe, expect, it } from 'vitest'
import type { ChangeEvent } from '../../../core/types.js'
import { DeviceChangeStream } from '../../ws-device-stream.js'

function change(seq: number, txId: string): ChangeEvent {
  return {
    type: 'insert',
    table: 'notes',
    row: { id: seq },
    seq: BigInt(seq),
    timestamp: 0,
    rowId: String(seq),
    txId,
    origin: 'a'.repeat(32),
  }
}

function makeStream(maxUnacknowledgedChanges: number) {
  const sent: ChangeEvent[] = []
  let overloads = 0
  const stream = new DeviceChangeStream({
    deviceId: 'b'.repeat(32),
    maxUnacknowledgedChanges,
    send: event => {
      sent.push(event)
      return 'sent'
    },
    onOverload: () => {
      overloads += 1
    },
  })
  return { stream, sent, overloadCount: () => overloads }
}

describe('DeviceChangeStream', () => {
  it('holds a transaction until it is complete and marks its last change', () => {
    const { stream, sent } = makeStream(1000)

    stream.receive(change(1, 'tx-1'))
    stream.receive(change(2, 'tx-1'))
    expect(sent).toHaveLength(0)

    stream.onBatchEnd(true)
    expect(sent).toHaveLength(2)
    expect(sent[0].txEnd).toBeUndefined()
    expect(sent[1].txEnd).toBe(true)
  })

  it('closes a transaction when the next one starts', () => {
    const { stream, sent } = makeStream(1000)

    stream.receive(change(1, 'tx-1'))
    stream.receive(change(2, 'tx-2'))

    expect(sent).toHaveLength(1)
    expect(sent[0].seq).toBe(1n)
    expect(sent[0].txEnd).toBe(true)
  })

  it('keeps an incomplete transaction buffered when the batch cut it', () => {
    const { stream, sent } = makeStream(1000)

    stream.receive(change(1, 'tx-1'))
    stream.onBatchEnd(false)
    expect(sent).toHaveLength(0)

    stream.receive(change(2, 'tx-1'))
    stream.onBatchEnd(true)
    expect(sent.map(event => event.seq)).toEqual([1n, 2n])
    expect(sent[1].txEnd).toBe(true)
  })

  it('ignores a live batch boundary while replaying history', () => {
    const { stream, sent } = makeStream(1000)
    stream.beginPriming()

    stream.receive(change(1, 'tx-1'))
    stream.onBatchEnd(true)
    expect(sent).toHaveLength(0)

    stream.receive(change(2, 'tx-1'))
    stream.endPriming()

    expect(sent.map(event => event.seq)).toEqual([1n, 2n])
    expect(sent[1].txEnd).toBe(true)
  })

  it('stops delivering past the window and resumes on an acknowledgement', () => {
    const { stream, sent } = makeStream(2)

    stream.receive(change(1, 'tx-1'))
    stream.receive(change(2, 'tx-1'))
    stream.receive(change(3, 'tx-1'))
    stream.onBatchEnd(true)
    expect(sent).toHaveLength(3)

    stream.receive(change(4, 'tx-2'))
    stream.onBatchEnd(true)
    expect(sent).toHaveLength(3)

    stream.onAck(3n)
    expect(sent).toHaveLength(4)
    expect(sent[3].seq).toBe(4n)
  })

  it('fails loud when held work outgrows the window', () => {
    const stream = makeStream(2)

    stream.stream.receive(change(1, 'tx-1'))
    stream.stream.receive(change(2, 'tx-1'))
    stream.stream.receive(change(3, 'tx-1'))
    stream.stream.onBatchEnd(true)

    for (const seq of [4, 5, 6]) {
      stream.stream.receive(change(seq, `tx-${seq}`))
      stream.stream.onBatchEnd(true)
    }

    expect(stream.overloadCount()).toBe(1)
    expect(stream.stream.stopped).toBe(true)
  })
})

import { describe, expect, it } from 'vitest'
import type { ChangeEvent } from '../../../core/types.js'
import type { WSSendOutcome } from '../../ws-connection.js'
import { DeviceFramePacker } from '../../ws-device-frames.js'
import type { DeviceStreamPacing } from '../../ws-device-stream.js'
import { DeviceChangeStream } from '../../ws-device-stream.js'

const DEVICE_ID = 'b'.repeat(32)
const OTHER_ORIGIN = 'a'.repeat(32)

function change(seq: number, txId: string, origin: string = OTHER_ORIGIN): ChangeEvent {
  return {
    type: 'insert',
    table: 'notes',
    row: { id: seq },
    seq: BigInt(seq),
    timestamp: 0,
    rowId: String(seq),
    txId,
    origin,
  }
}

async function settle(): Promise<void> {
  for (let i = 0; i < 5; i++) {
    await new Promise(resolve => setTimeout(resolve, 0))
  }
}

interface HarnessOptions {
  max: number
  pacing?: DeviceStreamPacing
  baseline?: bigint
  startMode?: 'live' | 'catchup'
  packed?: boolean
  sendOutcomes?: WSSendOutcome[]
  congestedAbove?: number
}

function makeHarness(options: HarnessOptions) {
  const log: ChangeEvent[] = []
  let cursor = 0n
  let atBoundary = true
  const sent: ChangeEvent[] = []
  const frames: string[] = []
  const reads: Array<[bigint, bigint]> = []
  let overloads = 0
  let faults = 0
  const outcomes = options.sendOutcomes ?? []

  let buffered = 0
  let flushes = 0
  const congestedAbove = options.congestedAbove ?? 0

  const nextOutcome = (): WSSendOutcome => {
    const outcome = outcomes.shift() ?? 'sent'
    if (outcome === 'buffered') buffered += 1
    return outcome
  }

  const sendText = (data: string): WSSendOutcome => {
    const outcome = nextOutcome()
    if (outcome === 'dropped') return outcome
    frames.push(data)
    const parsed = JSON.parse(data) as { events: { seq: string; txEnd?: boolean; txId?: string }[] }
    for (const wire of parsed.events) {
      sent.push({ ...change(Number(wire.seq), wire.txId ?? ''), txEnd: wire.txEnd })
    }
    return outcome
  }

  const stream = new DeviceChangeStream(
    {
      deviceId: DEVICE_ID,
      maxUnacknowledgedChanges: options.max,
      pacing: options.pacing ?? 'perEvent',
      packer: options.packed === true ? new DeviceFramePacker('sub-1', sendText) : null,
      sendEvent: event => {
        const outcome = nextOutcome()
        if (outcome === 'dropped') return outcome
        sent.push(event)
        return outcome
      },
      socketBuffered: () => buffered,
      socketCongested: () => buffered > congestedAbove,
      flushSocket: () => {
        flushes += 1
        buffered = 0
      },
      onOverload: () => {
        overloads += 1
      },
      onFault: () => {
        faults += 1
      },
      readLog: async (afterSeq, upToSeq, limit) => {
        reads.push([afterSeq, upToSeq])
        return log.filter(event => event.seq > afterSeq && event.seq <= upToSeq).slice(0, limit)
      },
      logCursor: () => cursor,
      logCursorAtTxBoundary: () => atBoundary,
      transform: event => (event.origin === DEVICE_ID || event.origin === undefined ? null : event),
    },
    options.baseline ?? 0n,
    options.startMode ?? 'live',
  )

  const dispatch = (events: ChangeEvent[], boundary: boolean): void => {
    for (const event of events) {
      log.push(event)
      cursor = event.seq
    }
    atBoundary = boundary
    for (const event of events) {
      if (event.origin === DEVICE_ID || event.origin === undefined) continue
      stream.receiveLive(event)
    }
    stream.onBatchEnd(boundary)
  }

  return {
    stream,
    sent,
    frames,
    reads,
    dispatch,
    emptySocket: () => {
      buffered = 0
    },
    holdSocket: (bytes: number) => {
      buffered = bytes
    },
    flushCount: () => flushes,
    overloadCount: () => overloads,
    faultCount: () => faults,
    seqs: () => sent.map(event => Number(event.seq)),
  }
}

describe('DeviceChangeStream', () => {
  it('streams a transaction as its events arrive instead of buffering it', () => {
    const h = makeHarness({ max: 1000 })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-1'), change(3, 'tx-1')], false)

    expect(h.seqs()).toEqual([1, 2])
    expect(h.sent.every(event => event.txEnd !== true)).toBe(true)
  })

  it('marks the last change of a transaction at a boundary batch end', () => {
    const h = makeHarness({ max: 1000 })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-1')], true)

    expect(h.seqs()).toEqual([1, 2])
    expect(h.sent[0].txEnd).toBeUndefined()
    expect(h.sent[1].txEnd).toBe(true)
  })

  it('closes a transaction when the next one starts', () => {
    const h = makeHarness({ max: 1000 })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-2')], false)

    expect(h.seqs()).toEqual([1])
    expect(h.sent[0].txEnd).toBe(true)
  })

  it('keeps holding the last change across a non-boundary batch end', () => {
    const h = makeHarness({ max: 1000 })

    h.dispatch([change(1, 'tx-1')], false)
    expect(h.seqs()).toEqual([])

    h.dispatch([change(2, 'tx-1')], true)
    expect(h.seqs()).toEqual([1, 2])
    expect(h.sent[1].txEnd).toBe(true)
  })

  it('pauses mid-transaction at the window and resumes from the log on an acknowledgement', async () => {
    const h = makeHarness({ max: 2, pacing: 'perEvent' })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-1'), change(3, 'tx-1'), change(4, 'tx-1'), change(5, 'tx-1')], true)
    expect(h.seqs()).toEqual([1, 2, 3])
    expect(h.stream.catchingUp).toBe(true)

    h.stream.onAck(3n)
    await settle()

    expect(h.seqs()).toEqual([1, 2, 3, 4, 5])
    expect(h.sent[4].txEnd).toBe(true)
    expect(h.sent.slice(0, 4).every(event => event.txEnd !== true)).toBe(true)
    expect(h.stream.catchingUp).toBe(false)
  })

  it('delivers a transaction larger than the window whole under per-transaction pacing', () => {
    const h = makeHarness({ max: 2, pacing: 'perTransaction' })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-1'), change(3, 'tx-1'), change(4, 'tx-1'), change(5, 'tx-1')], true)

    expect(h.seqs()).toEqual([1, 2, 3, 4, 5])
    expect(h.sent[4].txEnd).toBe(true)
  })

  it('holds the next transaction at a closed window under per-transaction pacing', async () => {
    const h = makeHarness({ max: 2, pacing: 'perTransaction' })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-1'), change(3, 'tx-1'), change(4, 'tx-1')], true)
    expect(h.seqs()).toEqual([1, 2, 3, 4])

    h.dispatch([change(5, 'tx-2')], true)
    expect(h.seqs()).toEqual([1, 2, 3, 4])
    expect(h.stream.catchingUp).toBe(true)

    h.stream.onAck(4n)
    await settle()
    expect(h.seqs()).toEqual([1, 2, 3, 4, 5])
    expect(h.sent[4].txEnd).toBe(true)
  })

  it('fails loud when the socket drops a frame', () => {
    const h = makeHarness({ max: 1000, sendOutcomes: ['dropped'] })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-2')], true)

    expect(h.overloadCount()).toBe(1)
    expect(h.stream.stopped).toBe(true)
  })

  it('pauses on socket backpressure and resumes from the log on drain', async () => {
    const h = makeHarness({ max: 1000, sendOutcomes: ['buffered'] })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-2')], true)
    expect(h.seqs()).toEqual([1])
    expect(h.stream.catchingUp).toBe(true)

    h.stream.onSocketDrain()
    await settle()

    expect(h.seqs()).toEqual([1, 2])
    expect(h.sent[1].txEnd).toBe(true)
    expect(h.stream.catchingUp).toBe(false)
  })

  it('starts in catch-up from a resume position and replays the retained log', async () => {
    const h = makeHarness({ max: 1000, baseline: 2n, startMode: 'catchup' })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-1'), change(3, 'tx-2'), change(4, 'tx-2'), change(5, 'tx-3')], true)
    h.sent.length = 0

    h.stream.start()
    await settle()

    expect(h.seqs()).toEqual([3, 4, 5])
    expect(h.sent[1].txEnd).toBe(true)
    expect(h.sent[2].txEnd).toBe(true)
  })

  it("suppresses echoes of the device's own writes during catch-up", async () => {
    const h = makeHarness({ max: 1000, baseline: 0n, startMode: 'catchup' })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-2', DEVICE_ID), change(3, 'tx-3')], true)
    h.sent.length = 0

    h.stream.start()
    await settle()

    expect(h.seqs()).toEqual([1, 3])
  })

  it('packs several events into one frame under the byte target', () => {
    const h = makeHarness({ max: 1000, packed: true })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-1'), change(3, 'tx-1'), change(4, 'tx-1')], true)

    expect(h.seqs()).toEqual([1, 2, 3, 4])
    expect(h.sent[3].txEnd).toBe(true)
    expect(h.frames.length).toBe(1)
  })

  it('finishes a transaction wider than the window after mid-transaction backpressure under per-transaction pacing', async () => {
    const h = makeHarness({ max: 2, pacing: 'perTransaction', sendOutcomes: ['sent', 'sent', 'buffered'] })

    h.dispatch(
      [
        change(1, 'tx-1'),
        change(2, 'tx-1'),
        change(3, 'tx-1'),
        change(4, 'tx-1'),
        change(5, 'tx-1'),
        change(6, 'tx-1'),
      ],
      true,
    )
    expect(h.seqs()).toEqual([1, 2, 3])
    expect(h.stream.catchingUp).toBe(true)

    h.stream.onSocketDrain()
    await settle()

    expect(h.seqs()).toEqual([1, 2, 3, 4, 5, 6])
    expect(h.sent[5].txEnd).toBe(true)
    expect(h.sent.slice(0, 5).every(event => event.txEnd !== true)).toBe(true)
    expect(h.stream.catchingUp).toBe(false)
  })

  it('reads a long suppressed span once across window-gate pauses', async () => {
    const events: ChangeEvent[] = [change(1, 'tx-a'), change(2, 'tx-a'), change(3, 'tx-a'), change(4, 'tx-a')]
    for (let seq = 5; seq <= 1004; seq++) {
      events.push(change(seq, `echo-${seq}`, DEVICE_ID))
    }
    events.push(change(1005, 'tx-b'))
    events.push(change(1006, 'tx-c'))

    const h = makeHarness({ max: 3, baseline: 0n, startMode: 'catchup' })
    h.dispatch(events, true)

    h.stream.start()
    await settle()
    expect(h.seqs()).toEqual([1, 2, 3, 4])
    expect(h.stream.catchingUp).toBe(true)

    h.stream.onAck(4n)
    await settle()
    expect(h.seqs()).toEqual([1, 2, 3, 4, 1005])

    h.stream.onAck(1005n)
    await settle()

    expect(h.seqs()).toEqual([1, 2, 3, 4, 1005, 1006])
    expect(h.stream.catchingUp).toBe(false)
    expect(h.reads.filter(([afterSeq]) => afterSeq < 1000n).length).toBe(1)
  })

  it('survives repeated backpressure pauses without loss or duplication', async () => {
    const h = makeHarness({ max: 1000, sendOutcomes: ['buffered', 'buffered'] })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-2'), change(3, 'tx-3')], true)
    expect(h.seqs()).toEqual([1])

    h.stream.onSocketDrain()
    await settle()
    expect(h.seqs()).toEqual([1, 2])
    expect(h.stream.catchingUp).toBe(true)

    h.stream.onSocketDrain()
    await settle()

    expect(h.seqs()).toEqual([1, 2, 3])
    expect(h.sent.every(event => event.txEnd === true)).toBe(true)
    expect(h.stream.catchingUp).toBe(false)
    expect(h.overloadCount()).toBe(0)
  })

  it('resumes a stream whose socket emptied without a drain notification', async () => {
    const h = makeHarness({ max: 1000, sendOutcomes: ['buffered'] })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-2'), change(3, 'tx-3')], true)
    expect(h.seqs()).toEqual([1])
    expect(h.stream.catchingUp).toBe(true)

    h.emptySocket()
    h.stream.onBatchEnd(true)
    await settle()

    expect(h.seqs()).toEqual([1, 2, 3])
    expect(h.stream.catchingUp).toBe(false)
    expect(h.overloadCount()).toBe(0)
  })

  it('flushes a socket that stopped writing with a remainder still queued', () => {
    const h = makeHarness({ max: 1000, congestedAbove: 1_000 })

    h.dispatch([change(1, 'tx-1')], true)
    h.holdSocket(400)

    h.stream.onBatchEnd(true)
    expect(h.flushCount()).toBe(0)

    h.stream.onBatchEnd(true)
    expect(h.flushCount()).toBe(1)
  })

  it('leaves a congested socket to its own flow control', () => {
    const h = makeHarness({ max: 1000, congestedAbove: 100 })

    h.dispatch([change(1, 'tx-1')], true)
    h.holdSocket(400)

    h.stream.onBatchEnd(true)
    h.stream.onBatchEnd(true)

    expect(h.flushCount()).toBe(0)
  })

  it('keeps reading when a send reports backpressure the socket no longer holds', async () => {
    const h = makeHarness({ max: 1000, startMode: 'catchup', sendOutcomes: ['sent', 'sent', 'sent'] })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-1'), change(3, 'tx-1')], true)
    h.stream.start()
    await settle()

    expect(h.seqs()).toEqual([1, 2, 3])
    expect(h.stream.catchingUp).toBe(false)
  })

  it('re-enters catch-up when the send at the live transition hits backpressure', async () => {
    const h = makeHarness({ max: 1000, baseline: 0n, startMode: 'catchup', sendOutcomes: ['buffered'] })

    h.dispatch([change(1, 'tx-1')], true)
    h.stream.start()
    await settle()

    expect(h.seqs()).toEqual([1])
    expect(h.stream.catchingUp).toBe(true)

    h.stream.onSocketDrain()
    await settle()

    expect(h.seqs()).toEqual([1])
    expect(h.sent[0].txEnd).toBe(true)
    expect(h.stream.catchingUp).toBe(false)
  })

  it('re-enters catch-up when the batch-end frame flush hits backpressure', async () => {
    const h = makeHarness({ max: 1000, packed: true, sendOutcomes: ['buffered'] })

    h.dispatch([change(1, 'tx-1'), change(2, 'tx-1')], true)

    expect(h.frames.length).toBe(1)
    expect(h.seqs()).toEqual([1, 2])
    expect(h.stream.catchingUp).toBe(true)

    h.stream.onSocketDrain()
    await settle()

    expect(h.seqs()).toEqual([1, 2])
    expect(h.stream.catchingUp).toBe(false)
    expect(h.overloadCount()).toBe(0)
  })

  it('ignores a log read that resolves after the stream stops', async () => {
    let releaseRead: ((events: ChangeEvent[]) => void) | undefined
    const sent: ChangeEvent[] = []
    let faults = 0
    const stream = new DeviceChangeStream(
      {
        deviceId: DEVICE_ID,
        maxUnacknowledgedChanges: 1000,
        pacing: 'perEvent',
        packer: null,
        sendEvent: event => {
          sent.push(event)
          return 'sent'
        },
        socketBuffered: () => 0,
        socketCongested: () => false,
        flushSocket: () => {},
        onOverload: () => {},
        onFault: () => {
          faults += 1
        },
        readLog: () =>
          new Promise<ChangeEvent[]>(resolve => {
            releaseRead = resolve
          }),
        logCursor: () => 3n,
        logCursorAtTxBoundary: () => true,
        transform: event => event,
      },
      0n,
      'catchup',
    )

    stream.start()
    await settle()
    expect(releaseRead).toBeDefined()

    stream.stop()
    releaseRead?.([change(1, 'tx-1'), change(2, 'tx-1'), change(3, 'tx-1')])
    await settle()

    expect(sent).toEqual([])
    expect(faults).toBe(0)
    expect(stream.stopped).toBe(true)
  })

  it('applies an acknowledgement that arrives during an awaited log read', async () => {
    const reads: bigint[] = []
    let releaseRead: ((events: ChangeEvent[]) => void) | undefined
    let pendingReads = 0
    const sent: ChangeEvent[] = []
    const stream = new DeviceChangeStream(
      {
        deviceId: DEVICE_ID,
        maxUnacknowledgedChanges: 2,
        pacing: 'perEvent',
        packer: null,
        sendEvent: event => {
          sent.push(event)
          return 'sent'
        },
        socketBuffered: () => 0,
        socketCongested: () => false,
        flushSocket: () => {},
        onOverload: () => {},
        onFault: () => {},
        readLog: (afterSeq: bigint) => {
          reads.push(afterSeq)
          pendingReads += 1
          expect(pendingReads).toBe(1)
          return new Promise<ChangeEvent[]>(resolve => {
            releaseRead = events => {
              pendingReads -= 1
              resolve(events)
            }
          })
        },
        logCursor: () => 5n,
        logCursorAtTxBoundary: () => true,
        transform: event => event,
      },
      0n,
      'catchup',
    )

    stream.start()
    await settle()

    stream.onAck(3n)
    releaseRead?.([change(1, 'tx-1'), change(2, 'tx-2'), change(3, 'tx-3'), change(4, 'tx-4'), change(5, 'tx-5')])
    await settle()

    expect(sent.map(event => Number(event.seq))).toEqual([1, 2, 3, 4, 5])
    expect(stream.catchingUp).toBe(false)
    expect(reads.length).toBeLessThanOrEqual(2)
  })

  it('reports a fault and stops when the log read fails', async () => {
    let faulted = 0
    const failing = new DeviceChangeStream(
      {
        deviceId: DEVICE_ID,
        maxUnacknowledgedChanges: 1000,
        pacing: 'perEvent',
        packer: null,
        sendEvent: () => 'sent',
        socketBuffered: () => 0,
        socketCongested: () => false,
        flushSocket: () => {},
        onOverload: () => {},
        onFault: () => {
          faulted += 1
        },
        readLog: async () => {
          throw new Error('log unavailable')
        },
        logCursor: () => 5n,
        logCursorAtTxBoundary: () => true,
        transform: event => event,
      },
      0n,
      'catchup',
    )

    failing.start()
    await settle()

    expect(faulted).toBe(1)
    expect(failing.stopped).toBe(true)
  })
})

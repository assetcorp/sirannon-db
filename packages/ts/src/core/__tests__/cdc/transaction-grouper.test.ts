import { describe, expect, it } from 'vitest'
import { TransactionGrouper } from '../../cdc/transaction-grouper.js'
import type { ChangeEvent } from '../../types.js'

function event(seq: number, txId?: string): ChangeEvent {
  return {
    type: 'insert',
    table: 'orders',
    row: { id: seq },
    seq: BigInt(seq),
    timestamp: 1,
    ...(txId === undefined ? {} : { txId }),
  }
}

function recorder(): { delivered: ChangeEvent[]; deliver: (event: ChangeEvent) => boolean } {
  const delivered: ChangeEvent[] = []
  return {
    delivered,
    deliver: (received: ChangeEvent) => {
      delivered.push(received)
      return true
    },
  }
}

describe('TransactionGrouper', () => {
  it('marks only the last event of a transaction', () => {
    const { delivered, deliver } = recorder()
    const grouper = new TransactionGrouper(deliver)

    grouper.receive(event(1, 'tx-a'))
    grouper.receive(event(2, 'tx-a'))
    grouper.receive(event(3, 'tx-a'))
    grouper.flush(true)

    expect(delivered.map(e => e.txEnd)).toEqual([undefined, undefined, true])
    expect(delivered.map(e => e.seq)).toEqual([1n, 2n, 3n])
  })

  it('closes a transaction as soon as the next one starts', () => {
    const { delivered, deliver } = recorder()
    const grouper = new TransactionGrouper(deliver)

    grouper.receive(event(1, 'tx-a'))
    grouper.receive(event(2, 'tx-b'))

    expect(delivered).toHaveLength(1)
    expect(delivered[0].seq).toBe(1n)
    expect(delivered[0].txEnd).toBe(true)
  })

  it('holds at most one event, whatever the transaction size', () => {
    const { delivered, deliver } = recorder()
    const grouper = new TransactionGrouper(deliver)

    for (let seq = 1; seq <= 500; seq++) {
      grouper.receive(event(seq, 'tx-a'))
      expect(delivered).toHaveLength(seq - 1)
    }
    grouper.flush(true)
    expect(delivered).toHaveLength(500)
  })

  it('keeps holding when the batch ended part-way through a transaction', () => {
    const { delivered, deliver } = recorder()
    const grouper = new TransactionGrouper(deliver)

    grouper.receive(event(1, 'tx-a'))
    grouper.receive(event(2, 'tx-a'))
    grouper.flush(false)
    expect(delivered.map(e => e.seq)).toEqual([1n])

    grouper.receive(event(3, 'tx-a'))
    grouper.flush(true)
    expect(delivered.map(e => e.txEnd)).toEqual([undefined, undefined, true])
  })

  it('marks the last surviving event of a transaction whose remaining events were all filtered out', () => {
    const { delivered, deliver } = recorder()
    const grouper = new TransactionGrouper(deliver)

    grouper.receive(event(1, 'tx-a'))
    grouper.receive(event(2, 'tx-a'))
    grouper.flush(false)
    grouper.flush(false)
    grouper.flush(true)

    expect(delivered.map(e => [e.seq, e.txEnd])).toEqual([
      [1n, undefined],
      [2n, true],
    ])
  })

  it('releases an event carrying no transaction id immediately', () => {
    const { delivered, deliver } = recorder()
    const grouper = new TransactionGrouper(deliver)

    grouper.receive(event(1))
    expect(delivered).toHaveLength(1)
    expect(delivered[0].txEnd).toBe(true)
  })

  it('closes the open transaction before an event carrying no transaction id', () => {
    const { delivered, deliver } = recorder()
    const grouper = new TransactionGrouper(deliver)

    grouper.receive(event(1, 'tx-a'))
    grouper.receive(event(2))

    expect(delivered.map(e => [e.seq, e.txEnd])).toEqual([
      [1n, true],
      [2n, true],
    ])
  })

  it('copies the event it marks rather than mutating the one every subscriber holds', () => {
    const { delivered, deliver } = recorder()
    const grouper = new TransactionGrouper(deliver)
    const single = event(1, 'tx-a')

    grouper.receive(single)
    grouper.flush(true)

    expect(delivered[0]).not.toBe(single)
    expect(delivered[0].txEnd).toBe(true)
    expect(single.txEnd).toBeUndefined()
  })

  it('stops delivering once a send is refused', () => {
    const delivered: ChangeEvent[] = []
    const grouper = new TransactionGrouper(received => {
      delivered.push(received)
      return false
    })

    expect(grouper.receive(event(1))).toBe(false)
    expect(grouper.receive(event(2))).toBe(false)
    expect(grouper.flush(true)).toBe(false)
    expect(delivered).toHaveLength(1)
  })
})

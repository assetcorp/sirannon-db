import { describe, expect, it } from 'vitest'
import { LWWResolver } from '../../sync/conflict/lww.js'
import { HLC } from '../../sync/hlc.js'
import type { ReplicationChange } from '../../sync/types.js'

const LOCAL_NODE = 'a'.repeat(32)
const REMOTE_NODE = 'b'.repeat(32)

function change(operation: ReplicationChange['operation'], hlc: string, nodeId: string): ReplicationChange {
  return {
    table: 'notes',
    operation,
    rowId: '1',
    primaryKey: { id: 1 },
    hlc,
    txId: 'tx',
    nodeId,
    newData: operation === 'delete' ? null : { id: 1, body: 'remote' },
    oldData: operation === 'delete' ? { id: 1, body: 'gone' } : null,
  }
}

describe('LWW resolver', () => {
  it('accepts a delete even when the local row was written later', () => {
    const olderDelete = HLC.encode(1_000, 0, REMOTE_NODE)
    const newerLocal = HLC.encode(5_000, 0, LOCAL_NODE)

    const resolution = new LWWResolver().resolve({
      table: 'notes',
      rowId: '1',
      localChange: change('update', newerLocal, LOCAL_NODE),
      remoteChange: change('delete', olderDelete, REMOTE_NODE),
      localHlc: newerLocal,
      remoteHlc: olderDelete,
    })

    expect(resolution.action).toBe('accept_remote')
  })

  it('keeps a newer local row against an older remote update', () => {
    const olderRemote = HLC.encode(1_000, 0, REMOTE_NODE)
    const newerLocal = HLC.encode(5_000, 0, LOCAL_NODE)

    const resolution = new LWWResolver().resolve({
      table: 'notes',
      rowId: '1',
      localChange: change('update', newerLocal, LOCAL_NODE),
      remoteChange: change('update', olderRemote, REMOTE_NODE),
      localHlc: newerLocal,
      remoteHlc: olderRemote,
    })

    expect(resolution.action).toBe('keep_local')
  })
})

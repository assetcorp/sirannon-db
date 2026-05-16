import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import {
  attachDiagnostics,
  compareDatabases,
  createMtlsCerts,
  createPrimary,
  createReplica,
  type DiagnosticsHandle,
  type ManagedNode,
  type MtlsCerts,
  stopNode,
  waitForReady,
  waitForReplica,
} from './lib/index.js'

const PRIMARY_ID = 'primary-cv-aaaa'
const REPLICA_ID = 'replica-cv-bbbb'

const SCHEMA = `
  CREATE TABLE typed_rows (
    id INTEGER PRIMARY KEY,
    str_col TEXT,
    big_col INTEGER,
    float_col REAL,
    bool_col INTEGER,
    blob_col BLOB,
    null_col TEXT
  )
`

describe('E2E: ColumnValue round-trip across all variants', () => {
  let certs: MtlsCerts
  let primary: ManagedNode | null = null
  let replica: ManagedNode | null = null
  let diagnostics: DiagnosticsHandle | null = null

  beforeAll(async () => {
    certs = await createMtlsCerts([PRIMARY_ID, REPLICA_ID])
  })

  afterAll(() => {
    certs?.cleanup()
  })

  beforeEach(async () => {
    primary = await createPrimary({
      nodeId: PRIMARY_ID,
      certs,
      initialize: async (conn, tracker) => {
        await conn.exec(SCHEMA)
        await tracker.watch(conn, 'typed_rows')
      },
    })

    replica = await createReplica({
      nodeId: REPLICA_ID,
      certs,
      primaryHost: '127.0.0.1',
      primaryPort: primary.port,
    })
    diagnostics = attachDiagnostics(primary, replica)

    await waitForReady(replica.engine, 20_000)
  })

  afterEach(async ctx => {
    diagnostics?.dumpIfFailed(ctx)
    if (replica) await stopNode(replica)
    if (primary) await stopNode(primary)
    diagnostics?.cleanup()
    primary = null
    replica = null
    diagnostics = null
  })

  it('replicates strings, large integers, floats, booleans, blobs and nulls without distortion', async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    const big = 9007199254740993n
    const blob = Buffer.from([0x01, 0x02, 0x03, 0xff, 0x7f, 0x00])

    await primary.engine.execute(
      'INSERT INTO typed_rows (id, str_col, big_col, float_col, bool_col, blob_col, null_col) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [1, 'hello world', big, 12345.6789, 1, blob, null],
    )
    await primary.engine.execute(
      'INSERT INTO typed_rows (id, str_col, big_col, float_col, bool_col, blob_col, null_col) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [2, '', 0, -0.5, 0, Buffer.alloc(0), null],
    )

    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)

    await compareDatabases(primary.conn, replica.conn)

    const firstRow = (await (
      await replica.conn.prepare(
        'SELECT id, str_col, CAST(big_col AS TEXT) AS big_col, float_col, bool_col, blob_col, null_col FROM typed_rows WHERE id = 1',
      )
    ).get()) as {
      id: number
      str_col: string
      big_col: string
      float_col: number
      bool_col: number
      blob_col: Buffer
      null_col: unknown
    }

    expect(firstRow.str_col).toBe('hello world')
    expect(BigInt(firstRow.big_col)).toBe(big)
    expect(firstRow.float_col).toBeCloseTo(12345.6789, 4)
    expect(firstRow.bool_col).toBe(1)
    const blobValue: unknown = firstRow.blob_col
    expect(Buffer.isBuffer(blobValue) || blobValue instanceof Uint8Array).toBe(true)
    expect(Buffer.compare(Buffer.from(firstRow.blob_col), blob)).toBe(0)
    expect(firstRow.null_col).toBeNull()
  })
})

import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import { CHANGES_TABLE } from '../internal-tables.js'
import { randomHex } from '../random-hex.js'
import {
  ensureMetaTable,
  insertMetaValueIfAbsent,
  selectMaxChangeHlc,
  selectMetaValue,
  selectTableExists,
  UPSERT_META_VALUE_SQL,
  updateChangeStampsAboveMetaSeqSql,
  upsertMetaMaxChangeSeqSql,
} from '../system-catalog/index.js'
import { HLC } from './hlc.js'
import { HLC_CLOCK_META_KEY, isWellFormedHlc, loadPersistedHlc } from './hlc-store.js'

export const NODE_ID_META_KEY = 'node_id'

const STAMP_FLOOR_META_KEY = 'stamp_floor_seq'

export interface StampStatement {
  sql: string
  params: unknown[]
  trusted: true
}

export class SyncStamper {
  readonly openingStatement: StampStatement
  private readonly stampSql: string

  private constructor(
    readonly nodeId: string,
    readonly hlc: HLC,
    changesTable: string,
  ) {
    this.openingStatement = {
      sql: upsertMetaMaxChangeSeqSql(changesTable),
      params: [STAMP_FLOOR_META_KEY],
      trusted: true,
    }
    this.stampSql = updateChangeStampsAboveMetaSeqSql(changesTable)
  }

  static async init(conn: SQLiteConnection, changesTable: string = CHANGES_TABLE): Promise<SyncStamper> {
    await ensureMetaTable(conn)

    await insertMetaValueIfAbsent(conn, NODE_ID_META_KEY, randomHex(16))
    const nodeId = await selectMetaValue(conn, NODE_ID_META_KEY)
    if (nodeId === null || nodeId.length === 0) {
      throw new CDCError('Failed to read the persisted node identity')
    }

    const hlc = new HLC(nodeId)
    const persisted = await loadPersistedHlc(conn)
    if (persisted !== null) {
      hlc.receive(persisted)
    }
    const persistedMax = (await selectTableExists(conn, changesTable))
      ? await selectMaxChangeHlc(conn, changesTable)
      : null
    if (persistedMax !== null && isWellFormedHlc(persistedMax)) {
      hlc.receive(persistedMax)
    }

    return new SyncStamper(nodeId, hlc, changesTable)
  }

  stampStatements(options?: { persistClock?: boolean }): StampStatement[] {
    const hlcValue = this.hlc.now()
    const txId = randomHex(16)
    const statements: StampStatement[] = [
      {
        sql: this.stampSql,
        params: [this.nodeId, txId, hlcValue, STAMP_FLOOR_META_KEY],
        trusted: true,
      },
    ]
    if (options?.persistClock !== false) {
      statements.push({
        sql: UPSERT_META_VALUE_SQL,
        params: [HLC_CLOCK_META_KEY, hlcValue],
        trusted: true,
      })
    }
    return statements
  }

  async openStamps(txConn: SQLiteConnection): Promise<void> {
    const stmt = await txConn.prepare(this.openingStatement.sql)
    await stmt.run(...this.openingStatement.params)
  }

  async applyStamps(txConn: SQLiteConnection): Promise<void> {
    for (const statement of this.stampStatements()) {
      const stmt = await txConn.prepare(statement.sql)
      await stmt.run(...statement.params)
    }
  }
}

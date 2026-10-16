import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import { CHANGES_TABLE } from '../internal-tables.js'
import { execute } from '../query-executor.js'
import { randomHex } from '../random-hex.js'
import {
  ensureMetaTable,
  insertMetaValueIfAbsent,
  selectMaxChangeHlc,
  selectMetaValue,
  selectTableExists,
  updateChangeStampsAboveMetaSeqSql,
  upsertMetaMaxChangeSeqSql,
  upsertMetaValueAndMaxChangeSeqSql,
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
  readonly claimingStatement: StampStatement
  private readonly stampSql: string
  private readonly closingSql: string

  private constructor(
    readonly nodeId: string,
    readonly hlc: HLC,
    changesTable: string,
  ) {
    this.claimingStatement = {
      sql: upsertMetaMaxChangeSeqSql(changesTable),
      params: [STAMP_FLOOR_META_KEY],
      trusted: true,
    }
    this.stampSql = updateChangeStampsAboveMetaSeqSql(changesTable)
    this.closingSql = upsertMetaValueAndMaxChangeSeqSql(changesTable)
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

  /**
   * Returns the statement that a write path executes first in its transaction, which stamps every unstamped change
   * above the stamp floor with this node, one fresh transaction id, and one fresh timestamp.
   *
   * Those changes are the ones that another process wrote into the file since Sirannon last stamped, so they become
   * one transaction of their own, ordered before the write path's own changes.
   *
   * @returns The statement, with fresh parameters on every call.
   */
  openingStatement(): StampStatement {
    return {
      sql: this.stampSql,
      params: [this.nodeId, randomHex(16), this.hlc.now(), STAMP_FLOOR_META_KEY],
      trusted: true,
    }
  }

  /**
   * Returns the statements that a write path executes last in its transaction, which stamp its own changes under one
   * fresh transaction id and, unless `persistClock` is `false`, store the clock and raise the stamp floor to the
   * highest sequence in the change log.
   *
   * @param options - Set `persistClock` to `false` for every unit of a group but the last.
   * @returns The statements, with fresh parameters on every call.
   */
  stampStatements(options?: { persistClock?: boolean }): StampStatement[] {
    const hlcValue = this.hlc.now()
    const statements: StampStatement[] = [
      {
        sql: this.stampSql,
        params: [this.nodeId, randomHex(16), hlcValue, STAMP_FLOOR_META_KEY],
        trusted: true,
      },
    ]
    if (options?.persistClock !== false) {
      statements.push({
        sql: this.closingSql,
        params: [HLC_CLOCK_META_KEY, hlcValue, STAMP_FLOOR_META_KEY],
        trusted: true,
      })
    }
    return statements
  }

  async openStamps(txConn: SQLiteConnection): Promise<void> {
    const opening = this.openingStatement()
    await execute(txConn, opening.sql, opening.params, true)
  }

  /**
   * Raises the stamp floor to the highest sequence in the change log without stamping anything, so that the changes
   * that a migration wrote stay unstamped and no later write path claims them.
   *
   * @param txConn - The connection inside the migration's transaction.
   * @returns A promise that settles once SQLite has stored the floor.
   */
  async claimWithoutStamping(txConn: SQLiteConnection): Promise<void> {
    await execute(txConn, this.claimingStatement.sql, this.claimingStatement.params, true)
  }

  async applyStamps(txConn: SQLiteConnection): Promise<void> {
    for (const statement of this.stampStatements()) {
      await execute(txConn, statement.sql, statement.params, true)
    }
  }
}

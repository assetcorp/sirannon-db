import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import { CHANGES_TABLE, META_TABLE } from '../internal-tables.js'
import { ensureMetaTable, getMetaValue } from '../system-catalog/index.js'
import { HLC } from './hlc.js'
import { HLC_CLOCK_META_KEY, isWellFormedHlc, loadPersistedHlc } from './hlc-store.js'

export const NODE_ID_META_KEY = 'node_id'

export interface StampStatement {
  sql: string
  params: unknown[]
  trusted: true
}

function randomHex(byteLength: number): string {
  const bytes = new Uint8Array(byteLength)
  globalThis.crypto.getRandomValues(bytes)
  return Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('')
}

export class SyncStamper {
  private constructor(
    readonly nodeId: string,
    readonly hlc: HLC,
    private readonly changesTable: string,
  ) {}

  static async init(conn: SQLiteConnection, changesTable: string = CHANGES_TABLE): Promise<SyncStamper> {
    await ensureMetaTable(conn)

    const insert = await conn.prepare(`INSERT OR IGNORE INTO "${META_TABLE}" (key, value) VALUES (?, ?)`)
    await insert.run(NODE_ID_META_KEY, randomHex(16))
    const nodeId = await getMetaValue(conn, NODE_ID_META_KEY)
    if (nodeId === null || nodeId.length === 0) {
      throw new CDCError('Failed to read the persisted node identity')
    }

    const hlc = new HLC(nodeId)
    const persisted = await loadPersistedHlc(conn)
    if (persisted !== null) {
      hlc.receive(persisted)
    }
    const maxStmt = await conn.prepare(`SELECT MAX(hlc) AS hlc FROM "${changesTable}" WHERE hlc != ''`)
    const row = (await maxStmt.get()) as { hlc?: unknown } | undefined
    if (typeof row?.hlc === 'string' && isWellFormedHlc(row.hlc)) {
      hlc.receive(row.hlc)
    }

    return new SyncStamper(nodeId, hlc, changesTable)
  }

  stampStatements(options?: { persistClock?: boolean }): StampStatement[] {
    const hlcValue = this.hlc.now()
    const txId = randomHex(16)
    const statements: StampStatement[] = [
      {
        sql: `UPDATE "${this.changesTable}" SET node_id = ?, tx_id = ?, hlc = ? WHERE node_id = ''`,
        params: [this.nodeId, txId, hlcValue],
        trusted: true,
      },
    ]
    if (options?.persistClock !== false) {
      statements.push({
        sql: `INSERT INTO "${META_TABLE}" (key, value) VALUES (?, ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        params: [HLC_CLOCK_META_KEY, hlcValue],
        trusted: true,
      })
    }
    return statements
  }

  async applyStamps(txConn: SQLiteConnection): Promise<void> {
    for (const statement of this.stampStatements()) {
      const stmt = await txConn.prepare(statement.sql)
      await stmt.run(...statement.params)
    }
  }
}

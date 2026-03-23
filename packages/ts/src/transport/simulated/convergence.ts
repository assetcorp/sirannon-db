import type { SQLiteConnection } from '../../core/driver/types.js'

export interface NodeSnapshot {
  nodeId: string
  tables: Map<string, TableSnapshot>
}

export interface TableSnapshot {
  columns: string[]
  primaryKeyColumns: string[]
  rows: Array<Record<string, unknown>>
}

export interface RowDivergence {
  key: string
  nodeAValue: Record<string, unknown> | undefined
  nodeBValue: Record<string, unknown> | undefined
}

export interface TableDivergence {
  table: string
  nodeA: string
  nodeB: string
  schemaMismatch: boolean
  rowDiffs: RowDivergence[]
}

export interface ConvergenceResult {
  converged: boolean
  divergences: TableDivergence[]
}

async function getTableColumns(conn: SQLiteConnection, table: string): Promise<string[]> {
  const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
  const info = (await stmt.all()) as Array<{ name: string; pk: number }>
  return info.map(col => col.name)
}

async function getPrimaryKeyColumns(conn: SQLiteConnection, table: string): Promise<string[]> {
  const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
  const info = (await stmt.all()) as Array<{ name: string; pk: number }>
  const pkCols = info
    .filter(col => col.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map(col => col.name)
  if (pkCols.length === 0) {
    return info.map(col => col.name)
  }
  return pkCols
}

function rowKey(row: Record<string, unknown>, pkColumns: string[]): string {
  return pkColumns.map(col => JSON.stringify(row[col] ?? null)).join('|')
}

export class ConvergenceOracle {
  async snapshot(conn: SQLiteConnection, nodeId: string, tables: string[]): Promise<NodeSnapshot> {
    const tableSnapshots = new Map<string, TableSnapshot>()

    for (const table of tables) {
      const columns = await getTableColumns(conn, table)
      const pkColumns = await getPrimaryKeyColumns(conn, table)
      const orderBy = pkColumns.map(col => `"${col}" ASC`).join(', ')
      const selectStmt = await conn.prepare(`SELECT * FROM "${table}" ORDER BY ${orderBy}`)
      const rows = (await selectStmt.all()) as Array<Record<string, unknown>>
      tableSnapshots.set(table, { columns: columns.sort(), primaryKeyColumns: pkColumns, rows })
    }

    return { nodeId, tables: tableSnapshots }
  }

  compare(snapshots: NodeSnapshot[]): ConvergenceResult {
    const divergences: TableDivergence[] = []

    for (let i = 0; i < snapshots.length; i++) {
      for (let j = i + 1; j < snapshots.length; j++) {
        const a = snapshots[i]
        const b = snapshots[j]
        const allTables = new Set([...a.tables.keys(), ...b.tables.keys()])

        for (const table of allTables) {
          const tableA = a.tables.get(table)
          const tableB = b.tables.get(table)

          if (!tableA || !tableB) {
            divergences.push({
              table,
              nodeA: a.nodeId,
              nodeB: b.nodeId,
              schemaMismatch: true,
              rowDiffs: [],
            })
            continue
          }

          const schemasMatch =
            tableA.columns.length === tableB.columns.length &&
            tableA.columns.every((col, idx) => col === tableB.columns[idx])

          if (!schemasMatch) {
            divergences.push({
              table,
              nodeA: a.nodeId,
              nodeB: b.nodeId,
              schemaMismatch: true,
              rowDiffs: [],
            })
            continue
          }

          const rowDiffs = this.diffRows(tableA.rows, tableB.rows, tableA.primaryKeyColumns)

          if (rowDiffs.length > 0) {
            divergences.push({
              table,
              nodeA: a.nodeId,
              nodeB: b.nodeId,
              schemaMismatch: false,
              rowDiffs,
            })
          }
        }
      }
    }

    return { converged: divergences.length === 0, divergences }
  }

  async assertConverged(nodes: Array<{ nodeId: string; conn: SQLiteConnection }>, tables: string[]): Promise<void> {
    const snapshots: NodeSnapshot[] = []
    for (const node of nodes) {
      snapshots.push(await this.snapshot(node.conn, node.nodeId, tables))
    }

    const result = this.compare(snapshots)
    if (result.converged) return

    const messages: string[] = ['Nodes have not converged:']
    for (const d of result.divergences) {
      if (d.schemaMismatch) {
        messages.push(`  Table "${d.table}": schema mismatch between ${d.nodeA} and ${d.nodeB}`)
        continue
      }
      messages.push(`  Table "${d.table}" between ${d.nodeA} and ${d.nodeB}: ${d.rowDiffs.length} row(s) differ`)
      for (const row of d.rowDiffs.slice(0, 5)) {
        if (!row.nodeAValue) {
          messages.push(`    key=${row.key}: missing on ${d.nodeA}`)
        } else if (!row.nodeBValue) {
          messages.push(`    key=${row.key}: missing on ${d.nodeB}`)
        } else {
          messages.push(`    key=${row.key}: ${JSON.stringify(row.nodeAValue)} vs ${JSON.stringify(row.nodeBValue)}`)
        }
      }
      if (d.rowDiffs.length > 5) {
        messages.push(`    ... and ${d.rowDiffs.length - 5} more`)
      }
    }
    throw new Error(messages.join('\n'))
  }

  private diffRows(
    rowsA: Array<Record<string, unknown>>,
    rowsB: Array<Record<string, unknown>>,
    pkColumns: string[],
  ): RowDivergence[] {
    const mapA = new Map<string, Record<string, unknown>>()
    const mapB = new Map<string, Record<string, unknown>>()

    for (const row of rowsA) {
      mapA.set(rowKey(row, pkColumns), row)
    }
    for (const row of rowsB) {
      mapB.set(rowKey(row, pkColumns), row)
    }

    const allKeys = new Set([...mapA.keys(), ...mapB.keys()])
    const diffs: RowDivergence[] = []

    for (const key of allKeys) {
      const valA = mapA.get(key)
      const valB = mapB.get(key)

      if (!valA || !valB) {
        diffs.push({ key, nodeAValue: valA, nodeBValue: valB })
        continue
      }

      if (JSON.stringify(valA) !== JSON.stringify(valB)) {
        diffs.push({ key, nodeAValue: valA, nodeBValue: valB })
      }
    }

    return diffs
  }
}

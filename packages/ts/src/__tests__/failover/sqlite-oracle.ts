import { existsSync } from 'node:fs'
import { DatabaseSync } from 'node:sqlite'

export interface ItemRow {
  id: number
  owner: string
  value: number
  note: string | null
}

export function readItems(dbPath: string): ItemRow[] {
  return readFile(dbPath, 'SELECT id, owner, value, note FROM failover_items ORDER BY id').map(row => ({
    id: Number(row.id),
    owner: String(row.owner),
    value: Number(row.value),
    note: row.note === null ? null : String(row.note),
  }))
}

export function readItemIds(dbPath: string): number[] {
  return readItems(dbPath).map(row => row.id)
}

export function readEventIds(dbPath: string): number[] {
  return readFile(dbPath, 'SELECT id FROM failover_events ORDER BY id').map(row => Number(row.id))
}

export function readHighestOwnChangeSeq(dbPath: string, nodeId: string): bigint {
  const connection = new DatabaseSync(dbPath, { readOnly: true })
  try {
    const row = connection.prepare('SELECT MAX(seq) AS seq FROM _sirannon_changes WHERE node_id = ?').get(nodeId) as
      | { seq: number | null }
      | undefined
    return BigInt(row?.seq ?? 0)
  } finally {
    connection.close()
  }
}

export function readHighestAppliedSeq(dbPath: string, sourceNodeId: string): bigint {
  const connection = new DatabaseSync(dbPath, { readOnly: true })
  try {
    const row = connection
      .prepare('SELECT MAX(source_seq) AS seq FROM _sirannon_applied_changes WHERE source_node_id = ?')
      .get(sourceNodeId) as { seq: number | null } | undefined
    return BigInt(row?.seq ?? 0)
  } finally {
    connection.close()
  }
}

export function readFile(
  dbPath: string,
  sql: string,
  params: Array<string | number> = [],
): Array<Record<string, unknown>> {
  if (!existsSync(dbPath)) return []
  const connection = new DatabaseSync(dbPath, { readOnly: true })
  try {
    const exists = connection
      .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'failover_items'")
      .all()
    if (exists.length === 0) return []
    return connection.prepare(sql).all(...params) as Array<Record<string, unknown>>
  } finally {
    connection.close()
  }
}

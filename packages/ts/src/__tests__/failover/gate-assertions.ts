import { expect } from 'vitest'
import { type GateEnvironment, requireNode } from './gate-environment.js'
import { collectNodeDiagnostics, queryRows, queryRowsWhenReady, statusOf } from './gate-observations.js'
import { fileSize, jsonReplacer, waitForCondition } from './gate-support.js'
import type { FailoverNodeProcess } from './node-process.js'

export async function waitForRows(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  expectedRows: number,
  timeoutMs: number,
): Promise<void> {
  const latest: Record<string, unknown> = {}
  try {
    await waitForCondition(async () => {
      for (const nodeId of nodeIds) {
        const node = requireNode(environment, nodeId)
        const rows = await queryRowsWhenReady(node, 'SELECT COUNT(*) AS count FROM failover_items')
        if (rows === null) {
          latest[nodeId] = await statusOf(node)
          return false
        }
        latest[nodeId] = rows[0] ?? null
        if (Number(rows[0]?.count ?? 0) < expectedRows) {
          return false
        }
      }
      return true
    }, timeoutMs)
  } catch (err: unknown) {
    const diagnostics = await collectNodeDiagnostics(environment)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for ${expectedRows} failover_items rows on ${nodeIds.join(
        ',',
      )}: ${message}\nlatest=${JSON.stringify(latest, jsonReplacer, 2)}\ndiagnostics=${JSON.stringify(
        diagnostics,
        jsonReplacer,
        2,
      )}`,
    )
  }
}

export async function waitForItemNote(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  itemId: number,
  note: string,
  timeoutMs: number,
): Promise<void> {
  const latest: Record<string, unknown> = {}
  try {
    await waitForCondition(async () => {
      for (const nodeId of nodeIds) {
        const node = requireNode(environment, nodeId)
        const rows = await queryRowsWhenReady(node, 'SELECT note FROM failover_items WHERE id = ?', [itemId])
        if (rows === null) {
          latest[nodeId] = await statusOf(node)
          return false
        }
        latest[nodeId] = rows
        if (rows[0]?.note !== note) return false
      }
      return true
    }, timeoutMs)
  } catch (err: unknown) {
    const diagnostics = await collectNodeDiagnostics(environment)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for failover_items id ${itemId} note '${note}' on ${nodeIds.join(
        ',',
      )}: ${message}\nlatest=${JSON.stringify(latest, jsonReplacer, 2)}\ndiagnostics=${JSON.stringify(
        diagnostics,
        jsonReplacer,
        2,
      )}`,
    )
  }
}

export async function waitForEvent(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  eventId: number,
  timeoutMs: number,
): Promise<void> {
  const latest: Record<string, unknown> = {}
  try {
    await waitForCondition(async () => {
      for (const nodeId of nodeIds) {
        const node = requireNode(environment, nodeId)
        const rows = await queryRowsWhenReady(node, 'SELECT id FROM failover_events WHERE id = ?', [eventId])
        if (rows === null) {
          latest[nodeId] = await statusOf(node)
          return false
        }
        latest[nodeId] = rows
        if (rows.length === 0) return false
      }
      return true
    }, timeoutMs)
  } catch (err: unknown) {
    const diagnostics = await collectNodeDiagnostics(environment)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for failover_events id ${eventId} on ${nodeIds.join(
        ',',
      )}: ${message}\nlatest=${JSON.stringify(latest, jsonReplacer, 2)}\ndiagnostics=${JSON.stringify(
        diagnostics,
        jsonReplacer,
        2,
      )}`,
    )
  }
}

export async function assertItemVisible(node: FailoverNodeProcess, id: number): Promise<void> {
  const rows = await queryRows(node, 'SELECT id FROM failover_items WHERE id = ?', [id])
  expect(rows).toHaveLength(1)
}

export async function assertItemAbsent(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  itemId: number,
  timeoutMs: number,
): Promise<void> {
  await waitForCondition(async () => {
    for (const nodeId of nodeIds) {
      const node = requireNode(environment, nodeId)
      const rows = await queryRowsWhenReady(node, 'SELECT id FROM failover_items WHERE id = ?', [itemId])
      if (rows === null || rows.length > 0) return false
    }
    return true
  }, timeoutMs)
}

export async function expectRejectsWith(promise: Promise<unknown>, codes: string[]): Promise<void> {
  try {
    await promise
  } catch (err: unknown) {
    const code = (err as { code?: string }).code
    expect(codes).toContain(code)
    return
  }
  throw new Error(`Expected operation to reject with one of: ${codes.join(', ')}`)
}

export async function expectResourceUseBelow(environment: GateEnvironment, maxStorageBytes: number): Promise<void> {
  for (const node of environment.nodes.values()) {
    const storageBytes =
      fileSize(node.config.dbPath) + fileSize(`${node.config.dbPath}-wal`) + fileSize(`${node.config.dbPath}-shm`)
    expect(storageBytes).toBeLessThanOrEqual(maxStorageBytes)
  }
  expect(process.memoryUsage().heapUsed).toBeLessThanOrEqual(512 * 1024 * 1024)
}

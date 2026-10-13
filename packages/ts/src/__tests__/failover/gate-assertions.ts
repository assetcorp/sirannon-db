import { expect } from 'vitest'
import { dbPathFor, type GateEnvironment } from './gate-environment.js'
import { collectNodeDiagnostics } from './gate-observations.js'
import { describeValue, holdsThroughout, waitForCondition } from './gate-support.js'
import { type ItemRow, readEventIds, readItems } from './sqlite-oracle.js'

export async function waitForItemsOnDisk(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  expected: readonly ItemRow[],
  timeoutMs: number,
): Promise<void> {
  const latest: Record<string, unknown> = {}
  try {
    await waitForCondition(async () => {
      for (const nodeId of nodeIds) {
        const rows = readItems(dbPathFor(environment, nodeId))
        latest[nodeId] = rows.length
        const byId = new Map(rows.map(row => [row.id, row]))
        for (const row of expected) {
          const stored = byId.get(row.id)
          if (!stored || stored.owner !== row.owner || stored.value !== row.value || stored.note !== row.note) {
            latest[`${nodeId}-missing`] = row
            return false
          }
        }
      }
      return true
    }, timeoutMs)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `${nodeIds.join(',')} never held ${expected.length} expected rows on disk: ${message}\nlatest=${describeValue(
        latest,
      )}\ndiagnostics=${describeValue(await collectNodeDiagnostics(environment))}`,
    )
  }
}

export async function expectItemsAbsentOnDisk(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  itemIds: readonly number[],
  durationMs: number,
): Promise<void> {
  await holdsThroughout(
    async () =>
      nodeIds.every(nodeId => {
        const stored = new Set(readItems(dbPathFor(environment, nodeId)).map(row => row.id))
        return itemIds.every(itemId => !stored.has(itemId))
      }),
    durationMs,
    `rows ${itemIds.join(',')} to stay off the disks of ${nodeIds.join(',')}`,
  )
}

export async function waitForEventOnDisk(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  eventId: number,
  timeoutMs: number,
): Promise<void> {
  try {
    await waitForCondition(
      async () => nodeIds.every(nodeId => readEventIds(dbPathFor(environment, nodeId)).includes(eventId)),
      timeoutMs,
    )
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(`event ${eventId} never reached ${nodeIds.join(',')}: ${message}`)
  }
}

export async function waitForIdenticalFiles(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  timeoutMs: number,
): Promise<ItemRow[]> {
  let latest: Record<string, ItemRow[]> = {}
  try {
    await waitForCondition(async () => {
      latest = Object.fromEntries(nodeIds.map(nodeId => [nodeId, readItems(dbPathFor(environment, nodeId))]))
      const encoded = nodeIds.map(nodeId => JSON.stringify(latest[nodeId]))
      return encoded.every(value => value === encoded[0])
    }, timeoutMs)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(`files of ${nodeIds.join(',')} never matched: ${message}\nlatest=${describeValue(latest)}`)
  }
  const first = nodeIds[0]
  return first === undefined ? [] : (latest[first] ?? [])
}

export async function expectRejectsWith(promise: Promise<unknown>, codes: readonly string[]): Promise<Error> {
  try {
    await promise
  } catch (err: unknown) {
    const code = (err as { code?: string }).code
    expect(codes, err instanceof Error ? err.message : String(err)).toContain(code)
    return err instanceof Error ? err : new Error(String(err))
  }
  throw new Error(`Expected operation to reject with one of: ${codes.join(', ')}`)
}

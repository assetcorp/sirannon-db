import { HLC } from '../hlc.js'
import type { ConflictContext, ConflictResolution, ConflictResolver } from '../types.js'
import { LWWResolver } from './lww.js'

type ColumnVersionGetter = (table: string, rowId: string) => Promise<Map<string, { hlc: string; nodeId: string }>>

export class FieldMergeResolver implements ConflictResolver {
  private readonly getColumnVersions: ColumnVersionGetter
  private readonly lww = new LWWResolver()

  constructor(getColumnVersions: ColumnVersionGetter) {
    this.getColumnVersions = getColumnVersions
  }

  async resolve(ctx: ConflictContext): Promise<ConflictResolution> {
    const columnVersions = await this.getColumnVersions(ctx.table, ctx.rowId)

    if (columnVersions.size === 0) {
      return this.lww.resolve(ctx)
    }

    const localData = ctx.localChange?.newData ?? ctx.localChange?.oldData ?? {}
    const remoteData = ctx.remoteChange.newData ?? {}
    const oldData = ctx.remoteChange.oldData ?? {}

    const localChanged = new Set<string>()
    const remoteChanged = new Set<string>()

    for (const key of Object.keys(localData)) {
      if (JSON.stringify(localData[key]) !== JSON.stringify(oldData[key])) {
        localChanged.add(key)
      }
    }

    for (const key of Object.keys(remoteData)) {
      if (JSON.stringify(remoteData[key]) !== JSON.stringify(oldData[key])) {
        remoteChanged.add(key)
      }
    }

    const overlapping = new Set<string>()
    for (const key of remoteChanged) {
      if (localChanged.has(key)) {
        overlapping.add(key)
      }
    }

    if (overlapping.size === 0 && (localChanged.size > 0 || remoteChanged.size > 0)) {
      const merged: Record<string, unknown> = { ...localData }
      for (const key of remoteChanged) {
        merged[key] = remoteData[key]
      }
      return { action: 'merge', mergedData: merged }
    }

    const merged: Record<string, unknown> = { ...localData }
    let anyRemoteWins = false

    for (const key of overlapping) {
      const cv = columnVersions.get(key)
      if (!cv) {
        const rowLww = this.lww.resolve(ctx)
        if (rowLww.action === 'accept_remote') {
          merged[key] = remoteData[key]
          anyRemoteWins = true
        }
        continue
      }

      const cmp = HLC.compare(ctx.remoteHlc, cv.hlc)
      if (cmp > 0) {
        merged[key] = remoteData[key]
        anyRemoteWins = true
      } else if (cmp === 0 && ctx.remoteChange.nodeId > cv.nodeId) {
        merged[key] = remoteData[key]
        anyRemoteWins = true
      }
    }

    for (const key of remoteChanged) {
      if (!overlapping.has(key)) {
        merged[key] = remoteData[key]
        anyRemoteWins = true
      }
    }

    if (anyRemoteWins) {
      return { action: 'merge', mergedData: merged }
    }

    return { action: 'keep_local' }
  }
}

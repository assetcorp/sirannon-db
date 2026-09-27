import { canonicaliseForChecksum } from '../canonicalise.js'
import { HLC } from '../hlc.js'
import type { ConflictContext, ConflictResolution, ConflictResolver } from '../types.js'
import { LWWResolver } from './lww.js'

type ColumnVersionGetter = (table: string, rowId: string) => Promise<Map<string, { hlc: string; nodeId: string }>>

/**
 * Resolves a conflict column by column, and returns a merged row with both sides' changes when the two sides changed different columns.
 *
 * The resolver compares the local row and the remote row with the remote
 * change's `oldData` to find the columns that each side changed. When the two
 * sets share no column, the resolver returns one row that holds both sides'
 * changes. For each column that both sides changed, the resolver compares the
 * remote HLC with that column's stamp from `getColumnVersions`, and it takes
 * the remote value when the remote stamp is higher, or equal with a higher
 * node ID. When the row has no column stamps, the resolver applies whole-row
 * {@link LWWResolver}.
 *
 * @public
 */
export class FieldMergeResolver implements ConflictResolver {
  private readonly getColumnVersions: ColumnVersionGetter
  private readonly lww = new LWWResolver()

  constructor(getColumnVersions: ColumnVersionGetter) {
    this.getColumnVersions = getColumnVersions
  }

  /**
   * Returns the resolution for one conflicting row, merging changes to different columns and settling each column that both sides changed by its per-column stamp.
   *
   * @param ctx - The local and incoming versions of one row.
   * @returns The version to keep, or the merged row.
   */
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
      if (canonicaliseForChecksum(localData[key]) !== canonicaliseForChecksum(oldData[key])) {
        localChanged.add(key)
      }
    }

    for (const key of Object.keys(remoteData)) {
      if (canonicaliseForChecksum(remoteData[key]) !== canonicaliseForChecksum(oldData[key])) {
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

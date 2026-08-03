import type { ReplicationEngine } from './engine.js'

export async function refreshTriggersAfterDdl(engine: ReplicationEngine): Promise<void> {
  const tracker = engine.tracker
  if (!tracker) return
  for (const table of Array.from(tracker.watchedTables)) {
    try {
      await tracker.watch(engine.writerConn, table)
    } catch {}
  }
}

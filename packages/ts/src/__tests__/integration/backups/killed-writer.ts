import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { CREATE_ORDERS, directoryDestination, INSERT_ORDER } from './harness.js'

export const KILLED_WRITER_ENV = 'SIRANNON_KILLED_WRITER'
export const KILLED_WRITER_READY = 'ready to be killed'
export const CAPTURED_BEFORE_THE_KILL = 'captured-before-the-kill'
export const WRITTEN_BEFORE_THE_KILL = 'written-before-the-kill'

export type KilledWriterMode = 'unsent-writes' | 'refused-capture'

async function writeThenWaitToBeKilled(path: string, bucket: string, stagingDir: string, mode: string): Promise<void> {
  const destination = directoryDestination(bucket)
  if (mode === 'refused-capture') destination.faults.refuseWrite = name => name.endsWith('.wal')
  const db = await new Sirannon({ driver: betterSqlite3() }).open('killed', path, {
    backups: { destination, intervalMs: 0, stagingDir, onError: () => {} },
  })
  await db.execute(CREATE_ORDERS)
  await db.execute(INSERT_ORDER, [CAPTURED_BEFORE_THE_KILL, ''])
  await db.captureBackupChanges().catch(() => undefined)
  await db.execute(INSERT_ORDER, [WRITTEN_BEFORE_THE_KILL, ''])
  process.stdout.write(`${KILLED_WRITER_READY}\n`)
  process.kill(process.pid, 'SIGKILL')
}

const [path, bucket, stagingDir, mode] = process.argv.slice(2)
if (process.env[KILLED_WRITER_ENV] === '1' && path && bucket && stagingDir && mode) {
  await writeThenWaitToBeKilled(path, bucket, stagingDir, mode)
}

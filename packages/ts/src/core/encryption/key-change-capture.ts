import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { EncryptionError } from './errors.js'
import { assertSameWriter, type KeyChangeCapture, keyChangeAborted } from './key-change-round.js'
import { startTriggerCapture } from './key-change-trigger-capture.js'

const SMALL_SESSION_ROUND_BYTES = 256 * 1024

async function applySession(target: SQLiteConnection, changes: Uint8Array): Promise<void> {
  if (changes.byteLength === 0) return
  const apply = target.applyCapturedChanges
  if (!apply) {
    throw new EncryptionError(
      'The driver opens connections that cannot apply recorded changes, so a key change cannot run on it',
      'ENCRYPTION_UNSUPPORTED',
    )
  }
  try {
    await apply.call(target, changes)
  } catch (err) {
    if (err instanceof SirannonError) throw err
    throw keyChangeAborted('A write committed during the key change could not be applied exactly to the new file', err)
  }
}

function startSessionCapture(
  writer: SQLiteConnection,
  take: () => Promise<Uint8Array>,
  stop: () => Promise<void>,
): KeyChangeCapture {
  return {
    async drain(current) {
      assertSameWriter(writer, current)
      const changes = await take()
      return {
        small: changes.byteLength <= SMALL_SESSION_ROUND_BYTES,
        apply: target => applySession(target, changes),
      }
    },
    finish: async () => undefined,
    stop,
  }
}

/**
 * Starts recording the writes that commit on the writer: through SQLite's session extension where the driver's
 * connections expose it, and otherwise through temporary triggers that record the key of each row that a write
 * touches.
 *
 * @param writer - The original's writer, with writes held.
 * @returns The capture.
 * @internal
 */
export async function startKeyChangeCapture(writer: SQLiteConnection): Promise<KeyChangeCapture> {
  const { startChangeCapture, takeCapturedChanges, stopChangeCapture } = writer
  if (!startChangeCapture || !takeCapturedChanges || !stopChangeCapture) return startTriggerCapture(writer)
  await startChangeCapture.call(writer)
  return startSessionCapture(
    writer,
    () => takeCapturedChanges.call(writer),
    () => stopChangeCapture.call(writer),
  )
}

import { upsertDeviceAck } from './device-cursors.js'
import { isValidDeviceId } from './sync-protocol.js'
import type { WSConnection } from './ws-connection.js'
import type { ConnectionState } from './ws-handler.js'
import type { WSSubscribeDeps } from './ws-subscribe.js'

const SEQ_RE = /^\d{1,19}$/

export async function handleAckMessage(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  msg: Record<string, unknown>,
  id: string,
): Promise<void> {
  if (!isValidDeviceId(msg.deviceId)) {
    deps.sendError(conn, id, 'INVALID_MESSAGE', '"deviceId" must be a 32-hex device id')
    return
  }
  if (typeof msg.seq !== 'string' || !SEQ_RE.test(msg.seq)) {
    deps.sendError(conn, id, 'INVALID_MESSAGE', '"seq" must be a non-negative integer string')
    return
  }

  const deviceId = msg.deviceId
  const seq = BigInt(msg.seq)
  try {
    await state.database.runCdcMaintenance(writer => upsertDeviceAck(writer, deviceId, seq))
    deps.sendResult(conn, id, { acked: true, seq: seq.toString() })
  } catch (err) {
    deps.sendSirannonError(conn, id, err)
  }
}

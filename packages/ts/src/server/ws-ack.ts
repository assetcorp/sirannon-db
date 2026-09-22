import { SEQ_STRING_RE } from '../core/sync/validators.js'
import { upsertDeviceAck } from './device-cursors.js'
import { isValidDeviceId } from './sync-protocol.js'
import type { WSConnection } from './ws-connection.js'
import type { ConnectionState } from './ws-handler.js'
import type { WSSubscribeDeps } from './ws-subscribe.js'

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
  if (typeof msg.seq !== 'string' || !SEQ_STRING_RE.test(msg.seq)) {
    deps.sendError(conn, id, 'INVALID_MESSAGE', '"seq" must be a non-negative integer string')
    return
  }

  const deviceId = msg.deviceId
  const streams = [...state.deviceStreams.values()].filter(stream => stream.deviceId === deviceId)
  if (streams.length === 0) {
    deps.sendError(
      conn,
      id,
      'DEVICE_NOT_SUBSCRIBED',
      'An acknowledgement names a device this connection holds no subscription for',
    )
    return
  }

  const seq = BigInt(msg.seq)
  try {
    await state.database.runCdcMaintenance(writer => upsertDeviceAck(writer, deviceId, seq))
    for (const stream of streams) {
      stream.onAck(seq)
    }
    deps.sendResult(conn, id, { acked: true, seq: seq.toString() })
  } catch (err) {
    deps.sendSirannonError(conn, id, err)
  }
}

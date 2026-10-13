import { afterEach, describe, expect, it } from 'vitest'
import { WebSocketTransport } from '../../../client/index.js'
import { readSqlite } from '../lib/served-registry.js'
import { DATABASE_ID, INSERT_ORDER, middlebox, pause, releaseClientServers, servedShop } from './harness.js'

const DEVICE_ID = '0123456789abcdef0123456789abcdef'
const RESTORE_HOOK_DELAY_MS = 300

afterEach(releaseClientServers)

describe('device acknowledgements across a reconnect', () => {
  it('holds an acknowledgement until the new connection confirms the subscription, then records it', async () => {
    let subscribeCalls = 0
    const shop = await servedShop({
      customers: ['ama', 'kofi', 'esi'],
      server: { acceptDeviceSync: true, authenticate: () => ({ device: DEVICE_ID }) },
      registry: {
        hooks: {
          onBeforeSubscribe: async () => {
            subscribeCalls += 1
            if (subscribeCalls > 1) await pause(RESTORE_HOOK_DELAY_MS)
          },
        },
      },
    })
    const box = await middlebox(shop.served.port)
    const transport = new WebSocketTransport(`ws://127.0.0.1:${box.port}/db/${DATABASE_ID}`, {
      autoReconnect: true,
      reconnectInterval: 5_000,
      requestTimeout: 5_000,
    })
    let baseline: bigint | undefined
    await transport.subscribe('orders', undefined, () => {}, {
      deviceId: DEVICE_ID,
      onSubscribed: info => {
        baseline = info.seq
      },
    })
    await shop.database.execute(INSERT_ORDER, ['yaw', 40])
    const toAcknowledge = (baseline ?? 0n) + 1n

    box.killAllConnections()
    await pause(20)
    const acknowledged = await transport.ack(DEVICE_ID, toAcknowledge)
    transport.close()

    const secondConnection = box.observed.filter(entry => entry.connection === 2)
    const order = secondConnection.map(entry => `${entry.direction}:${String(entry.message.type)}`)
    const [cursor] = readSqlite<{ acked_seq: bigint }>(
      shop.path,
      'SELECT acked_seq FROM _sirannon_device_cursors WHERE device_id = ?',
      [DEVICE_ID],
    )
    expect(acknowledged).toEqual({ acked: true, seq: toAcknowledge.toString() })
    expect(order.indexOf('client:ack')).toBeGreaterThan(order.indexOf('server:subscribed'))
    expect(order.filter(step => step === 'client:subscribe')).toHaveLength(1)
    expect(box.messages('server', 'error')).toEqual([])
    expect(subscribeCalls).toBe(2)
    expect(cursor?.acked_seq).toBe(toAcknowledge)
  })
})

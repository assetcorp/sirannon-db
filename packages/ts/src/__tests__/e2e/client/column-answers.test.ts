import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { waitUntil } from '../lib/served-registry.js'
import { closeLater, DATABASE_ID, middlebox, recordingProxy, releaseClientServers, servedShop } from './harness.js'

afterEach(releaseClientServers)

describe.each(['http', 'websocket'] as const)('columns answers over %s', transport => {
  it('rejects an incomplete answer and an integer outside SQLite storage before returning rows', async () => {
    const shop = await servedShop({ customers: [] })
    const proxy = await recordingProxy(shop.served.port)
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(transport === 'http' ? proxy.base : box.base, { transport })
    closeLater(() => client.close())
    const db = client.database(DATABASE_ID)
    await db.query('SELECT 1')
    box.stallServerFrames()
    const column = { name: 'n', declaredType: null, table: null, column: null, database: null }
    const bodies = [
      { columns: [], rows: [] },
      { columns: [column], rows: [[{ __sirannon_int: '9223372036854775808' }]], truncated: null },
      { columns: [column], rows: [[{ __sirannon_int: '-9223372036854775809' }]], truncated: null },
      { columns: [column], rows: [[{ __sirannon_blob: '0G' }]], truncated: null },
      { columns: [column], rows: [[1, 2]], truncated: null },
    ]
    for (const body of bodies) {
      proxy.answerWith(() => ({ kind: 'reply', status: 200, text: JSON.stringify(body) }))
      const previous = box.messages('client', 'query').length
      const refused = expect(db.queryWithColumns('SELECT 1 AS n')).rejects.toMatchObject({ code: 'INVALID_RESPONSE' })
      if (transport === 'websocket') {
        await waitUntil(() => box.messages('client', 'query').length > previous, 'the columns request')
        const frame = box.messages('client', 'query').at(-1)
        expect(box.inject(JSON.stringify({ type: 'result', id: frame?.id, data: body }))).toBe(1)
      }
      await refused
    }
  })
})

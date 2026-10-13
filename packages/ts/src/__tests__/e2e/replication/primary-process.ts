import { createPrimary } from '../lib/factory.js'

const [nodeId, caCertPath, certPath, keyPath, tempDir, dbPath, listenPort, schema, insert] = process.argv.slice(2)

if (
  nodeId === undefined ||
  caCertPath === undefined ||
  certPath === undefined ||
  keyPath === undefined ||
  tempDir === undefined ||
  dbPath === undefined ||
  listenPort === undefined ||
  schema === undefined ||
  insert === undefined
) {
  throw new Error(
    'Usage: primary-process <nodeId> <caCertPath> <certPath> <keyPath> <tempDir> <dbPath> <port> <schema> <insert>',
  )
}

const node = await createPrimary({
  nodeId,
  certs: { caCertPath, certDir: tempDir, certForNode: () => ({ certPath, keyPath }), cleanup: () => {} },
  storage: { tempDir, dbPath },
  listenPort: Number(listenPort),
  initialize: async (conn, tracker) => {
    await conn.exec(schema)
    await tracker.watch(conn, 'orders')
  },
})

process.stdout.write(`${JSON.stringify({ port: node.port })}\n`)

let id = 1
for (;;) {
  await node.engine.execute(insert, [id, `written-before-the-kill-${id}`, id])
  id += 1
  await new Promise(resolve => setImmediate(resolve))
}

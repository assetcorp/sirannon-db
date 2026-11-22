import { GrpcReplicationTransport } from '../../../transport/grpc/index.js'
import { LOOPBACK_HOST } from '../lib/loopback.js'

const [nodeId, caCertPath, certPath, keyPath, endpoint] = process.argv.slice(2)

if (
  nodeId === undefined ||
  caCertPath === undefined ||
  certPath === undefined ||
  keyPath === undefined ||
  endpoint === undefined
) {
  throw new Error('Usage: dialling-replica-process <nodeId> <caCertPath> <certPath> <keyPath> <endpoint>')
}

const transport = new GrpcReplicationTransport({
  host: LOOPBACK_HOST,
  tlsCert: certPath,
  tlsKey: keyPath,
  tlsCaCert: caCertPath,
})

await transport.connect(nodeId, { localRole: 'replica', endpoints: [endpoint] })

process.on('message', () => {
  transport
    .disconnect()
    .then(() => process.send?.('disconnected'))
    .catch((error: unknown) => process.send?.(`failed: ${String(error)}`))
})
process.channel?.unref()
process.send?.('dialling')

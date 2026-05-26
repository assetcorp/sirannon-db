import { readFileSync } from 'node:fs'
import { type ChannelCredentials, credentials, ServerCredentials } from '@grpc/grpc-js'
import { TransportError } from '../../replication/errors.js'
import type { GrpcReplicationOptions } from './options.js'

export function buildServerCreds(options: GrpcReplicationOptions): ServerCredentials {
  if (options.insecure) {
    return ServerCredentials.createInsecure()
  }
  if (!options.tlsCert || !options.tlsKey) {
    throw new TransportError('TLS certificate and key paths are required when insecure mode is disabled')
  }
  const cert = readFileSync(options.tlsCert)
  const key = readFileSync(options.tlsKey)
  const ca = options.tlsCaCert ? readFileSync(options.tlsCaCert) : null
  return ServerCredentials.createSsl(ca, [{ private_key: key, cert_chain: cert }], ca !== null)
}

export function buildChannelCreds(options: GrpcReplicationOptions): ChannelCredentials {
  if (options.insecure) {
    return credentials.createInsecure()
  }
  if (!options.tlsCert || !options.tlsKey) {
    throw new TransportError('TLS certificate and key paths are required when insecure mode is disabled')
  }
  const cert = readFileSync(options.tlsCert)
  const key = readFileSync(options.tlsKey)
  const ca = options.tlsCaCert ? readFileSync(options.tlsCaCert) : null
  return credentials.createSsl(ca, key, cert)
}

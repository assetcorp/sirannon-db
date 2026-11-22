import { lookup } from 'node:dns/promises'
import { isIP } from 'node:net'
import type { Server, ServerCredentials } from '@grpc/grpc-js'
import { TransportError } from '../../replication/errors.js'

const EPHEMERAL_PORT_BIND_ATTEMPTS = 10

class AddressBindFailure extends Error {
  constructor(
    readonly target: string,
    readonly position: number,
    cause: unknown,
  ) {
    super(cause instanceof Error ? cause.message : String(cause))
  }
}

function unbracketed(host: string): string {
  return host.startsWith('[') && host.endsWith(']') ? host.slice(1, -1) : host
}

function listenTarget(address: string, port: number): string {
  return isIP(address) === 6 ? `[${address}]:${port}` : `${address}:${port}`
}

async function addressesOf(host: string): Promise<string[]> {
  const literal = unbracketed(host)
  if (isIP(literal) !== 0) return [literal]
  let resolved: Array<{ address: string }>
  try {
    resolved = await lookup(literal, { all: true })
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err)
    throw new TransportError(`Failed to bind gRPC server: the host '${host}' did not resolve: ${reason}`)
  }
  const addresses = [...new Set(resolved.map(entry => entry.address))]
  if (addresses.length === 0) {
    throw new TransportError(`Failed to bind gRPC server: the host '${host}' resolved to no address`)
  }
  return addresses
}

function bindOne(server: Server, target: string, credentials: ServerCredentials): Promise<number> {
  return new Promise((resolve, reject) => {
    server.bindAsync(target, credentials, (err, assignedPort) => {
      if (err) reject(err)
      else resolve(assignedPort)
    })
  })
}

async function bindAll(
  server: Server,
  addresses: string[],
  port: number,
  credentials: ServerCredentials,
): Promise<number> {
  const bound: string[] = []
  let assignedPort = port
  try {
    for (const [position, address] of addresses.entries()) {
      const target = listenTarget(address, assignedPort)
      try {
        assignedPort = await bindOne(server, target, credentials)
      } catch (err) {
        throw new AddressBindFailure(target, position, err)
      }
      bound.push(listenTarget(address, assignedPort))
    }
    return assignedPort
  } catch (err) {
    for (const target of bound) server.unbind(target)
    throw err
  }
}

function bindRefusal(host: string, addresses: string[], failure: AddressBindFailure): TransportError {
  if (addresses.length === 1) {
    return new TransportError(`Failed to bind gRPC server on ${failure.target}: ${failure.message}`)
  }
  return new TransportError(
    `Failed to bind gRPC server on ${failure.target}, one of the addresses that '${host}' resolves to (${addresses.join(', ')}), so the server refuses to start listening on only some of them: ${failure.message}`,
  )
}

/**
 * Returns the port that the gRPC server listens on once it has bound every address that the host resolves to. With
 * port 0 and a host that resolves to several addresses, the server binds the first address to a port that the
 * operating system picks, binds the rest to that port, and starts again with a new port when a later address has
 * that port taken.
 *
 * @param server - The gRPC server to bind.
 * @param host - The configured host, which is an IP address, a bracketed IPv6 address, or a name to resolve.
 * @param port - The configured port, where 0 lets the operating system pick one.
 * @param credentials - The credentials that the server presents on every address.
 * @returns The port that every address of the host listens on.
 * @throws A `TransportError` that states the address that failed to bind when the server cannot listen on every
 * address of the host, after it unbinds the addresses that it did bind.
 *
 * @internal
 */
export async function bindEveryAddress(
  server: Server,
  host: string,
  port: number,
  credentials: ServerCredentials,
): Promise<number> {
  const addresses = await addressesOf(host)
  const attempts = port === 0 && addresses.length > 1 ? EPHEMERAL_PORT_BIND_ATTEMPTS : 1
  for (let attempt = 1; ; attempt += 1) {
    try {
      return await bindAll(server, addresses, port, credentials)
    } catch (err) {
      if (!(err instanceof AddressBindFailure)) throw err
      if (attempt >= attempts || err.position === 0) throw bindRefusal(host, addresses, err)
    }
  }
}

import { afterEach, describe, expect, it } from 'vitest'
import { GrpcReplicationTransport } from '../../index.js'
import { teardown, waitFor } from './_helpers.js'

async function reserveFreePort(): Promise<number> {
  const probe = new GrpcReplicationTransport({ insecure: true, port: 0 })
  await probe.connect('probe-node', { localRole: 'primary' })
  const port = probe.getPort()
  await probe.disconnect()
  return port
}

describe('GrpcReplicationTransport reconnection', () => {
  const transports: GrpcReplicationTransport[] = []

  afterEach(async () => {
    await teardown(transports)
  })

  it('redials a peer that restarts on the same endpoint', async () => {
    const primary = new GrpcReplicationTransport({ insecure: true, port: 0 })
    transports.push(primary)
    await primary.connect('primary-node', { localRole: 'primary' })
    const port = primary.getPort()

    const replica = new GrpcReplicationTransport({ insecure: true })
    transports.push(replica)
    await replica.connect('replica-node', { localRole: 'replica', endpoints: [`localhost:${port}`] })
    await waitFor(() => replica.peers().size === 1)

    await primary.disconnect()
    await waitFor(() => replica.peers().size === 0)

    const restarted = new GrpcReplicationTransport({ insecure: true, port })
    transports.push(restarted)
    await restarted.connect('primary-node', { localRole: 'primary' })

    await waitFor(() => replica.peers().size === 1, 20_000)
    expect(replica.peers().get('primary-node')?.role).toBe('primary')
    expect(restarted.peers().get('replica-node')).toBeDefined()
  }, 30_000)

  it('connects to a peer that was unreachable when the transport started', async () => {
    const port = await reserveFreePort()

    const replica = new GrpcReplicationTransport({ insecure: true })
    transports.push(replica)
    await replica.connect('replica-node', { localRole: 'replica', endpoints: [`localhost:${port}`] })
    expect(replica.peers().size).toBe(0)

    const primary = new GrpcReplicationTransport({ insecure: true, port })
    transports.push(primary)
    await primary.connect('primary-node', { localRole: 'primary' })

    await waitFor(() => replica.peers().size === 1, 20_000)
    expect(replica.peers().get('primary-node')?.role).toBe('primary')
  }, 30_000)

  it('stops redialling once the transport disconnects', async () => {
    const port = await reserveFreePort()

    const replica = new GrpcReplicationTransport({ insecure: true })
    await replica.connect('replica-node', { localRole: 'replica', endpoints: [`localhost:${port}`] })
    await replica.disconnect()

    const primary = new GrpcReplicationTransport({ insecure: true, port })
    transports.push(primary)
    await primary.connect('primary-node', { localRole: 'primary' })

    await new Promise(resolve => setTimeout(resolve, 3_000))
    expect(replica.peers().size).toBe(0)
    expect(primary.peers().size).toBe(0)
  }, 30_000)
})

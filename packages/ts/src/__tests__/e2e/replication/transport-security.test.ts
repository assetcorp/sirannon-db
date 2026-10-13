import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { type CertificateAuthority, createCertificateAuthority, expiredWindow } from '../lib/certificate-authority.js'
import { eventually, ReplicationScene, tableNames } from '../lib/replication-scene.js'
import { converged, INSERT_ORDER, INTRUDER, ORDERS_SCHEMA, order, PRIMARY, REPLICA, ready } from './harness.js'
import { ddlChange, helloOutcome, listenAsPeer, type PeerIdentity, signedBatch, stampAt } from './peers.js'

const DAY_MS = 86_400_000
const GRPC_INVALID_ARGUMENT = 3
const GRPC_UNAVAILABLE = 14
const GRPC_UNAUTHENTICATED = 16
const ATTEMPTS_THAT_PROVE_A_REFUSAL = 3

let trusted: CertificateAuthority
let untrusted: CertificateAuthority
let scene: ReplicationScene | undefined

beforeAll(async () => {
  const now = Date.now()
  trusted = await createCertificateAuthority('Orders replication CA', [
    { label: PRIMARY, commonName: PRIMARY },
    { label: REPLICA, commonName: REPLICA },
    { label: INTRUDER, commonName: INTRUDER },
    { label: 'expired-replica', commonName: REPLICA, ...expiredWindow() },
    { label: 'expired-primary', commonName: PRIMARY, ...expiredWindow() },
    {
      label: 'future-replica',
      commonName: REPLICA,
      notBefore: new Date(now + DAY_MS),
      notAfter: new Date(now + 2 * DAY_MS),
    },
  ])
  untrusted = await createCertificateAuthority('Orders replication CA', [
    { label: PRIMARY, commonName: PRIMARY },
    { label: REPLICA, commonName: REPLICA },
  ])
})

afterEach(async () => {
  await scene?.teardown()
  scene = undefined
})

afterAll(() => {
  trusted?.cleanup()
  untrusted?.cleanup()
})

function startScene(): ReplicationScene {
  scene = new ReplicationScene(trusted)
  return scene
}

function presenting(label: string, authority: CertificateAuthority = trusted): PeerIdentity {
  return { nodeId: label, cert: authority.issued(label), caCertPath: trusted.caCertPath }
}

describe('the connection between two nodes', () => {
  it('is refused to a dialler presenting another node name, a foreign authority, a lapsed certificate, or no TLS', async () => {
    const current = startScene()
    const primary = await current.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    const replica = await current.replica(REPLICA, { port: primary.port })
    await ready(replica)

    const genuine = await helloOutcome(primary.port, presenting(INTRUDER), INTRUDER)
    const renamed = await helloOutcome(primary.port, presenting(INTRUDER), REPLICA)
    const unannounced = await helloOutcome(primary.port, presenting(INTRUDER), INTRUDER, 'ack')
    const foreign = await helloOutcome(primary.port, presenting(REPLICA, untrusted), REPLICA)
    const expired = await helloOutcome(primary.port, presenting('expired-replica'), REPLICA)
    const early = await helloOutcome(primary.port, presenting('future-replica'), REPLICA)
    const plaintext = await helloOutcome(primary.port, 'plaintext', REPLICA)

    expect(genuine).toEqual({ answeredBy: PRIMARY, code: null })
    expect(renamed).toEqual({ answeredBy: null, code: GRPC_UNAUTHENTICATED })
    expect(unannounced).toEqual({ answeredBy: null, code: GRPC_INVALID_ARGUMENT })
    for (const refused of [foreign, expired, early, plaintext]) {
      expect(refused).toEqual({ answeredBy: null, code: GRPC_UNAVAILABLE })
    }
    await eventually(
      () => primary.engine.status().peers.filter(peer => peer.connected).length === 1,
      'the genuine stranger to leave again',
    )
    expect(
      primary.engine
        .status()
        .peers.filter(peer => peer.connected)
        .map(peer => peer.nodeId),
    ).toEqual([REPLICA])
    await primary.engine.execute(INSERT_ORDER, [1, 'still served', 1])
    await converged(primary, [replica], 'orders', [order(1, 'still served', 1)])
  })

  it('is refused by a replica when the endpoint it dials claims the primary under another name', async () => {
    const current = startScene()
    const impostor = await listenAsPeer(current, presenting(INTRUDER), 'primary', PRIMARY)
    const replica = await current.replica(REPLICA, { port: impostor.port, config: { initialSync: false } })

    await eventually(
      () => impostor.departures(REPLICA) >= ATTEMPTS_THAT_PROVE_A_REFUSAL || replica.engine.getAppliedSeq(PRIMARY) > 0n,
      'the replica to refuse the impostor more than once',
    )
    if (impostor.transport.peers().has(REPLICA)) {
      await impostor.transport
        .send(
          REPLICA,
          signedBatch(PRIMARY, 1n, [
            ddlChange('CREATE TABLE planted (id INTEGER PRIMARY KEY)', PRIMARY, stampAt(0, PRIMARY)),
          ]),
        )
        .catch(() => {})
    }

    expect(impostor.departures(REPLICA)).toBeGreaterThanOrEqual(ATTEMPTS_THAT_PROVE_A_REFUSAL)
    expect(replica.engine.getAppliedSeq(PRIMARY)).toBe(0n)
    expect(tableNames(replica.dbPath)).toEqual([])
  })

  it('is refused by a replica when the endpoint it dials presents a foreign authority or a lapsed certificate', async () => {
    const current = startScene()
    for (const identity of [presenting(PRIMARY, untrusted), presenting('expired-primary')]) {
      const impostor = await listenAsPeer(current, identity, 'primary', PRIMARY)
      const proxy = await current.proxy(impostor.port)
      const replica = await current.replica(REPLICA, { port: proxy.port, config: { initialSync: false } })

      await eventually(() => proxy.opened >= ATTEMPTS_THAT_PROVE_A_REFUSAL, `${identity.nodeId} to be refused`)

      expect(impostor.peerEvents).toEqual([])
      expect(replica.engine.status().peers.filter(peer => peer.connected)).toEqual([])
      await current.stop(replica)
    }
  })
})

import { describe, expect, it } from 'vitest'
import { SirannonClient, type TopologyAwareClientOptions } from '../client.js'
import { createClientServerHarness } from './server-harness.js'

const harness = createClientServerHarness()

describe('TopologyAwareClientOptions', () => {
  it('accepts primary and replicas config', () => {
    const opts: TopologyAwareClientOptions = {
      primary: harness.baseUrl,
      replicas: [harness.baseUrl],
      readPreference: 'primary',
      transport: 'http',
    }
    const client = new SirannonClient(opts)
    expect(client).toBeInstanceOf(SirannonClient)
    client.close()
  })

  it('rejects unsafe configured endpoint URLs', () => {
    expect(
      () =>
        new SirannonClient({
          primary: 'file:///tmp/test.db',
          transport: 'http',
        }),
    ).toThrow('must use http or https')

    expect(
      () =>
        new SirannonClient({
          primary: 'https://user:password@example.com',
          transport: 'http',
        }),
    ).toThrow('must not contain credentials')
  })

  it('rejects unsafe coordinator-discovered endpoint URLs', async () => {
    const baseUrl = await harness.restart({
      getClusterStatus: databaseId => ({
        databaseId,
        currentPrimary: { nodeId: 'node-a', endpoint: 'file:///tmp/test.db' },
        primaryTerm: 1n,
        readEndpoints: [],
        health: 'healthy',
      }),
    })

    const client = new SirannonClient({
      endpoints: [baseUrl],
      discovery: 'coordinator',
      transport: 'http',
    })

    await expect(client._refreshClusterRouting('testdb')).rejects.toMatchObject({ code: 'INVALID_RESPONSE' })
    client.close()
  })

  it('queries via primary with readPreference primary', async () => {
    const client = new SirannonClient({
      primary: harness.baseUrl,
      replicas: [],
      readPreference: 'primary',
      transport: 'http',
    })
    const db = client.database('testdb')
    const rows = await db.query<{ name: string }>('SELECT name FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')
    client.close()
  })

  describe('routing with distinct endpoints', () => {
    const replica = createClientServerHarness({ userName: 'ReplicaUser' })

    it('executes writes via primary', async () => {
      const client = new SirannonClient({
        primary: harness.baseUrl,
        replicas: [replica.baseUrl],
        readPreference: 'replica',
        transport: 'http',
      })
      const db = client.database('testdb')
      await db.execute("INSERT INTO users (name) VALUES ('Bob')")

      const primaryClient = new SirannonClient(harness.baseUrl, { transport: 'http' })
      const primaryRows = await primaryClient
        .database('testdb')
        .query<{ name: string }>("SELECT name FROM users WHERE name = 'Bob'")
      expect(primaryRows).toHaveLength(1)

      const replicaClient = new SirannonClient(replica.baseUrl, { transport: 'http' })
      const replicaRows = await replicaClient
        .database('testdb')
        .query<{ name: string }>("SELECT name FROM users WHERE name = 'Bob'")
      expect(replicaRows).toHaveLength(0)

      client.close()
      primaryClient.close()
      replicaClient.close()
    })

    it('routes reads to replica when readPreference is replica', async () => {
      const client = new SirannonClient({
        primary: harness.baseUrl,
        replicas: [replica.baseUrl],
        readPreference: 'replica',
        transport: 'http',
      })
      const db = client.database('testdb')
      const rows = await db.query<{ name: string }>('SELECT name FROM users')
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('ReplicaUser')
      client.close()
    })

    it('routes coordinator replica reads away from a readable current primary', async () => {
      const baseUrl = await harness.restart({
        getClusterStatus: databaseId => ({
          databaseId,
          currentPrimary: { nodeId: 'node-a', endpoint: harness.baseUrl },
          primaryTerm: 1n,
          readEndpoints: [
            { nodeId: 'node-a', endpoint: harness.baseUrl, readConcerns: ['local', 'majority', 'linearizable'] },
            { nodeId: 'node-b', endpoint: replica.baseUrl, readConcerns: ['local', 'majority'] },
          ],
          health: 'healthy',
        }),
      })

      const client = new SirannonClient({
        endpoints: [baseUrl],
        discovery: 'coordinator',
        readPreference: 'replica',
        transport: 'http',
      })
      const db = client.database('testdb')
      const rows = await db.query<{ name: string }>('SELECT name FROM users')
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('ReplicaUser')
      client.close()
    })

    it('routes reads with readPreference nearest', async () => {
      const client = new SirannonClient({
        primary: harness.baseUrl,
        replicas: [replica.baseUrl],
        readPreference: 'nearest',
        transport: 'http',
      })
      const db = client.database('testdb')
      const rows = await db.query<{ name: string }>('SELECT name FROM users')
      expect(rows).toHaveLength(1)
      expect(['Alice', 'ReplicaUser']).toContain(rows[0].name)
      client.close()
    })
  })

  it('falls back to primary when all replicas are unreachable', async () => {
    const client = new SirannonClient({
      primary: harness.baseUrl,
      replicas: ['http://127.0.0.1:1'],
      readPreference: 'replica',
      transport: 'http',
    })
    const db = client.database('testdb')
    try {
      await db.query<{ name: string }>('SELECT name FROM users')
    } catch {
      const rows = await db.query<{ name: string }>('SELECT name FROM users')
      expect(rows).toHaveLength(1)
    }
    client.close()
  })

  it('defaults to primary readPreference when omitted', async () => {
    const client = new SirannonClient({
      primary: harness.baseUrl,
      transport: 'http',
    })
    const db = client.database('testdb')
    const rows = await db.query<{ name: string }>('SELECT name FROM users')
    expect(rows).toHaveLength(1)
    client.close()
  })

  it('returns cached database instances', () => {
    const client = new SirannonClient({
      primary: harness.baseUrl,
      transport: 'http',
    })
    const db1 = client.database('testdb')
    const db2 = client.database('testdb')
    expect(db1).toBe(db2)
    client.close()
  })
})

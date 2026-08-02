import { describe, expect, it } from 'vitest'
import { SirannonClient } from '../client.js'
import { createClientServerHarness } from './server-harness.js'

const harness = createClientServerHarness()

describe('backward compatibility', () => {
  it('single-URL constructor works unchanged', () => {
    const client = new SirannonClient(harness.baseUrl)
    expect(client).toBeInstanceOf(SirannonClient)
    client.close()
  })

  it('single-URL constructor with options works unchanged', () => {
    const client = new SirannonClient(harness.baseUrl, { transport: 'http' })
    expect(client).toBeInstanceOf(SirannonClient)
    client.close()
  })

  it('queries work with single-URL constructor', async () => {
    const client = new SirannonClient(harness.baseUrl, { transport: 'http' })
    const db = client.database('testdb')
    const rows = await db.query<{ name: string }>('SELECT name FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')
    client.close()
  })

  it('throws after close with single-URL constructor', () => {
    const client = new SirannonClient(harness.baseUrl)
    client.close()
    expect(() => client.database('testdb')).toThrow('Client is closed')
  })
})

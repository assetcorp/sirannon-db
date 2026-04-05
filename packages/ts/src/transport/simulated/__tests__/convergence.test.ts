import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { ConvergenceOracle } from '../convergence.js'

describe('ConvergenceOracle', () => {
  let tempDir: string
  const connections: SQLiteConnection[] = []
  const oracle = new ConvergenceOracle()

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-conv-'))
  })

  afterEach(async () => {
    for (const conn of connections) {
      try {
        await conn.close()
      } catch {
        /* best-effort */
      }
    }
    connections.length = 0
    rmSync(tempDir, { recursive: true, force: true })
  })

  async function createDb(name: string): Promise<SQLiteConnection> {
    const conn = await testDriver.open(join(tempDir, `${name}.db`))
    connections.push(conn)
    return conn
  }

  it('reports convergence for identical tables', async () => {
    const connA = await createDb('a')
    const connB = await createDb('b')

    for (const conn of [connA, connB]) {
      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)')
      await conn.exec("INSERT INTO items VALUES (1, 'alpha', 10)")
      await conn.exec("INSERT INTO items VALUES (2, 'beta', 20)")
    }

    await oracle.assertConverged(
      [
        { nodeId: 'A', conn: connA },
        { nodeId: 'B', conn: connB },
      ],
      ['items'],
    )
  })

  it('detects row value differences', async () => {
    const connA = await createDb('a')
    const connB = await createDb('b')

    await connA.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    await connA.exec("INSERT INTO items VALUES (1, 'alpha')")

    await connB.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    await connB.exec("INSERT INTO items VALUES (1, 'beta')")

    await expect(
      oracle.assertConverged(
        [
          { nodeId: 'A', conn: connA },
          { nodeId: 'B', conn: connB },
        ],
        ['items'],
      ),
    ).rejects.toThrow('Nodes have not converged')
  })

  it('detects missing rows', async () => {
    const connA = await createDb('a')
    const connB = await createDb('b')

    await connA.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    await connA.exec("INSERT INTO items VALUES (1, 'alpha')")
    await connA.exec("INSERT INTO items VALUES (2, 'beta')")

    await connB.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    await connB.exec("INSERT INTO items VALUES (1, 'alpha')")

    const snapA = await oracle.snapshot(connA, 'A', ['items'])
    const snapB = await oracle.snapshot(connB, 'B', ['items'])
    const result = oracle.compare([snapA, snapB])

    expect(result.converged).toBe(false)
    expect(result.divergences).toHaveLength(1)
    expect(result.divergences[0].rowDiffs).toHaveLength(1)
    expect(result.divergences[0].rowDiffs[0].nodeBValue).toBeUndefined()
  })

  it('detects schema mismatch when a table exists on one node only', async () => {
    const connA = await createDb('a')
    const connB = await createDb('b')

    await connA.exec('CREATE TABLE items (id INTEGER PRIMARY KEY)')
    await connB.exec('CREATE TABLE other (id INTEGER PRIMARY KEY)')

    const snapA = await oracle.snapshot(connA, 'A', ['items'])
    const snapB = await oracle.snapshot(connB, 'B', ['other'])
    const result = oracle.compare([snapA, snapB])

    expect(result.converged).toBe(false)
    expect(result.divergences.some(d => d.schemaMismatch)).toBe(true)
  })

  it('converges on empty tables', async () => {
    const connA = await createDb('a')
    const connB = await createDb('b')

    await connA.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    await connB.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')

    await oracle.assertConverged(
      [
        { nodeId: 'A', conn: connA },
        { nodeId: 'B', conn: connB },
      ],
      ['items'],
    )
  })

  it('compares three nodes pairwise', async () => {
    const connA = await createDb('a')
    const connB = await createDb('b')
    const connC = await createDb('c')

    for (const conn of [connA, connB]) {
      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, val INTEGER)')
      await conn.exec('INSERT INTO items VALUES (1, 100)')
    }
    await connC.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, val INTEGER)')
    await connC.exec('INSERT INTO items VALUES (1, 999)')

    const nodes = [
      { nodeId: 'A', conn: connA },
      { nodeId: 'B', conn: connB },
      { nodeId: 'C', conn: connC },
    ]

    const snaps = await Promise.all(nodes.map(n => oracle.snapshot(n.conn, n.nodeId, ['items'])))
    const result = oracle.compare(snaps)

    expect(result.converged).toBe(false)
    const divergentPairs = result.divergences.map(d => `${d.nodeA}-${d.nodeB}`)
    expect(divergentPairs).toContain('A-C')
    expect(divergentPairs).toContain('B-C')
    expect(divergentPairs).not.toContain('A-B')
  })

  it('uses primary key columns for row matching, not rowid', async () => {
    const connA = await createDb('a')
    const connB = await createDb('b')

    await connA.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    await connA.exec("INSERT INTO items VALUES (1, 'first')")
    await connA.exec("INSERT INTO items VALUES (2, 'second')")

    await connB.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    await connB.exec("INSERT INTO items VALUES (2, 'second')")
    await connB.exec("INSERT INTO items VALUES (1, 'first')")

    await oracle.assertConverged(
      [
        { nodeId: 'A', conn: connA },
        { nodeId: 'B', conn: connB },
      ],
      ['items'],
    )
  })
})

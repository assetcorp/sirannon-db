import { readFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import { SirannonClient } from '../client.js'
import * as browserEntry from '../index.js'
import { RemoteError } from '../types.js'

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../../..')

function constructWith(argument: unknown): () => void {
  return () => new SirannonClient(argument as string)
}

describe('the browser-facing client', () => {
  it('refuses every topology option and names the entry point that carries it', () => {
    const rejected = [
      { endpoints: ['http://127.0.0.1:9876'] },
      { primary: 'http://127.0.0.1:9876' },
      { replicas: ['http://127.0.0.1:9876'] },
      { discovery: 'coordinator' },
      { readPreference: 'nearest' },
      { readConcern: 'majority' },
    ]

    for (const options of rejected) {
      expect(constructWith(options)).toThrow(RemoteError)
      expect(constructWith(options)).toThrow(/client\/topology/)
      expect(constructWith(options)).toThrow(new RegExp(Object.keys(options)[0]))
    }
  })

  it('names the topology entry point even for an unrecognised object', () => {
    expect(constructWith({})).toThrow(/client\/topology/)
  })

  it('still accepts a single URL with options', () => {
    const client = new SirannonClient('http://127.0.0.1:9876', { transport: 'http' })
    expect(client.database('orders').id).toBe('orders')
    client.close()
  })

  it('exports no topology surface', () => {
    expect(Object.keys(browserEntry).filter(name => name.toLowerCase().includes('topology'))).toEqual([])
  })

  it('declares the topology entry point in the package exports', () => {
    const manifest = JSON.parse(readFileSync(join(packageRoot, 'package.json'), 'utf8')) as {
      exports: Record<string, { import?: string }>
    }
    expect(manifest.exports['./client/topology']?.import).toBe('./dist/client/topology.mjs')
  })
})

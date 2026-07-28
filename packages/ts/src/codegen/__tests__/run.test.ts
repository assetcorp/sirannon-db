import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { runCodegen } from '../run.js'

const REGISTRY_MODULE = `export const operations = {
  shop: {
    reads: {
      openOrders: {
        args: ['status'],
        fromIdentity: { tenant: 'tenantId' },
        statement: args => ({
          sql: 'SELECT id, reference FROM orders WHERE tenant_id = ? AND status = ?',
          params: [args.tenant, args.status],
        }),
      },
    },
    writes: {
      cancelOrder: {
        args: ['id'],
        statements: args => ({ sql: 'UPDATE orders SET status = ? WHERE id = ?', params: ['cancelled', args.id] }),
      },
    },
  },
}
`

let workspace: string

beforeEach(() => {
  workspace = mkdtempSync(join(tmpdir(), 'sirannon-codegen-'))
  writeFileSync(join(workspace, 'operations.mjs'), REGISTRY_MODULE)
})

afterEach(() => {
  rmSync(workspace, { recursive: true, force: true })
})

describe('runCodegen', () => {
  it('writes typed references and a manifest from the registry source', async () => {
    const typesPath = join(workspace, 'generated', 'operations.ts')
    const manifestPath = join(workspace, 'generated', 'operations.json')

    await runCodegen(['--registry', join(workspace, 'operations.mjs'), '--out', typesPath, '--manifest', manifestPath])

    const generated = readFileSync(typesPath, 'utf8')
    expect(generated).toContain('export interface ShopOpenOrdersRow {')
    expect(generated).toContain('openOrders')
    expect(generated).toContain('cancelOrder')

    const manifest = JSON.parse(readFileSync(manifestPath, 'utf8')) as {
      version: number
      databases: Record<string, { reads: Record<string, { args: string[]; identityArgs: string[] }> }>
    }
    expect(manifest.version).toBe(1)
    expect(manifest.databases.shop.reads.openOrders).toMatchObject({ args: ['status'], identityArgs: ['tenant'] })
  })

  it('refuses a module that exports no registry, and an unknown argument', async () => {
    writeFileSync(join(workspace, 'empty.mjs'), 'export const unrelated = 1\n')

    await expect(
      runCodegen(['--registry', join(workspace, 'empty.mjs'), '--out', join(workspace, 'out.ts')]),
    ).rejects.toThrow(/exports no operation registry/)

    await expect(runCodegen(['--registry', join(workspace, 'operations.mjs')])).rejects.toThrow(/both required/)
  })
})

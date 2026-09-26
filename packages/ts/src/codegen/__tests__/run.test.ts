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
        columns: ['id', 'reference'],
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

  it('writes the operations that every database shares from their own export', async () => {
    writeFileSync(
      join(workspace, 'shared.mjs'),
      `export const sharedOperations = {
  reads: { openInvoices: { columns: ['id', 'amount'], statement: () => ({ sql: 'SELECT id, amount FROM invoices' }) } },
}
`,
    )
    const typesPath = join(workspace, 'shared.ts')
    const manifestPath = join(workspace, 'shared.json')

    await runCodegen(['--registry', join(workspace, 'shared.mjs'), '--out', typesPath, '--manifest', manifestPath])

    expect(readFileSync(typesPath, 'utf8')).toContain('export interface SharedOpenInvoicesRow {')
    const manifest = JSON.parse(readFileSync(manifestPath, 'utf8')) as {
      databases: Record<string, unknown>
      shared: { reads: Record<string, { columns: string[] }> }
    }
    expect(manifest.databases).toEqual({})
    expect(manifest.shared.reads.openInvoices.columns).toEqual(['id', 'amount'])
  })

  it('refuses a shared export name the module does not have', async () => {
    await expect(
      runCodegen([
        '--registry',
        join(workspace, 'operations.mjs'),
        '--out',
        join(workspace, 'out.ts'),
        '--shared-export',
        'everyTenant',
      ]),
    ).rejects.toThrow(/exports no shared operations named 'everyTenant'/)
  })

  it('refuses a module that exports no registry, and an unknown argument', async () => {
    writeFileSync(join(workspace, 'empty.mjs'), 'export const unrelated = 1\n')

    await expect(
      runCodegen(['--registry', join(workspace, 'empty.mjs'), '--out', join(workspace, 'out.ts')]),
    ).rejects.toThrow(/exports no operation registry/)

    await expect(runCodegen(['--registry', join(workspace, 'operations.mjs')])).rejects.toThrow(/both required/)
  })

  it('refuses a named export the module does not have rather than reading another one', async () => {
    writeFileSync(
      join(workspace, 'two-exports.mjs'),
      'export const operations = { shop: { reads: {} } }\nexport default { other: { reads: {} } }\n',
    )

    await expect(
      runCodegen([
        '--registry',
        join(workspace, 'two-exports.mjs'),
        '--out',
        join(workspace, 'out.ts'),
        '--export',
        'operatons',
      ]),
    ).rejects.toThrow(/exports no operation registry named 'operatons'/)
  })
})

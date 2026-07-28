import { describe, expect, it } from 'vitest'
import type { OperationArguments, OperationRegistry } from '../../core/operation-registry.js'
import { buildOperationManifest, selectColumns } from '../manifest.js'
import { renderOperationTypes } from '../render.js'

interface Identity {
  tenantId: string
}

const registry: OperationRegistry<Identity> = {
  shop: {
    reads: {
      openOrders: {
        args: ['status'],
        fromIdentity: { tenant: 'tenantId' },
        statement: (args: OperationArguments) => ({
          sql: 'SELECT id, reference, total AS amount FROM orders WHERE tenant_id = ? AND status = ?',
          params: [args.tenant, args.status],
        }),
      },
      everyColumn: {
        statement: () => ({ sql: 'SELECT * FROM orders' }),
      },
    },
    writes: {
      cancelOrder: {
        args: ['id'],
        fromIdentity: { tenant: 'tenantId' },
        statements: (args: OperationArguments) => ({
          sql: 'UPDATE orders SET status = ? WHERE id = ? AND tenant_id = ?',
          params: ['cancelled', args.id, args.tenant],
        }),
      },
    },
  },
}

describe('buildOperationManifest', () => {
  it('records the arguments a caller supplies, those the server fills, and the columns', () => {
    const manifest = buildOperationManifest(registry)

    expect(manifest.databases.shop.reads.openOrders).toEqual({
      args: ['status'],
      identityArgs: ['tenant'],
      columns: ['id', 'reference', 'amount'],
    })
    expect(manifest.databases.shop.writes.cancelOrder).toEqual({
      args: ['id'],
      identityArgs: ['tenant'],
      columns: null,
    })
    expect(manifest.digest).toMatch(/^[0-9a-f]{64}$/)
  })

  it('leaves the row shape open when the select list names no columns', () => {
    expect(buildOperationManifest(registry).databases.shop.reads.everyColumn.columns).toBeNull()
  })

  it('leaves the row shape open when the statement cannot run without real arguments', () => {
    const manifest = buildOperationManifest({
      shop: {
        reads: {
          orderTotal: {
            args: ['id'],
            statement: (args: OperationArguments) => ({
              sql: `SELECT total FROM orders WHERE id = ${(args.id as { value: number }).value}`,
            }),
          },
        },
      },
    })

    expect(manifest.databases.shop.reads.orderTotal.columns).toBeNull()
  })
})

describe('selectColumns', () => {
  it('names aliased and bare columns and refuses a star', () => {
    expect(selectColumns('SELECT id, name AS reference FROM orders')).toEqual(['id', 'reference'])
    expect(selectColumns('SELECT orders.id FROM orders')).toEqual(['id'])
    expect(selectColumns('SELECT id, * FROM orders')).toBeNull()
    expect(selectColumns('SELECT total * 2 FROM orders')).toBeNull()
    expect(selectColumns('UPDATE orders SET status = ?')).toBeNull()
  })
})

describe('renderOperationTypes', () => {
  it('writes a typed reference for every operation', () => {
    const generated = renderOperationTypes(buildOperationManifest(registry))

    expect(generated).toContain("import type { OperationRef } from '@delali/sirannon-db'")
    expect(generated).toContain('export interface ShopOpenOrdersRow {')
    expect(generated).toContain('  amount: unknown')
    expect(generated).toContain(
      '    openOrders: { name: "openOrders" } as OperationRef<{ status: unknown }, ShopOpenOrdersRow>,',
    )
    expect(generated).toContain(
      '    everyColumn: { name: "everyColumn" } as OperationRef<Record<string, never>, Record<string, unknown>>,',
    )
    expect(generated).toContain('    cancelOrder: { name: "cancelOrder" } as OperationRef<{ id: unknown }, never>,')
    expect(generated).not.toContain('tenant')
  })

  it('imports from the module the caller names', () => {
    const generated = renderOperationTypes(buildOperationManifest(registry), { packageName: '../sdk.js' })
    expect(generated).toContain("import type { OperationRef } from '../sdk.js'")
  })
})

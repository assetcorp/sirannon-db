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
        columns: ['id', 'reference', 'amount'],
        statement: (args: OperationArguments) => ({
          sql: 'SELECT id, reference, total AS amount FROM orders WHERE tenant_id = ? AND status = ?',
          params: [args.tenant, args.status],
        }),
      },
      undeclaredWithArgs: {
        args: ['status'],
        statement: () => ({ sql: 'SELECT id, reference FROM orders WHERE status = ?' }),
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

  it('leaves the row shape open when a read takes arguments and declares no columns', () => {
    expect(buildOperationManifest(registry).databases.shop.reads.undeclaredWithArgs.columns).toBeNull()
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

  it('leaves the row shape open when the statement selects different columns per argument', () => {
    const manifest = buildOperationManifest({
      shop: {
        reads: {
          orders: {
            args: ['status'],
            statement: (args: OperationArguments) =>
              args.status === 'open'
                ? { sql: 'SELECT id, reference FROM orders WHERE status = :status' }
                : { sql: 'SELECT id, reference, cancelled_at FROM orders WHERE status = :status' },
          },
        },
      },
    })

    expect(manifest.databases.shop.reads.orders.columns).toBeNull()
  })

  it('takes the declared columns over the statement it never runs', () => {
    const manifest = buildOperationManifest({
      shop: {
        reads: {
          orders: {
            columns: ['id', 'reference'],
            statement: () => {
              throw new Error('code generation must not run this statement')
            },
          },
        },
      },
    })

    expect(manifest.databases.shop.reads.orders.columns).toEqual(['id', 'reference'])
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

  it('names both operations when two of them generate one identifier', () => {
    const colliding = buildOperationManifest({
      shop: {
        reads: {
          'list-orders': { statement: () => ({ sql: 'SELECT id FROM orders' }) },
          list_orders: { statement: () => ({ sql: 'SELECT id FROM orders' }) },
        },
      },
    })

    expect(() => renderOperationTypes(colliding)).toThrow(
      /shop.reads.list-orders.*shop.reads.list_orders|both generate/,
    )
  })

  it('names both databases when two of them generate one identifier', () => {
    const colliding = buildOperationManifest({
      'my-db': { reads: {} },
      my_db: { reads: {} },
    })

    expect(() => renderOperationTypes(colliding)).toThrow(/both generate the identifier 'my_db'/)
  })

  it('imports from the module the caller names', () => {
    const generated = renderOperationTypes(buildOperationManifest(registry), { packageName: '../sdk.js' })
    expect(generated).toContain("import type { OperationRef } from '../sdk.js'")
  })
})

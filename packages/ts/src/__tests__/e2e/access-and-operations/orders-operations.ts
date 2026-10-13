import { RequestDeniedError } from '../../../core/errors.js'
import type { OperationArguments, OperationRegistry } from '../../../core/operation-registry.js'
import type { AuthenticateHook } from '../../../core/types.js'

export interface Tenant {
  tenant?: string
}

export const LEAKED_DETAIL = '/var/lib/orders/secret.key'
export const SPACED_NAME = 'orders of customer'
export const ENCODED_NAME = 'café/ordres'

function ordersOfCustomer(args: OperationArguments) {
  return { sql: 'SELECT id, customer, total FROM orders WHERE customer = ? ORDER BY id', params: [args.customer] }
}

export const ordersOperations: OperationRegistry<Tenant> = {
  orders: {
    reads: {
      ordersOfCustomer: { args: ['customer'], statement: ordersOfCustomer },
      [SPACED_NAME]: { args: ['customer'], statement: ordersOfCustomer },
      [ENCODED_NAME]: { args: ['customer'], statement: ordersOfCustomer },
      ordersOfTenant: {
        fromIdentity: { tenant: 'tenant' },
        statement: args => ({
          sql: 'SELECT id, customer, total FROM orders WHERE tenant = ? ORDER BY id',
          params: [args.tenant],
        }),
      },
      ordersOfEither: {
        args: ['constructor', 'toString'],
        statement: args => ({
          sql: 'SELECT id, customer, total FROM orders WHERE customer IN (?, ?) ORDER BY id',
          params: [args.constructor, args.toString],
        }),
      },
      everyOrder: { statement: () => ({ sql: 'SELECT id, customer, total, tenant FROM orders ORDER BY id' }) },
      refusingRead: {
        statement: () => {
          throw new RequestDeniedError(403, 'NOT_TODAY', 'This read is closed today')
        },
      },
      brokenRead: {
        statement: () => {
          throw new Error(`could not open ${LEAKED_DETAIL}`)
        },
      },
    },
    writes: {
      placeOrder: {
        args: ['customer', 'total'],
        fromIdentity: { tenant: 'tenant' },
        statements: args => ({
          sql: 'INSERT INTO orders (customer, total, tenant) VALUES (?, ?, ?)',
          params: [args.customer, args.total, args.tenant],
        }),
      },
      transfer: {
        args: ['from', 'to', 'amount'],
        statements: args => [
          { sql: 'UPDATE orders SET total = total + ? WHERE customer = ?', params: [args.amount, args.to] },
          { sql: 'UPDATE orders SET total = total - ? WHERE customer = ?', params: [args.amount, args.from] },
        ],
      },
      emptyWrite: { statements: () => [] },
    },
  },
}

export const tenantFromHeaders: AuthenticateHook<Tenant> = ctx => {
  if (ctx.headers['x-anonymous'] === 'yes') return undefined
  if (ctx.headers['x-no-tenant'] === 'yes') return {}
  return { tenant: ctx.headers['x-tenant'] ?? 'acme' }
}

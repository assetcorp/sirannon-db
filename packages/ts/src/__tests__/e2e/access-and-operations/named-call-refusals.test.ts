import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { errorCodeOf, errorMessageOf, scratchDirectory, sendRaw, trackedRegistry } from '../lib/served-registry.js'
import { ENCODED_NAME, ordersOperations, SPACED_NAME, type Tenant, tenantFromHeaders } from './orders-operations.js'
import {
  openOrders,
  openSocketWithHeaders,
  releaseAccessServers,
  SEEDED_ORDERS,
  serveTyped,
  storedOrders,
} from './served-access.js'

const PROTOTYPE_NAMES = [
  '__proto__',
  'constructor',
  'toString',
  'hasOwnProperty',
  'valueOf',
  '__defineGetter__',
  'isPrototypeOf',
]

let dir: string

beforeEach(() => {
  dir = scratchDirectory('named-call-refusals')
})

afterEach(releaseAccessServers)

function ordersOf(...customers: string[]) {
  return SEEDED_ORDERS.filter(order => customers.includes(order.customer)).map(order => ({
    id: order.id,
    customer: order.customer,
    total: order.total,
  }))
}

describe('named calls built to break the server', () => {
  it('decodes a percent-encoded name once before matching it, and refuses every name that no set registers', async () => {
    const sirannon = trackedRegistry()
    const { path } = await openOrders(sirannon, dir)
    const served = await serveTyped<Tenant>(sirannon, { operations: ordersOperations, authenticate: tenantFromHeaders })
    const body = JSON.stringify({ args: { customer: 'kofi' } })
    const found = [
      encodeURIComponent(SPACED_NAME),
      encodeURIComponent(ENCODED_NAME),
      encodeURIComponent(ENCODED_NAME).toLowerCase(),
      'orders%20of%20customer',
      '%6F%72%64%65%72%73%4F%66%43%75%73%74%6F%6D%65%72',
    ]
    const unknown = [
      'orders%2520of%2520customer',
      'ordersofcustomer',
      'ORDERSOFCUSTOMER',
      'ordersOfCustomer%00',
      'ordersOfCustomer%20',
      '%20ordersOfCustomer',
      'ordersOfCustomer%2F..',
      '..%2F..%2Fhealth',
      'caf%C3%A9',
      'x'.repeat(2_000),
      ...PROTOTYPE_NAMES.map(name => encodeURIComponent(name)),
    ]
    const malformed = ['orders%ZZ', '%', 'orders%2', '%E0%A4%A', '%C3%28', '%FF']

    for (const name of found) {
      const answer = await sendRaw(served.base, 'POST', `/db/orders/query/${name}`, body)
      expect([answer.status, answer.body], name).toEqual([200, { rows: ordersOf('kofi') }])
    }
    for (const name of unknown) {
      for (const route of ['query', 'execute']) {
        const answer = await sendRaw(served.base, 'POST', `/db/orders/${route}/${name}`, body)
        expect([answer.status, errorCodeOf(answer.body)], `${route} ${name.slice(0, 40)}`).toEqual([
          404,
          'UNKNOWN_QUERY',
        ])
      }
    }
    for (const name of malformed) {
      const answer = await sendRaw(served.base, 'POST', `/db/orders/query/${name}`, body)
      expect([answer.status, errorCodeOf(answer.body)], name).toEqual([400, 'INVALID_REQUEST'])
    }
    const oversized = await sendRaw(served.base, 'POST', `/db/orders/query/${'x'.repeat(64_000)}`, body)
    expect(oversized.status).toBe(431)
    const readAsWrite = await sendRaw(served.base, 'POST', '/db/orders/execute/ordersOfCustomer', body)
    const writeAsRead = await sendRaw(
      served.base,
      'POST',
      '/db/orders/query/placeOrder',
      JSON.stringify({ args: { customer: 'ghost', total: 1 } }),
    )
    expect([readAsWrite.status, errorCodeOf(readAsWrite.body)]).toEqual([404, 'UNKNOWN_QUERY'])
    expect([writeAsRead.status, errorCodeOf(writeAsRead.body)]).toEqual([404, 'UNKNOWN_QUERY'])

    const socket = await openSocketWithHeaders(`${served.wsBase}/db/orders`, {})
    for (const [index, name] of [...PROTOTYPE_NAMES, '', 'ordersOfCustomer ', SPACED_NAME.toUpperCase()].entries()) {
      for (const type of ['query', 'execute']) {
        const reply = await socket.request({ id: `${type}-${index}`, type, name, args: { customer: 'kofi' } })
        expect([reply.type, reply.error?.code], `${type} ${JSON.stringify(name)}`).toEqual(['error', 'UNKNOWN_QUERY'])
      }
    }
    const spaced = await socket.request({ id: 'spaced', type: 'query', name: SPACED_NAME, args: { customer: 'esi' } })
    expect(spaced).toMatchObject({ type: 'result', data: { rows: ordersOf('esi') } })

    expect(storedOrders(path)).toEqual(SEEDED_ORDERS)
    expect(Object.hasOwn(Object.prototype, 'customer')).toBe(false)
  })

  it('refuses a missing, undeclared, or identity-filled argument and every malformed body, treating inherited member names as absent', async () => {
    const sirannon = trackedRegistry()
    const { path } = await openOrders(sirannon, dir)
    const served = await serveTyped<Tenant>(sirannon, { operations: ordersOperations, authenticate: tenantFromHeaders })
    const socket = await openSocketWithHeaders(`${served.wsBase}/db/orders`, {})
    const anonymous = await openSocketWithHeaders(`${served.wsBase}/db/orders`, { 'x-anonymous': 'yes' })
    const tenantless = await openSocketWithHeaders(`${served.wsBase}/db/orders`, { 'x-no-tenant': 'yes' })
    const refusals: Array<[label: string, name: string, body: string, status: number, code: string]> = [
      ['no arguments', 'ordersOfCustomer', '{}', 400, 'MISSING_ARGUMENT'],
      ['an empty argument object', 'ordersOfCustomer', '{"args":{}}', 400, 'MISSING_ARGUMENT'],
      [
        'an undeclared argument',
        'ordersOfCustomer',
        '{"args":{"customer":"ama","limit":5}}',
        400,
        'ARGUMENT_NOT_ALLOWED',
      ],
      [
        'an argument named __proto__',
        'ordersOfCustomer',
        '{"args":{"customer":"ama","__proto__":{"customer":"kofi"}}}',
        400,
        'ARGUMENT_NOT_ALLOWED',
      ],
      [
        'an argument hidden under __proto__',
        'ordersOfCustomer',
        '{"args":{"__proto__":{"customer":"ama"}}}',
        400,
        'ARGUMENT_NOT_ALLOWED',
      ],
      ['inherited names left out', 'ordersOfEither', '{"args":{}}', 400, 'MISSING_ARGUMENT'],
      ['one inherited name left out', 'ordersOfEither', '{"args":{"constructor":"ama"}}', 400, 'MISSING_ARGUMENT'],
      ['an identity argument', 'ordersOfTenant', '{"args":{"tenant":"globex"}}', 400, 'ARGUMENT_NOT_ALLOWED'],
      ['an identity argument as null', 'ordersOfTenant', '{"args":{"tenant":null}}', 400, 'ARGUMENT_NOT_ALLOWED'],
      ['arguments as a list', 'ordersOfCustomer', '{"args":["ama"]}', 400, 'INVALID_REQUEST'],
      ['arguments as a string', 'ordersOfCustomer', '{"args":"ama"}', 400, 'INVALID_REQUEST'],
      ['arguments as a number', 'ordersOfCustomer', '{"args":5}', 400, 'INVALID_REQUEST'],
      ['arguments as null', 'ordersOfCustomer', '{"args":null}', 400, 'INVALID_REQUEST'],
      [
        'a malformed integer envelope',
        'ordersOfCustomer',
        '{"args":{"customer":{"__sirannon_int":"12x"}}}',
        400,
        'INVALID_REQUEST',
      ],
      [
        'a malformed blob envelope',
        'ordersOfCustomer',
        '{"args":{"customer":{"__sirannon_blob":"XYZ"}}}',
        400,
        'INVALID_REQUEST',
      ],
      [
        'an unknown read concern',
        'ordersOfCustomer',
        '{"args":{"customer":"ama"},"readConcern":{"level":"strong"}}',
        400,
        'INVALID_REQUEST',
      ],
    ]
    const bodies: Array<[label: string, body: string, code: string]> = [
      ['a list body', '[]', 'INVALID_REQUEST'],
      ['a list holding arguments', '[{"args":{"customer":"ama"}}]', 'INVALID_REQUEST'],
      ['a string body', '"ama"', 'INVALID_REQUEST'],
      ['a null body', 'null', 'INVALID_REQUEST'],
      ['a number body', '5', 'INVALID_REQUEST'],
      ['truncated JSON', '{"args":{"customer":"ama"}', 'INVALID_JSON'],
      ['plain text', 'customer=ama', 'INVALID_JSON'],
    ]

    for (const [index, [label, name, body, status, code]] of refusals.entries()) {
      const answer = await sendRaw(served.base, 'POST', `/db/orders/query/${name}`, body)
      const reply = await socket.request({ id: `refusal-${index}`, type: 'query', name, ...JSON.parse(body) })
      const wsCode = code === 'INVALID_REQUEST' ? 'INVALID_MESSAGE' : code
      expect([answer.status, errorCodeOf(answer.body)], label).toEqual([status, code])
      expect([reply.type, reply.error?.code], label).toEqual(['error', wsCode])
    }
    for (const [label, body, code] of bodies) {
      for (const route of ['query/ordersOfCustomer', 'execute/placeOrder']) {
        const answer = await sendRaw(served.base, 'POST', `/db/orders/${route}`, body)
        expect([answer.status, errorCodeOf(answer.body)], `${label} on ${route}`).toEqual([400, code])
      }
    }
    const identityRefusal = await sendRaw(
      served.base,
      'POST',
      '/db/orders/query/ordersOfTenant',
      '{"args":{"tenant":"globex"}}',
    )
    expect(errorMessageOf(identityRefusal.body)).toMatch(/identity/)
    const inheritedRefusal = await sendRaw(
      served.base,
      'POST',
      '/db/orders/query/ordersOfTenant',
      '{"args":{"toString":"x"}}',
    )
    expect([inheritedRefusal.status, errorCodeOf(inheritedRefusal.body)]).toEqual([400, 'ARGUMENT_NOT_ALLOWED'])
    expect(errorMessageOf(inheritedRefusal.body)).toMatch(/not declared/)

    const noBody = await sendRaw(served.base, 'POST', '/db/orders/query/ordersOfCustomer')
    const emptyBody = await sendRaw(served.base, 'POST', '/db/orders/query/everyOrder')
    const inherited = await sendRaw(
      served.base,
      'POST',
      '/db/orders/query/ordersOfEither',
      '{"args":{"constructor":"ama","toString":"esi"}}',
    )
    expect([noBody.status, errorCodeOf(noBody.body)]).toEqual([400, 'MISSING_ARGUMENT'])
    expect([emptyBody.status, emptyBody.body]).toEqual([200, { rows: SEEDED_ORDERS }])
    expect([inherited.status, inherited.body]).toEqual([200, { rows: ordersOf('ama', 'esi') }])

    const tenantRows = await sendRaw(served.base, 'POST', '/db/orders/query/ordersOfTenant', undefined, {
      'x-tenant': 'globex',
    })
    expect(tenantRows.body).toEqual({ rows: ordersOf('kofi') })
    const withoutTenant: Array<Record<string, string>> = [{ 'x-anonymous': 'yes' }, { 'x-no-tenant': 'yes' }]
    for (const headers of withoutTenant) {
      const answer = await sendRaw(served.base, 'POST', '/db/orders/query/ordersOfTenant', undefined, headers)
      expect([answer.status, errorCodeOf(answer.body)], JSON.stringify(headers)).toEqual([401, 'IDENTITY_REQUIRED'])
      const write = await sendRaw(
        served.base,
        'POST',
        '/db/orders/execute/placeOrder',
        '{"args":{"customer":"ghost","total":1}}',
        headers,
      )
      expect([write.status, errorCodeOf(write.body)], JSON.stringify(headers)).toEqual([401, 'IDENTITY_REQUIRED'])
    }
    for (const [label, connection] of [
      ['anonymous', anonymous],
      ['tenantless', tenantless],
    ] as const) {
      const reply = await connection.request({ id: 'tenant', type: 'query', name: 'ordersOfTenant' })
      expect([reply.type, reply.error?.code], label).toEqual(['error', 'IDENTITY_REQUIRED'])
    }

    expect(storedOrders(path)).toEqual(SEEDED_ORDERS)
    expect(({} as Record<string, unknown>).customer).toBeUndefined()
    const after = await sendRaw(served.base, 'POST', '/db/orders/query/ordersOfCustomer', '{"args":{"customer":"ama"}}')
    expect(after.body).toEqual({ rows: ordersOf('ama') })
  })
})

import { existsSync, mkdirSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { RequestDeniedError } from '../../../core/errors.js'
import { errorCodeOf, httpRequestText, scratchDirectory, trackedRegistry } from '../lib/served-registry.js'
import { rawHttp, rawUpgrade, releaseAccessServers, serveTyped } from './served-access.js'

const ESCAPED_FILE = 'escaped.db'
const TICKET = 'x-ticket'
const OPEN_TICKET = 'open sesame'
const QUERY_BODY = '{"sql":"SELECT 1 AS one"}'
const ESCAPING_IDENTIFIERS = [
  '..%2Fescaped',
  '..%2F..%2Fescaped',
  '..%2f..%2fescaped',
  '%2E%2E',
  '%2e',
  '.',
  '..',
  'tenants%5C..%5Cescaped',
  'tenants\\..\\escaped',
  'north%2F..%2F..%2Fescaped',
  '%2Fescaped',
  '%5Cescaped',
  'C%3A%5Cescaped',
  'c:escaped',
]

let scratch: string
let tenants: string

beforeEach(() => {
  scratch = scratchDirectory('path-identifiers')
  tenants = join(scratch, 'tenants')
  mkdirSync(tenants)
})

afterEach(async () => {
  await releaseAccessServers()
})

function recordingRegistry(seen: string[]) {
  return trackedRegistry({
    lifecycle: {
      autoOpen: {
        resolver: id => {
          seen.push(id)
          return { path: join(tenants, `${id}.db`) }
        },
      },
    },
  })
}

function queryRequest(id: string, headers: Record<string, string> = {}): string {
  return httpRequestText('POST', `/db/${id}/query`, QUERY_BODY, headers)
}

describe('a database identifier in the request path', () => {
  it('is refused with INVALID_REQUEST when it decodes to a rooted path or holds a dot segment, so a resolver that joins it onto a directory never sees it, while a nested identifier and other encoded characters still decode', async () => {
    const seen: string[] = []
    mkdirSync(join(tenants, 'east'))
    const served = await serveTyped(recordingRegistry(seen), { acceptSql: true })

    const answers = []
    for (const id of ESCAPING_IDENTIFIERS) {
      const answer = await rawHttp(served.port, queryRequest(id))
      answers.push([id, answer.status, errorCodeOf(answer.body)])
    }
    const upgrade = await rawUpgrade(served.port, '/db/..%2F..%2Fescaped', {})
    const spaced = await rawHttp(served.port, queryRequest('north%20wing'))
    const nested = await rawHttp(served.port, queryRequest('east%2Fwing'))

    expect(answers).toEqual(ESCAPING_IDENTIFIERS.map(id => [id, 400, 'INVALID_REQUEST']))
    expect(upgrade.statusLine).toMatch(/^HTTP\/1\.1 400/)
    expect([spaced.status, spaced.body]).toEqual([200, { rows: [{ one: 1 }] }])
    expect([nested.status, nested.body]).toEqual([200, { rows: [{ one: 1 }] }])
    expect(seen).toEqual(['north wing', 'east/wing'])
    expect(readdirSync(tenants).filter(name => name.endsWith('.db'))).toEqual(['north wing.db'])
    expect(readdirSync(join(tenants, 'east'))).toContain('wing.db')
    expect(existsSync(join(scratch, ESCAPED_FILE))).toBe(false)
    expect(existsSync(join(scratch, '..', ESCAPED_FILE))).toBe(false)
  })

  it('reaches the authenticate hook as sent, and is refused only once the hook accepts the request', async () => {
    const seen: string[] = []
    const hooked: Array<string | undefined> = []
    const served = await serveTyped(recordingRegistry(seen), {
      acceptSql: true,
      authenticate: ctx => {
        hooked.push(ctx.databaseId)
        if (ctx.headers[TICKET] !== OPEN_TICKET) throw new RequestDeniedError(401, 'TICKET_REFUSED', 'No ticket')
        return {}
      },
    })

    const refused = await rawHttp(served.port, queryRequest('..%2Fescaped'))
    const accepted = await rawHttp(served.port, queryRequest('..%2Fescaped', { [TICKET]: OPEN_TICKET }))
    const refusedUpgrade = await rawUpgrade(served.port, '/db/%2E%2E', {})
    const acceptedUpgrade = await rawUpgrade(served.port, '/db/%2E%2E', { [TICKET]: OPEN_TICKET })

    expect([refused.status, errorCodeOf(refused.body)]).toEqual([401, 'TICKET_REFUSED'])
    expect([accepted.status, errorCodeOf(accepted.body)]).toEqual([400, 'INVALID_REQUEST'])
    expect(refusedUpgrade.statusLine).toMatch(/^HTTP\/1\.1 101/)
    expect(acceptedUpgrade.statusLine).toMatch(/^HTTP\/1\.1 400/)
    expect(hooked).toEqual(['..%2Fescaped', '..%2Fescaped', '%2E%2E', '%2E%2E'])
    expect(seen).toEqual([])
    expect(existsSync(join(scratch, ESCAPED_FILE))).toBe(false)
  })
})

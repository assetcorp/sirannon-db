import { existsSync, readFileSync } from 'node:fs'
import { pathToFileURL } from 'node:url'
import ts from 'typescript'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { runCodegen } from '../../../codegen/run.js'
import { type CodegenWorkspace, codegenWorkspace } from './workspace.js'

const COMPILE_TIMEOUT_MS = 60_000

const SHOP_REGISTRY = `import { writeFileSync } from 'node:fs'

export const operations = {
  shop: {
    reads: {
      openOrders: {
        args: ['status'],
        fromIdentity: { tenant: 'tenantId' },
        columns: ['id', 'reference', 'amount'],
        statement: args => ({ sql: 'SELECT id, reference, total AS amount FROM orders WHERE tenant_id = ? AND status = ?', params: [args.tenant, args.status] }),
      },
      latestOrders: { statement: () => ({ sql: 'SELECT id, reference AS ref, orders.placed_at FROM orders ORDER BY id DESC' }) },
      everyColumn: { statement: () => ({ sql: 'SELECT * FROM orders' }) },
      doubled: { statement: () => ({ sql: 'SELECT total * 2 FROM orders' }) },
      repeated: { statement: () => ({ sql: 'SELECT id, orders.id, reference FROM orders' }) },
      mutating: { statement: () => ({ sql: 'UPDATE orders SET status = 1 RETURNING id' }) },
      byStatus: { args: ['status'], statement: () => ({ sql: 'SELECT id, reference FROM orders WHERE status = ?' }) },
      failing: { statement: () => { throw new Error('needs a live connection') } },
      declared: {
        columns: ['id', 'reference'],
        statement: () => {
          writeFileSync(process.env.SIRANNON_CODEGEN_MARKER ?? '/dev/null', 'ran')
          return { sql: 'SELECT 1' }
        },
      },
    },
    writes: {
      cancelOrder: {
        args: ['id'],
        fromIdentity: { tenant: 'tenantId' },
        statements: args => ({ sql: 'UPDATE orders SET status = ? WHERE id = ? AND tenant_id = ?', params: ['cancelled', args.id, args.tenant] }),
      },
      reopenOrder: { args: ['id', 'reason'], statements: () => [{ sql: 'SELECT 1' }, { sql: 'SELECT 2' }] },
    },
  },
}

export const sharedOperations = {
  reads: { openInvoices: { statement: () => ({ sql: 'SELECT id, amount FROM invoices WHERE paid = 0' }) } },
  writes: { payInvoice: { args: ['id'], statements: args => ({ sql: 'UPDATE invoices SET paid = 1 WHERE id = ?', params: [args.id] }) } },
}
`

const HOSTILE_NAMES = {
  databases: ['default', 'class', 'await', '2024', 'café', 'registryDigest', 'OperationRef', 'with space', ''],
  operations: [
    'delete',
    'new',
    'list orders',
    '"quoted"',
    "it's",
    '9lives',
    'naïve',
    'line\nbreak',
    'back\\slash',
    'constructor',
  ],
}

let workspace: CodegenWorkspace

beforeEach(() => {
  workspace = codegenWorkspace()
})

afterEach(() => {
  workspace.dispose()
  delete process.env.SIRANNON_CODEGEN_MARKER
})

async function generate(module: string, extra: string[] = []): Promise<string> {
  const registry = workspace.write(`registry-${Math.random().toString(16).slice(2)}.mjs`, module)
  await runCodegen(['--registry', registry, '--out', workspace.path('generated.ts'), ...extra])
  return readFileSync(workspace.path('generated.ts'), 'utf8')
}

async function importGenerated(source: string): Promise<Record<string, unknown>> {
  const javascript = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  }).outputText
  const path = workspace.write(`generated-${Math.random().toString(16).slice(2)}.mjs`, javascript)
  return (await import(pathToFileURL(path).href)) as Record<string, unknown>
}

function hostileRegistry(): string {
  const read = "{ statement: () => ({ sql: 'SELECT 1 AS one' }) }"
  const databases = HOSTILE_NAMES.databases.map(
    id =>
      `  ${JSON.stringify(id)}: { reads: { ${HOSTILE_NAMES.operations.map(name => `${JSON.stringify(name)}: ${read}`).join(', ')} }, writes: { ${HOSTILE_NAMES.operations.map(name => `${JSON.stringify(name)}: { args: [${JSON.stringify(name)}], statements: () => ({ sql: 'SELECT 1' }) }`).join(', ')} } },`,
  )
  return `export const operations = {\n${databases.join('\n')}\n}\n`
}

describe('typed references generated from a registry module', () => {
  it('writes a manifest that records the arguments that a caller supplies, those that the identity fills, and the columns of each read, without calling a statement that declares its columns', async () => {
    const marker = workspace.path('statement-ran')
    process.env.SIRANNON_CODEGEN_MARKER = marker
    await generate(SHOP_REGISTRY, ['--manifest', workspace.path('nested/dir/manifest.json')])
    const manifest = JSON.parse(readFileSync(workspace.path('nested/dir/manifest.json'), 'utf8'))

    expect(manifest).toEqual({
      version: expect.any(Number),
      digest: expect.any(String),
      databases: {
        shop: {
          reads: {
            byStatus: { args: ['status'], identityArgs: [], columns: null },
            declared: { args: [], identityArgs: [], columns: ['id', 'reference'] },
            doubled: { args: [], identityArgs: [], columns: null },
            everyColumn: { args: [], identityArgs: [], columns: null },
            failing: { args: [], identityArgs: [], columns: null },
            latestOrders: { args: [], identityArgs: [], columns: ['id', 'ref', 'placed_at'] },
            mutating: { args: [], identityArgs: [], columns: null },
            openOrders: { args: ['status'], identityArgs: ['tenant'], columns: ['id', 'reference', 'amount'] },
            repeated: { args: [], identityArgs: [], columns: ['id', 'id', 'reference'] },
          },
          writes: {
            cancelOrder: { args: ['id'], identityArgs: ['tenant'], columns: null },
            reopenOrder: { args: ['id', 'reason'], identityArgs: [], columns: null },
          },
        },
      },
      shared: {
        reads: { openInvoices: { args: [], identityArgs: [], columns: ['id', 'amount'] } },
        writes: { payInvoice: { args: ['id'], identityArgs: [], columns: null } },
      },
    })
    expect(existsSync(marker)).toBe(false)
  })

  it(
    'renders references that the TypeScript compiler accepts against the published client, and rejects a caller that reads an undeclared column or sends an undeclared or identity argument',
    async () => {
      const generated = await generate(SHOP_REGISTRY)
      workspace.write('generated.ts', generated)
      const preamble = [
        "import { SirannonClient } from '@delali/sirannon-db/client'",
        "import { registryDigest, sharedOperations, shop, type ShopLatestOrdersRow, type ShopOpenOrdersRow } from './generated.js'",
        "const db = new SirannonClient('http://127.0.0.1:1').database('shop')",
        'export async function use(): Promise<void> {',
      ]
      const valid = [
        "  const open: ShopOpenOrdersRow[] = await db.query(shop.reads.openOrders, { status: 'open' })",
        '  const first: unknown = open[0]?.reference',
        '  const latest: ShopLatestOrdersRow[] = await db.query(shop.reads.latestOrders, {})',
        '  const placed: unknown = latest[0]?.placed_at',
        '  const anyRow: Record<string, unknown>[] = await db.query(shop.reads.everyColumn, {})',
        '  const repeated: Array<{ id: unknown; reference: unknown }> = await db.query(shop.reads.repeated, {})',
        '  const results = await db.execute(shop.writes.cancelOrder, { id: 7 })',
        '  const changed: number | undefined = results[0]?.changes',
        "  await db.execute(shop.writes.reopenOrder, { id: 7, reason: 'customer asked' })",
        '  const invoices = await db.query(sharedOperations.reads.openInvoices, {})',
        '  const amount: unknown = invoices[0]?.amount',
        '  await db.execute(sharedOperations.writes.payInvoice, { id: 3 })',
        '  const digest: string | null = registryDigest',
        '  void [first, placed, anyRow, repeated, changed, amount, digest]',
      ]
      const misuses = [
        '  const missing = (await db.query(shop.reads.openOrders, { status: "open" }))[0]?.total',
        "  await db.query(shop.reads.openOrders, { status: 'open', limit: 5 })",
        "  await db.query(shop.reads.openOrders, { status: 'open', tenant: 'globex' })",
        "  await db.execute(shop.writes.cancelOrder, { id: 7, tenant: 'globex' })",
        '  const typed: number = (await db.query(shop.reads.openOrders, { status: "open" }))[0]?.amount',
        '  const ref: string = shop.reads.openOrders.name.missing',
      ]
      workspace.write('valid.ts', [...preamble, ...valid, '}', ''].join('\n'))
      workspace.write('misuse.ts', [...preamble, ...misuses, '  void [missing, typed, ref]', '}', ''].join('\n'))

      const diagnostics = workspace.compile(['generated.ts', 'valid.ts', 'misuse.ts'])

      expect(diagnostics.filter(found => found.file !== 'misuse.ts')).toEqual([])
      const misuseLines = misuses.map((_, at) => preamble.length + at + 1)
      const flagged = new Set(diagnostics.filter(found => found.file === 'misuse.ts').map(found => found.line))
      expect([...flagged].sort((left, right) => left - right)).toEqual(misuseLines)
    },
    COMPILE_TIMEOUT_MS,
  )

  it(
    'renders a file that compiles, and whose references carry the exact registered names, for database and operation names that are reserved words or not identifiers',
    async () => {
      const generated = await generate(hostileRegistry(), ['--manifest', workspace.path('manifest.json')])
      workspace.write('generated.ts', generated)

      const diagnostics = workspace.compile(['generated.ts'])
      const loaded = await importGenerated(generated)
      const manifest = JSON.parse(readFileSync(workspace.path('manifest.json'), 'utf8'))
      const references = Object.entries(loaded).filter(([name]) => name !== 'registryDigest')

      expect(diagnostics).toEqual([])
      expect(loaded.registryDigest).toBe(manifest.digest)
      expect(references).toHaveLength(HOSTILE_NAMES.databases.length)
      const names = references
        .map(([, value]) => {
          const set = value as { reads: Record<string, { name: string }>; writes: Record<string, { name: string }> }
          return [
            Object.entries(set.reads).map(([key, ref]) => [key, ref.name]),
            Object.entries(set.writes).map(([key, ref]) => [key, ref.name]),
          ]
        })
        .flat(2)
      for (const [key, name] of names) expect(key, JSON.stringify(name)).toBe(name)
      expect(names).toHaveLength(HOSTILE_NAMES.databases.length * HOSTILE_NAMES.operations.length * 2)
    },
    COMPILE_TIMEOUT_MS,
  )

  it('refuses to render two names that generate one identifier, names both, and writes no file', async () => {
    const read = "{ statement: () => ({ sql: 'SELECT id FROM orders' }) }"
    const collisions: Array<[label: string, module: string, first: string, second: string]> = [
      [
        'two reads',
        `export const operations = { shop: { reads: { 'list-orders': ${read}, list_orders: ${read} } } }`,
        'shop.reads.list-orders',
        'shop.reads.list_orders',
      ],
      ['two databases', `export const operations = { 'my-db': {}, my_db: {} }`, 'my-db', 'my_db'],
      [
        'a database and the shared set',
        `export const operations = { sharedOperations: {} }\nexport const sharedOperations = { reads: {} }`,
        'sharedOperations',
        'sharedOperations',
      ],
      [
        'a reserved word and its escape',
        `export const operations = { default: {}, _default: {} }`,
        'default',
        '_default',
      ],
    ]

    for (const [label, module, first, second] of collisions) {
      const out = workspace.path(`${label.replaceAll(' ', '-')}.ts`)
      const registry = workspace.write(`${label.replaceAll(' ', '-')}.mjs`, module)
      let thrown: unknown
      try {
        await runCodegen(['--registry', registry, '--out', out])
      } catch (err) {
        thrown = err
      }
      expect(thrown, label).toBeInstanceOf(Error)
      expect((thrown as Error).message, label).toContain(`'${first}'`)
      expect((thrown as Error).message, label).toContain(`'${second}'`)
      expect((thrown as Error).message, label).toMatch(/both generate/)
      expect(existsSync(out), label).toBe(false)
    }
  })
})

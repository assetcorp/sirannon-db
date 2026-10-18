import { spawn } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { CLI_ENTRY, type CodegenWorkspace, codegenWorkspace, PACKAGE_ROOT } from './workspace.js'

const CLI_TIMEOUT_MS = 60_000

const REGISTRY = `export const operations = {
  shop: {
    reads: { openOrders: { args: ['status'], columns: ['id', 'total'], statement: args => ({ sql: 'SELECT id, total FROM orders WHERE status = ?', params: [args.status] }) } },
  },
}
export const tenantOperations = { reads: { invoices: { columns: ['id'], statement: () => ({ sql: 'SELECT id FROM invoices' }) } } }
`

interface CliOutcome {
  code: number | null
  stdout: string
  stderr: string
}

function runCli(args: readonly string[]): Promise<CliOutcome> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--import', 'tsx', CLI_ENTRY, ...args], { cwd: PACKAGE_ROOT })
    let stdout = ''
    let stderr = ''
    child.stdout.on('data', chunk => {
      stdout += chunk.toString()
    })
    child.stderr.on('data', chunk => {
      stderr += chunk.toString()
    })
    child.once('error', reject)
    child.once('close', code => resolve({ code, stdout, stderr }))
  })
}

let workspace: CodegenWorkspace

beforeEach(() => {
  workspace = codegenWorkspace()
})

afterEach(() => {
  workspace.dispose()
})

describe('the sirannon-codegen command', () => {
  it(
    'writes references and a manifest that compile against the package that the caller names, and exits with status zero',
    async () => {
      const registry = workspace.write('registry.mjs', REGISTRY)
      workspace.write('sdk.ts', "export type { OperationRef } from '@delali/sirannon-db'\n")
      const out = workspace.path('deep/generated/operations.ts')
      const manifest = workspace.path('deep/generated/operations.json')

      const outcome = await runCli([
        '--registry',
        registry,
        '--out',
        out,
        '--manifest',
        manifest,
        '--shared-export',
        'tenantOperations',
        '--package',
        './sdk.js',
      ])
      workspace.write('generated.ts', readFileSync(out, 'utf8'))
      const withoutSdk = codegenWorkspace()
      withoutSdk.write('generated.ts', readFileSync(out, 'utf8'))
      const unresolved = withoutSdk.compile(['generated.ts'])
      withoutSdk.dispose()

      expect(outcome).toEqual({ code: 0, stdout: '', stderr: '' })
      expect(JSON.parse(readFileSync(manifest, 'utf8'))).toMatchObject({
        databases: {
          shop: { reads: { openOrders: { args: ['status'], identityArgs: [], columns: ['id', 'total'] } } },
        },
        shared: { reads: { invoices: { args: [], identityArgs: [], columns: ['id'] } } },
      })
      expect(workspace.compile(['generated.ts'])).toEqual([])
      expect(unresolved.map(found => found.message).join('\n')).toContain('./sdk.js')
    },
    CLI_TIMEOUT_MS,
  )

  it(
    'exits with status one, names the problem on standard error, and writes nothing for every invocation that it cannot carry out',
    async () => {
      const registry = workspace.write('registry.mjs', REGISTRY)
      const out = workspace.path('never.ts')
      workspace.write('throws.mjs', "throw new Error('the registry module failed to load')\n")
      workspace.write('unrelated.mjs', 'export const unrelated = 1\n')
      workspace.write('scalar.mjs', 'export const operations = 5\n')
      const cases: Array<[label: string, args: string[], message: string | RegExp]> = [
        ['no arguments', [], '--registry and --out are both required'],
        ['no output path', ['--registry', registry], '--registry and --out are both required'],
        ['no registry', ['--out', out], '--registry and --out are both required'],
        [
          'a misspelt flag',
          ['--registry', registry, '--out', out, '--exprot', 'operations'],
          "Unrecognised argument '--exprot'",
        ],
        ['a positional argument', [registry, '--out', out], `Unrecognised argument '${registry}'`],
        ['a flag with no value', ['--out', out, '--registry'], "Unrecognised argument '--registry'"],
        ['a flag written with an equals sign', [`--registry=${registry}`, '--out', out], 'Unrecognised argument'],
        [
          'a registry path that does not exist',
          ['--registry', workspace.path('missing.mjs'), '--out', out],
          /Cannot find module|ERR_MODULE_NOT_FOUND|missing\.mjs/,
        ],
        [
          'a registry module that throws',
          ['--registry', workspace.path('throws.mjs'), '--out', out],
          'the registry module failed to load',
        ],
        [
          'a module with no registry',
          ['--registry', workspace.path('unrelated.mjs'), '--out', out],
          'exports no operation registry',
        ],
        [
          'a registry that is not an object',
          ['--registry', workspace.path('scalar.mjs'), '--out', out],
          'exports no operation registry',
        ],
        [
          'an export name the module lacks',
          ['--registry', registry, '--out', out, '--export', 'operatons'],
          "exports no operation registry named 'operatons'",
        ],
        [
          'a shared export name the module lacks',
          ['--registry', registry, '--out', out, '--shared-export', 'everyTenant'],
          "exports no shared operations named 'everyTenant'",
        ],
      ]

      const outcomes = await Promise.all(cases.map(([, args]) => runCli(args)))

      for (const [index, [label, , message]] of cases.entries()) {
        const outcome = outcomes[index]
        expect(outcome?.code, label).toBe(1)
        expect(outcome?.stdout, label).toBe('')
        if (typeof message === 'string') expect(outcome?.stderr, label).toContain(message)
        else expect(outcome?.stderr, label).toMatch(message)
      }
      expect(outcomes[0]?.stderr).toContain('sirannon-codegen --registry <module> --out <file>')
      expect(existsSync(out)).toBe(false)
    },
    CLI_TIMEOUT_MS,
  )
})

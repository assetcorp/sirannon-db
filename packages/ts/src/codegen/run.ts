import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import type { OperationRegistry } from '../core/operation-registry.js'
import { buildOperationManifest } from './manifest.js'
import { renderOperationTypes } from './render.js'

export const CODEGEN_USAGE = `sirannon-codegen --registry <module> --out <file> [--manifest <file>] [--export <name>] [--package <name>]

Reads the operation registry a server is built from and writes the typed
references a client calls it through. The registry module is imported, so run
it under a loader that reads your source format when it is not JavaScript.`

interface Options {
  registry: string
  out: string
  manifest: string | undefined
  exportName: string
  packageName: string | undefined
}

function parseOptions(argv: readonly string[]): Options {
  const values = new Map<string, string>()
  for (let i = 0; i < argv.length; i += 2) {
    const flag = argv[i]
    const value = argv[i + 1]
    if (!flag.startsWith('--') || value === undefined) {
      throw new Error(`Unrecognised argument '${flag}'\n\n${CODEGEN_USAGE}`)
    }
    values.set(flag.slice(2), value)
  }

  const registry = values.get('registry')
  const out = values.get('out')
  if (registry === undefined || out === undefined) {
    throw new Error(`--registry and --out are both required\n\n${CODEGEN_USAGE}`)
  }

  return {
    registry,
    out,
    manifest: values.get('manifest'),
    exportName: values.get('export') ?? 'operations',
    packageName: values.get('package'),
  }
}

function readRegistry(module: Record<string, unknown>, exportName: string, path: string): OperationRegistry {
  const candidate = module[exportName] ?? module.default
  if (candidate === undefined || typeof candidate !== 'object') {
    throw new Error(`Module '${path}' exports no operation registry named '${exportName}'`)
  }
  return candidate as OperationRegistry
}

function write(path: string, contents: string): void {
  const target = resolve(path)
  mkdirSync(dirname(target), { recursive: true })
  writeFileSync(target, contents)
}

export async function runCodegen(argv: readonly string[]): Promise<void> {
  const options = parseOptions(argv)
  const modulePath = resolve(options.registry)
  const imported = (await import(pathToFileURL(modulePath).href)) as Record<string, unknown>
  const manifest = buildOperationManifest(readRegistry(imported, options.exportName, modulePath))

  write(options.out, renderOperationTypes(manifest, { packageName: options.packageName }))
  if (options.manifest !== undefined) {
    write(options.manifest, `${JSON.stringify(manifest, null, 2)}\n`)
  }
}

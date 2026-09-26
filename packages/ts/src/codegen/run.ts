import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import type { DatabaseOperations, OperationRegistry } from '../core/operation-registry.js'
import { buildOperationManifest } from './manifest.js'
import { renderOperationTypes } from './render.js'

/**
 * The code generator appends this usage text to its error message when it cannot parse the arguments.
 *
 * @public
 */
export const CODEGEN_USAGE = `sirannon-codegen --registry <module> --out <file> [--manifest <file>] [--export <name>] [--shared-export <name>] [--package <name>]

Reads the operations that you register with a server and writes the typed
references through which a client calls them. The generator takes the
per-database registry from the 'operations' export, or from the default
export, and the operations for every database from the 'sharedOperations'
export. The generator imports the registry module, so run it under a loader
that reads your source format when that format is not JavaScript.`

interface Options {
  registry: string
  out: string
  manifest: string | undefined
  exportName: string | undefined
  sharedExportName: string | undefined
  packageName: string | undefined
}

const DEFAULT_SHARED_EXPORT = 'sharedOperations'

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
    exportName: values.get('export'),
    sharedExportName: values.get('shared-export'),
    packageName: values.get('package'),
  }
}

interface RegistryExports {
  registry: OperationRegistry
  shared: DatabaseOperations | undefined
}

function readShared(
  module: Record<string, unknown>,
  exportName: string | undefined,
  path: string,
): DatabaseOperations | undefined {
  const candidate = module[exportName ?? DEFAULT_SHARED_EXPORT]
  if (candidate === undefined && exportName === undefined) return undefined
  if (candidate === undefined || candidate === null || typeof candidate !== 'object') {
    throw new Error(`Module '${path}' exports no shared operations named '${exportName ?? DEFAULT_SHARED_EXPORT}'`)
  }
  return candidate as DatabaseOperations
}

function readRegistry(module: Record<string, unknown>, options: Options, path: string): RegistryExports {
  const shared = readShared(module, options.sharedExportName, path)
  const { exportName } = options
  const candidate = exportName === undefined ? (module.operations ?? module.default) : module[exportName]
  if (candidate === undefined && exportName === undefined && shared !== undefined) return { registry: {}, shared }
  if (candidate === undefined || candidate === null || typeof candidate !== 'object') {
    const named = exportName === undefined ? "'operations' or a default export" : `'${exportName}'`
    throw new Error(`Module '${path}' exports no operation registry named ${named}`)
  }
  return { registry: candidate as OperationRegistry, shared }
}

function write(path: string, contents: string): void {
  const target = resolve(path)
  mkdirSync(dirname(target), { recursive: true })
  writeFileSync(target, contents)
}

/**
 * Writes typed operation references for the registry in the `--registry` module to the `--out` file.
 *
 * When you pass `--manifest`, the generator also writes the operation manifest to that file as JSON.
 *
 * @param argv - The command-line arguments that follow the executable and script names.
 * @throws An `Error` when an argument is missing or unrecognised, when Node cannot import the registry module, when the module exports neither an operation registry nor shared operations, or when the module has no export of the name that `--export` or `--shared-export` gives.
 *
 * @public
 */
export async function runCodegen(argv: readonly string[]): Promise<void> {
  const options = parseOptions(argv)
  const modulePath = resolve(options.registry)
  const imported = (await import(pathToFileURL(modulePath).href)) as Record<string, unknown>
  const { registry, shared } = readRegistry(imported, options, modulePath)
  const manifest = buildOperationManifest(registry, shared)

  write(options.out, renderOperationTypes(manifest, { packageName: options.packageName }))
  if (options.manifest !== undefined) {
    write(options.manifest, `${JSON.stringify(manifest, null, 2)}\n`)
  }
}

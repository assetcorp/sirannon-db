import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import type { DatabaseOperations, OperationRegistry } from '../core/operation-registry.js'
import { buildOperationManifest, type OperationManifest } from './manifest.js'
import { parseOperationManifest } from './manifest-file.js'
import { renderOperationTypes } from './render.js'
import { renderGoOperations } from './render-go.js'
import { renderPythonOperations } from './render-python.js'

/**
 * The code generator appends this usage text to its error message when it cannot parse the arguments.
 *
 * @public
 */
export const CODEGEN_USAGE = `sirannon-codegen (--registry <module> | --from-manifest <file>) [--out <file>] [--python <file>] [--go <file>] [--go-package <name>] [--manifest <file>] [--export <name>] [--shared-export <name>] [--package <name>]

Reads the operations that you register with a server and writes the typed
references through which a client calls them: TypeScript to --out, Python to
--python, and Go to --go, in the package that --go-package gives. The
generator takes the per-database registry from the 'operations' export, or
from the default export, and the operations for every database from the
'sharedOperations' export. The generator imports the registry module, so run
it under a loader that reads your source format when that format is not
JavaScript. --from-manifest reads a manifest that --manifest wrote, so a
Python or Go project can generate its references from a committed file.`

interface Options {
  registry: string | undefined
  fromManifest: string | undefined
  out: string | undefined
  python: string | undefined
  go: string | undefined
  goPackage: string | undefined
  manifest: string | undefined
  exportName: string | undefined
  sharedExportName: string | undefined
  packageName: string | undefined
}

const DEFAULT_SHARED_EXPORT = 'sharedOperations'

const KNOWN_FLAGS = new Set([
  'registry',
  'from-manifest',
  'out',
  'python',
  'go',
  'go-package',
  'manifest',
  'export',
  'shared-export',
  'package',
])

function parseOptions(argv: readonly string[]): Options {
  const values = new Map<string, string>()
  for (let i = 0; i < argv.length; i += 2) {
    const flag = argv[i]
    const value = argv[i + 1]
    if (!flag.startsWith('--') || !KNOWN_FLAGS.has(flag.slice(2)) || value === undefined) {
      throw new Error(`Unrecognised argument '${flag}'\n\n${CODEGEN_USAGE}`)
    }
    values.set(flag.slice(2), value)
  }

  const registry = values.get('registry')
  const fromManifest = values.get('from-manifest')
  if ((registry === undefined) === (fromManifest === undefined)) {
    throw new Error(`Pass exactly one of --registry and --from-manifest\n\n${CODEGEN_USAGE}`)
  }
  const out = values.get('out')
  const python = values.get('python')
  const go = values.get('go')
  if (out === undefined && python === undefined && go === undefined) {
    throw new Error(`Pass at least one of --out, --python, and --go\n\n${CODEGEN_USAGE}`)
  }

  return {
    registry,
    fromManifest,
    out,
    python,
    go,
    goPackage: values.get('go-package'),
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
 * Writes typed operation references for the registry in the `--registry` module, or for the manifest in the
 * `--from-manifest` file, as TypeScript to `--out`, Python to `--python`, and Go to `--go`.
 *
 * When you pass `--manifest`, the generator also writes the operation manifest to that file as JSON. The generator
 * renders every output before it writes any file, so a rendering error leaves every file as it was.
 *
 * @param argv - The command-line arguments that follow the executable and script names.
 * @throws An `Error` when an argument is missing or unrecognised, when Node cannot import the registry module, when the module exports neither an operation registry nor shared operations, when the module has no export of the name that `--export` or `--shared-export` gives, when the manifest file is unreadable or malformed, or when two operations generate the same identifier.
 *
 * @public
 */
export async function runCodegen(argv: readonly string[]): Promise<void> {
  const options = parseOptions(argv)
  const manifest = await loadManifest(options)
  const outputs: Array<[string, string]> = []
  if (options.out !== undefined) {
    outputs.push([options.out, renderOperationTypes(manifest, { packageName: options.packageName })])
  }
  if (options.python !== undefined) outputs.push([options.python, renderPythonOperations(manifest)])
  if (options.go !== undefined) {
    outputs.push([options.go, renderGoOperations(manifest, { packageName: options.goPackage })])
  }
  if (options.manifest !== undefined) outputs.push([options.manifest, `${JSON.stringify(manifest, null, 2)}\n`])
  for (const [path, contents] of outputs) write(path, contents)
}

async function loadManifest(options: Options): Promise<OperationManifest> {
  if (options.fromManifest !== undefined) {
    const path = resolve(options.fromManifest)
    let data: unknown
    try {
      data = JSON.parse(readFileSync(path, 'utf8'))
    } catch (err) {
      throw new Error(
        `The manifest '${path}' is not readable JSON: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
    return parseOperationManifest(data, path)
  }
  const modulePath = resolve(options.registry ?? '')
  const imported = (await import(pathToFileURL(modulePath).href)) as Record<string, unknown>
  const { registry, shared } = readRegistry(imported, options, modulePath)
  return buildOperationManifest(registry, shared)
}

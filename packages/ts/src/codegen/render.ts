import type { DatabaseManifest, OperationManifest, OperationShape } from './manifest.js'

/**
 * Settings for the TypeScript source that {@link renderOperationTypes} returns.
 *
 * @public
 */
export interface RenderOptions {
  /**
   * The package that the generated file imports its types from, which defaults to `@delali/sirannon-db`.
   */
  packageName?: string
}

const DEFAULT_PACKAGE = '@delali/sirannon-db'

/**
 * Returns TypeScript source that declares a typed reference for every operation in a manifest.
 *
 * @param manifest - The operations to render.
 * @param options - The package that the generated file imports its types from.
 * @returns The TypeScript source to write.
 *
 * @public
 */
export function renderOperationTypes(manifest: OperationManifest, options?: RenderOptions): string {
  const lines: string[] = [
    `import type { OperationRef } from '${options?.packageName ?? DEFAULT_PACKAGE}'`,
    '',
    `export const registryDigest = ${JSON.stringify(manifest.digest ?? null)}`,
  ]

  const declared = new Map<string, Claimant>()
  for (const databaseId of Object.keys(manifest.databases).sort()) {
    const database = manifest.databases[databaseId]
    const names = {
      constant: identifier(databaseId),
      rowPrefix: pascalCase(databaseId),
      claimant: { owner: `database/${databaseId}`, label: databaseId },
    }
    lines.push('', ...renderOperations(names, database, declared))
  }
  if (manifest.shared !== undefined) {
    const names = { constant: SHARED_CONSTANT, rowPrefix: SHARED_ROW_PREFIX, claimant: SHARED_CLAIMANT }
    lines.push('', ...renderOperations(names, manifest.shared, declared))
  }

  return `${lines.join('\n')}\n`
}

const SHARED_CONSTANT = 'sharedOperations'
const SHARED_ROW_PREFIX = 'Shared'

interface Claimant {
  owner: string
  label: string
}

const SHARED_CLAIMANT: Claimant = { owner: 'shared', label: SHARED_CONSTANT }

interface OperationsNames {
  constant: string
  rowPrefix: string
  claimant: Claimant
}

function claim(declared: Map<string, Claimant>, generated: string, claimant: Claimant): string {
  const taken = declared.get(generated)
  if (taken !== undefined && taken.owner !== claimant.owner) {
    throw new Error(
      `'${claimant.label}' and '${taken.label}' both generate the identifier '${generated}'. Rename one of them so the generated file declares each name once.`,
    )
  }
  declared.set(generated, claimant)
  return generated
}

function readClaimant(claimant: Claimant, name: string): Claimant {
  return { owner: `${claimant.owner}/reads/${name}`, label: `${claimant.label}.reads.${name}` }
}

function renderOperations(
  names: OperationsNames,
  database: DatabaseManifest,
  declared: Map<string, Claimant>,
): string[] {
  const rowTypes = new Map<string, string>()
  const lines: string[] = []

  for (const [name, shape] of Object.entries(database.reads)) {
    if (shape.columns === null) continue
    const rowType = claim(declared, `${names.rowPrefix}${pascalCase(name)}Row`, readClaimant(names.claimant, name))
    rowTypes.set(name, rowType)
    lines.push(`export interface ${rowType} {`)
    for (const column of shape.columns) lines.push(`  ${propertyKey(column)}: unknown`)
    lines.push('}', '')
  }

  lines.push(`export const ${claim(declared, names.constant, names.claimant)} = {`, '  reads: {')
  for (const [name, shape] of Object.entries(database.reads)) {
    const row = rowTypes.get(name) ?? 'Record<string, unknown>'
    lines.push(
      `    ${propertyKey(name)}: { name: ${JSON.stringify(name)} } as OperationRef<${argsType(shape)}, ${row}>,`,
    )
  }
  lines.push('  },', '  writes: {')
  for (const [name, shape] of Object.entries(database.writes)) {
    lines.push(
      `    ${propertyKey(name)}: { name: ${JSON.stringify(name)} } as OperationRef<${argsType(shape)}, never>,`,
    )
  }
  lines.push('  },', '}')

  return lines
}

function argsType(shape: OperationShape): string {
  if (shape.args.length === 0) return 'Record<string, never>'
  return `{ ${shape.args.map(name => `${propertyKey(name)}: unknown`).join('; ')} }`
}

const IDENTIFIER = /^[A-Za-z_$][A-Za-z0-9_$]*$/

function propertyKey(name: string): string {
  return IDENTIFIER.test(name) ? name : JSON.stringify(name)
}

function identifier(value: string): string {
  const cleaned = value.replace(/[^A-Za-z0-9_$]/g, '_')
  return IDENTIFIER.test(cleaned) ? cleaned : `_${cleaned}`
}

function pascalCase(value: string): string {
  return value
    .split(/[^A-Za-z0-9]+/)
    .filter(part => part.length > 0)
    .map(part => part[0].toUpperCase() + part.slice(1))
    .join('')
}

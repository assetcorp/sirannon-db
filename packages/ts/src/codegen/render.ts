import type { DatabaseManifest, OperationManifest, OperationShape } from './manifest.js'

export interface RenderOptions {
  packageName?: string
}

const DEFAULT_PACKAGE = '@delali/sirannon-db'

export function renderOperationTypes(manifest: OperationManifest, options?: RenderOptions): string {
  const lines: string[] = [
    `import type { OperationRef } from '${options?.packageName ?? DEFAULT_PACKAGE}'`,
    '',
    `export const registryDigest = ${JSON.stringify(manifest.digest ?? null)}`,
  ]

  for (const databaseId of Object.keys(manifest.databases).sort()) {
    const database = manifest.databases[databaseId]
    lines.push('', ...renderDatabase(databaseId, database))
  }

  return `${lines.join('\n')}\n`
}

function renderDatabase(databaseId: string, database: DatabaseManifest): string[] {
  const prefix = pascalCase(databaseId)
  const lines: string[] = []

  for (const [name, shape] of Object.entries(database.reads)) {
    if (shape.columns === null) continue
    lines.push(`export interface ${prefix}${pascalCase(name)}Row {`)
    for (const column of shape.columns) lines.push(`  ${propertyKey(column)}: unknown`)
    lines.push('}', '')
  }

  lines.push(`export const ${identifier(databaseId)} = {`, '  reads: {')
  for (const [name, shape] of Object.entries(database.reads)) {
    const row = shape.columns === null ? 'Record<string, unknown>' : `${prefix}${pascalCase(name)}Row`
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

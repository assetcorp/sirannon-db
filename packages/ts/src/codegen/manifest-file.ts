import * as v from 'valibot'
import {
  type DatabaseManifest,
  OPERATION_MANIFEST_VERSION,
  type OperationManifest,
  type OperationShape,
} from './manifest.js'

const KeyedObject = v.custom<Record<string, unknown>>(
  value => typeof value === 'object' && value !== null && !Array.isArray(value),
  'is not a JSON object',
)

const Names = v.array(v.string())

const Shape = v.object({ args: Names, identityArgs: Names, columns: v.nullable(Names) })

const ManifestHeader = v.object({
  version: v.literal(OPERATION_MANIFEST_VERSION),
  digest: v.optional(v.string()),
  databases: KeyedObject,
  shared: v.optional(KeyedObject),
})

const DatabaseEntry = v.object({ reads: KeyedObject, writes: KeyedObject })

function checked<const TSchema extends v.GenericSchema>(
  schema: TSchema,
  value: unknown,
  path: string,
  where: string,
): v.InferOutput<TSchema> {
  const result = v.safeParse(schema, value)
  if (result.success) return result.output
  const issue = result.issues[0]
  const at = [where, ...(v.getDotPath(issue)?.split('.') ?? [])].filter(part => part !== '').join('.')
  throw new Error(`The manifest '${path}' does not hold the manifest format at ${at || 'its root'}: ${issue.message}`)
}

function shapes(value: Record<string, unknown>, path: string, where: string): Record<string, OperationShape> {
  const listed: Record<string, OperationShape> = Object.create(null)
  for (const name of Object.keys(value)) {
    const shape = checked(Shape, value[name], path, `${where}.${name}`)
    listed[name] = { args: shape.args, identityArgs: shape.identityArgs, columns: shape.columns }
  }
  return listed
}

function databaseOf(value: unknown, path: string, where: string): DatabaseManifest {
  const entry = checked(DatabaseEntry, value, path, where)
  return { reads: shapes(entry.reads, path, `${where}.reads`), writes: shapes(entry.writes, path, `${where}.writes`) }
}

/**
 * Checks the JSON that `sirannon-codegen --manifest` writes, and returns it as a manifest.
 *
 * The function keeps every database and operation name as written, including `constructor` and `__proto__`.
 *
 * @param data - The parsed JSON of the manifest file.
 * @param path - The path of the file, which appears in an error message.
 * @returns The manifest that the file describes.
 * @throws An `Error` whose message gives the file and the first part of it that breaks the manifest format.
 *
 * @public
 */
export function parseOperationManifest(data: unknown, path: string): OperationManifest {
  const header = checked(ManifestHeader, data, path, '')
  const databases: Record<string, DatabaseManifest> = Object.create(null)
  for (const databaseId of Object.keys(header.databases)) {
    databases[databaseId] = databaseOf(header.databases[databaseId], path, `databases.${databaseId}`)
  }
  return {
    version: OPERATION_MANIFEST_VERSION,
    digest: header.digest,
    databases,
    ...(header.shared === undefined ? {} : { shared: databaseOf(header.shared, path, 'shared') }),
  }
}

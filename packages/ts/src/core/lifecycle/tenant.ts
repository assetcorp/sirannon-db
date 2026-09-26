import type { DatabaseOptions } from '../types.js'

/**
 * Configures how {@link createTenantResolver} maps a tenant ID to a database file.
 *
 * @public
 */
export interface TenantResolverOptions {
  /**
   * The directory that holds every tenant's database file.
   */
  basePath: string
  /**
   * The file extension that the resolver appends to the tenant ID, which defaults to `.db`.
   */
  extension?: string
  /**
   * The options with which Sirannon opens every tenant database.
   */
  defaultOptions?: DatabaseOptions
}

const SAFE_ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/
const MAX_ID_LENGTH = 255
const MAX_FILENAME_LENGTH = 255

/**
 * Returns a tenant ID unchanged when it is safe to use in a file name, which
 * means 1 to 255 characters that start with a letter or digit and continue
 * with letters, digits, underscores, or hyphens.
 *
 * @param id - The ID to check.
 * @returns The ID when it is safe, or `undefined` otherwise.
 *
 * @public
 */
export function sanitizeTenantId(id: string): string | undefined {
  if (!id || id.length > MAX_ID_LENGTH) return undefined
  if (!SAFE_ID_PATTERN.test(id)) return undefined
  return id
}

/**
 * Returns the database file path for one tenant.
 *
 * @param basePath - The directory that holds the file.
 * @param tenantId - The tenant's ID.
 * @param extension - The file extension to append, which defaults to `.db`.
 * @returns The full path of that tenant's database file.
 * @throws An `Error` when the ID is unsafe or the file name exceeds 255 characters.
 *
 * @public
 */
export function tenantPath(basePath: string, tenantId: string, extension = '.db'): string {
  const sanitized = sanitizeTenantId(tenantId)
  if (!sanitized) {
    throw new Error(`Invalid tenant ID: '${tenantId}'`)
  }
  const filename = `${sanitized}${extension}`
  if (filename.length > MAX_FILENAME_LENGTH) {
    throw new Error(`Tenant filename exceeds maximum length of ${MAX_FILENAME_LENGTH} characters`)
  }
  return `${basePath}/${filename}`
}

/**
 * Returns a resolver for {@link LifecycleConfig.autoOpen} that turns a tenant ID into a database path.
 *
 * @param options - The base directory, the file extension, and the options for each tenant database.
 * @returns A resolver that returns a path and options, or `undefined` for an unsafe ID or a file name over 255 characters.
 *
 * @public
 */
export function createTenantResolver(
  options: TenantResolverOptions,
): (id: string) => { path: string; options?: DatabaseOptions } | undefined {
  const ext = options.extension ?? '.db'
  const defaultOpts = options.defaultOptions

  return (id: string) => {
    const sanitized = sanitizeTenantId(id)
    if (!sanitized) return undefined

    const filename = `${sanitized}${ext}`
    if (filename.length > MAX_FILENAME_LENGTH) return undefined

    return {
      path: `${options.basePath}/${filename}`,
      options: defaultOpts,
    }
  }
}

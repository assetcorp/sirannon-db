import type { DatabaseOptions } from '../types.js'

/**
 * How tenant identifiers map onto database files.
 *
 * @public
 */
export interface TenantResolverOptions {
  /**
   * Directory every tenant's database file is written to.
   */
  basePath: string
  /**
   * File extension appended to the tenant identifier. Default: '.db'.
   */
  extension?: string
  /**
   * Options every tenant database opens with.
   */
  defaultOptions?: DatabaseOptions
}

const SAFE_ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/
const MAX_ID_LENGTH = 255
const MAX_FILENAME_LENGTH = 255

/**
 * Checks a tenant identifier against the characters and length a file name may carry.
 *
 * @param id - The identifier to check.
 * @returns The identifier when it is safe, and undefined when it is not.
 *
 * @public
 */
export function sanitizeTenantId(id: string): string | undefined {
  if (!id || id.length > MAX_ID_LENGTH) return undefined
  if (!SAFE_ID_PATTERN.test(id)) return undefined
  return id
}

/**
 * Builds the database file path for one tenant.
 *
 * @param basePath - Directory the file is written to.
 * @param tenantId - Identifier of the tenant.
 * @param extension - File extension to append. Default: '.db'.
 * @returns The full path for that tenant's database file.
 * @throws When the identifier is unsafe or the resulting file name is too long.
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
 * Builds a resolver that turns a tenant identifier into a database path, for {@link LifecycleConfig.autoOpen}.
 *
 * @param options - Base directory, file extension, and the options each tenant database opens with.
 * @returns A resolver that returns a path and options, or undefined for an unsafe identifier.
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

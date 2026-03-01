import { join } from 'node:path'
import type { DatabaseOptions } from '../types.js'

/**
 * Options for {@link createTenantResolver}.
 */
export interface TenantResolverOptions {
  /** Base directory where tenant database files are stored. */
  basePath: string
  /** File extension for database files. Default: `'.db'`. */
  extension?: string
  /** Default options applied to every auto-opened tenant database. */
  defaultOptions?: DatabaseOptions
}

/**
 * Pattern that matches safe tenant IDs: starts with an alphanumeric
 * character, followed by zero or more alphanumerics, hyphens, or
 * underscores. This prevents path traversal, shell injection, and
 * filesystem-unsafe names.
 */
const SAFE_ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/

/** Maximum allowed length for a tenant ID (filesystem name safety). */
const MAX_ID_LENGTH = 255

/**
 * Maximum allowed length for the full filename (id + extension).
 * Matches the limit on ext4, APFS, and NTFS.
 */
const MAX_FILENAME_LENGTH = 255

/**
 * Validate and sanitize a tenant ID. Returns the ID unchanged when
 * it passes validation, or `undefined` when it is invalid.
 *
 * Rules:
 * - Must be 1–255 characters long
 * - Must start with a letter or digit
 * - May contain letters, digits, hyphens, and underscores
 * - Must not contain `..` (redundant given the regex, but explicit)
 */
export function sanitizeTenantId(id: string): string | undefined {
  if (!id || id.length > MAX_ID_LENGTH) return undefined
  if (!SAFE_ID_PATTERN.test(id)) return undefined
  return id
}

/**
 * Build a filesystem path for a tenant database.
 *
 * @throws {Error} when the tenant ID fails validation.
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
  return join(basePath, filename)
}

/**
 * Create a resolver function suitable for {@link LifecycleConfig.autoOpen}.
 * Maps tenant IDs to database file paths under a common base directory.
 *
 * Invalid tenant IDs (path traversal, special characters) cause the
 * resolver to return `undefined`, preventing the database from opening.
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
      path: join(options.basePath, filename),
      options: defaultOpts,
    }
  }
}

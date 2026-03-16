import type { DatabaseOptions } from '../types.js'

export interface TenantResolverOptions {
  basePath: string
  extension?: string
  defaultOptions?: DatabaseOptions
}

const SAFE_ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/
const MAX_ID_LENGTH = 255
const MAX_FILENAME_LENGTH = 255

export function sanitizeTenantId(id: string): string | undefined {
  if (!id || id.length > MAX_ID_LENGTH) return undefined
  if (!SAFE_ID_PATTERN.test(id)) return undefined
  return id
}

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

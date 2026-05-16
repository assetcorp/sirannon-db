import { describe, expect, it } from 'vitest'
import { LifecycleManager } from '../../lifecycle/manager.js'
import { createTenantResolver, sanitizeTenantId, tenantPath } from '../../lifecycle/tenant.js'
import { createRegistry, getTempDir } from './setup.js'

describe('tenant utilities', () => {
  describe('sanitizeTenantId', () => {
    it('allows simple alphanumeric IDs', () => {
      expect(sanitizeTenantId('tenant1')).toBe('tenant1')
      expect(sanitizeTenantId('ABC')).toBe('ABC')
      expect(sanitizeTenantId('a')).toBe('a')
    })

    it('allows hyphens and underscores after the first character', () => {
      expect(sanitizeTenantId('my-tenant')).toBe('my-tenant')
      expect(sanitizeTenantId('my_tenant')).toBe('my_tenant')
      expect(sanitizeTenantId('a-b-c_d')).toBe('a-b-c_d')
    })

    it('rejects IDs starting with a hyphen', () => {
      expect(sanitizeTenantId('-invalid')).toBeUndefined()
    })

    it('rejects IDs starting with an underscore', () => {
      expect(sanitizeTenantId('_invalid')).toBeUndefined()
    })

    it('rejects path traversal attempts', () => {
      expect(sanitizeTenantId('../etc/passwd')).toBeUndefined()
      expect(sanitizeTenantId('..%2F..%2Fetc')).toBeUndefined()
      expect(sanitizeTenantId('foo/../bar')).toBeUndefined()
    })

    it('rejects slashes', () => {
      expect(sanitizeTenantId('a/b')).toBeUndefined()
      expect(sanitizeTenantId('a\\b')).toBeUndefined()
    })

    it('rejects empty strings', () => {
      expect(sanitizeTenantId('')).toBeUndefined()
    })

    it('rejects IDs longer than 255 characters', () => {
      const long = 'a'.repeat(256)
      expect(sanitizeTenantId(long)).toBeUndefined()

      const exact = 'a'.repeat(255)
      expect(sanitizeTenantId(exact)).toBe(exact)
    })

    it('rejects special characters', () => {
      expect(sanitizeTenantId('ten ant')).toBeUndefined()
      expect(sanitizeTenantId('ten@ant')).toBeUndefined()
      expect(sanitizeTenantId('ten.ant')).toBeUndefined()
      expect(sanitizeTenantId('ten$ant')).toBeUndefined()
    })
  })

  describe('tenantPath', () => {
    it('generates a path from basePath and tenantId', () => {
      const result = tenantPath('/data/tenants', 'acme')
      expect(result).toBe('/data/tenants/acme.db')
    })

    it('uses a custom extension', () => {
      const result = tenantPath('/data', 'acme', '.sqlite')
      expect(result).toBe('/data/acme.sqlite')
    })

    it('throws on invalid tenant ID', () => {
      expect(() => tenantPath('/data', '../escape')).toThrow('Invalid tenant ID')
      expect(() => tenantPath('/data', '')).toThrow('Invalid tenant ID')
      expect(() => tenantPath('/data', '-bad')).toThrow('Invalid tenant ID')
    })

    it('throws when filename (id + extension) exceeds 255 characters', () => {
      const longId = 'a'.repeat(253)
      expect(() => tenantPath('/data', longId)).toThrow('maximum length')

      const okId = 'a'.repeat(252)
      expect(() => tenantPath('/data', okId)).not.toThrow()
    })

    it('throws when a long extension pushes filename past the limit', () => {
      const id = 'a'.repeat(250)
      expect(() => tenantPath('/data', id, '.sqlite3')).toThrow('maximum length')
    })
  })

  describe('createTenantResolver', () => {
    it('creates a resolver that maps IDs to paths', () => {
      const resolver = createTenantResolver({ basePath: '/data/dbs' })

      const result = resolver('tenant1')
      expect(result).toBeDefined()
      expect(result?.path).toBe('/data/dbs/tenant1.db')
    })

    it('uses custom extension', () => {
      const resolver = createTenantResolver({
        basePath: '/data',
        extension: '.sqlite3',
      })

      const result = resolver('mydb')
      expect(result?.path).toBe('/data/mydb.sqlite3')
    })

    it('returns undefined for invalid IDs', () => {
      const resolver = createTenantResolver({ basePath: '/data' })

      expect(resolver('../escape')).toBeUndefined()
      expect(resolver('')).toBeUndefined()
      expect(resolver('a/b')).toBeUndefined()
      expect(resolver('-bad')).toBeUndefined()
    })

    it('passes through default options', () => {
      const resolver = createTenantResolver({
        basePath: '/data',
        defaultOptions: { readOnly: true, readPoolSize: 2 },
      })

      const result = resolver('tenant1')
      expect(result).toBeDefined()
      expect(result?.options).toEqual({ readOnly: true, readPoolSize: 2 })
    })

    it('returns undefined options when none are configured', () => {
      const resolver = createTenantResolver({ basePath: '/data' })
      const result = resolver('tenant1')
      expect(result?.options).toBeUndefined()
    })

    it('returns undefined when filename exceeds 255 characters', () => {
      const resolver = createTenantResolver({ basePath: '/data' })
      const longId = 'a'.repeat(253)
      expect(resolver(longId)).toBeUndefined()
    })

    it('accepts IDs where filename is exactly 255 characters', () => {
      const resolver = createTenantResolver({ basePath: '/data' })
      const okId = 'a'.repeat(252)
      expect(resolver(okId)).toBeDefined()
    })

    it('works with LifecycleManager for end-to-end tenant resolution', async () => {
      const tempDir = getTempDir()
      const { dbs, callbacks } = createRegistry()
      const resolver = createTenantResolver({ basePath: tempDir })

      const manager = new LifecycleManager({ autoOpen: { resolver } }, callbacks)

      const db = await manager.resolve('acme')
      expect(db).toBeDefined()
      expect(db?.id).toBe('acme')
      expect(db?.path).toBe(`${tempDir}/acme.db`)
      expect(dbs.has('acme')).toBe(true)

      manager.dispose()
      await db?.close()
    })
  })
})

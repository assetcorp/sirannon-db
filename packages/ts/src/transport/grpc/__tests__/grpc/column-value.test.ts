import { describe, expect, it } from 'vitest'
import { fromColumnValue, toColumnValue } from '../../index.js'

describe('GrpcReplicationTransport', () => {
  describe('ColumnValue round-trip', () => {
    it('converts null correctly', () => {
      const cv = toColumnValue(null)
      expect(cv.nullValue).toBe(true)
      expect(fromColumnValue(cv)).toBeNull()
    })

    it('converts undefined correctly', () => {
      const cv = toColumnValue(undefined)
      expect(cv.nullValue).toBe(true)
      expect(fromColumnValue(cv)).toBeNull()
    })

    it('converts string correctly', () => {
      const cv = toColumnValue('hello world')
      expect(cv.stringValue).toBe('hello world')
      expect(fromColumnValue(cv)).toBe('hello world')
    })

    it('converts integer correctly', () => {
      const cv = toColumnValue(42)
      expect(cv.intValue).toBe(42n)
      expect(fromColumnValue(cv)).toBe(42n)
    })

    it('converts float correctly', () => {
      const cv = toColumnValue(3.14)
      expect(cv.floatValue).toBe(3.14)
      expect(fromColumnValue(cv)).toBe(3.14)
    })

    it('converts bigint correctly', () => {
      const cv = toColumnValue(9007199254740993n)
      expect(cv.intValue).toBe(9007199254740993n)
      expect(fromColumnValue(cv)).toBe(9007199254740993n)
    })

    it('converts boolean true correctly', () => {
      const cv = toColumnValue(true)
      expect(cv.boolValue).toBe(true)
      expect(fromColumnValue(cv)).toBe(true)
    })

    it('converts boolean false correctly', () => {
      const cv = toColumnValue(false)
      expect(cv.boolValue).toBe(false)
      expect(fromColumnValue(cv)).toBe(false)
    })

    it('converts Uint8Array correctly', () => {
      const data = new Uint8Array([0x01, 0x02, 0x03, 0xff])
      const cv = toColumnValue(data)
      expect(cv.blobValue).toBeDefined()
      const result = fromColumnValue(cv)
      expect(Buffer.isBuffer(result)).toBe(true)
      expect(Buffer.compare(result as Buffer, Buffer.from(data))).toBe(0)
    })

    it('converts Buffer correctly', () => {
      const data = Buffer.from([0xde, 0xad, 0xbe, 0xef])
      const cv = toColumnValue(data)
      expect(cv.blobValue).toBeDefined()
      const result = fromColumnValue(cv)
      expect(Buffer.isBuffer(result)).toBe(true)
      expect(Buffer.compare(result as Buffer, data)).toBe(0)
    })

    it('throws TypeError for unsupported types', () => {
      expect(() => toColumnValue(new Date())).toThrow(TypeError)
      expect(() => toColumnValue(Symbol('test'))).toThrow(TypeError)
      expect(() => toColumnValue({ nested: true })).toThrow(TypeError)
      expect(() => toColumnValue([1, 2, 3])).toThrow(TypeError)
    })
  })
})

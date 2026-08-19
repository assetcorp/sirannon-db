import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'
import { testDriver } from '../../core/__tests__/helpers/test-driver.js'
import { nodeStreamingSupport } from '../node-runtime.js'

describe('nodeStreamingSupport', () => {
  it('reports no streaming where the runtime parses no URI file names', () => {
    const support = nodeStreamingSupport({
      driver: testDriver,
      uriFilenames: false,
      extensionPath: '/tmp/sirannonvfs.dylib',
    })

    expect(support).toBeUndefined()
  })

  it('carries the extension the operator named', () => {
    const support = nodeStreamingSupport({
      driver: testDriver,
      uriFilenames: true,
      extensionPath: '/tmp/sirannonvfs.dylib',
    })

    expect(support?.extensionPath).toBe('/tmp/sirannonvfs.dylib')
  })

  it('names the extension by an absolute path where the operator gave a relative one', () => {
    const support = nodeStreamingSupport({
      driver: testDriver,
      uriFilenames: true,
      extensionPath: 'build/sirannonvfs.dylib',
    })

    expect(support?.extensionPath).toBe(resolve('build/sirannonvfs.dylib'))
    expect(support?.extensionPath).not.toBe('build/sirannonvfs.dylib')
  })
})

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
})

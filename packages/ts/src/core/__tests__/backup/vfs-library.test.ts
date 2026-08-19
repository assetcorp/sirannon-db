import { describe, expect, it } from 'vitest'
import {
  resolveVfsExtensionPath,
  vfsLibraryFileName,
  vfsLibrarySegments,
  vfsPackageName,
} from '../../../drivers/vfs-library.js'

describe('vfsPackageName', () => {
  it('names one package per platform Sirannon publishes a binary for', () => {
    expect(vfsPackageName('darwin', 'arm64')).toBe('@delali/sirannon-vfs-darwin-arm64')
    expect(vfsPackageName('linux', 'x64')).toBe('@delali/sirannon-vfs-linux-x64')
    expect(vfsPackageName('win32', 'arm64')).toBe('@delali/sirannon-vfs-win32-arm64')
  })

  it('names no package for a platform with no published binary', () => {
    expect(vfsPackageName('freebsd', 'x64')).toBeNull()
    expect(vfsPackageName('linux', 'ppc64')).toBeNull()
  })
})

describe('vfsLibraryFileName', () => {
  it('takes the file extension each platform loads shared libraries under', () => {
    expect(vfsLibraryFileName('darwin')).toBe('sirannonvfs.dylib')
    expect(vfsLibraryFileName('win32')).toBe('sirannonvfs.dll')
    expect(vfsLibraryFileName('linux')).toBe('sirannonvfs.so')
  })
})

describe('vfsLibrarySegments', () => {
  it('reads the musl build from a directory of its own, so SQLite still finds the entry point', () => {
    expect(vfsLibrarySegments('linux', true)).toEqual(['musl', 'sirannonvfs.so'])
    expect(vfsLibrarySegments('linux', false)).toEqual(['sirannonvfs.so'])
  })

  it('leaves every other platform with one library beside its manifest', () => {
    expect(vfsLibrarySegments('darwin', true)).toEqual(['sirannonvfs.dylib'])
    expect(vfsLibrarySegments('win32', false)).toEqual(['sirannonvfs.dll'])
  })
})

describe('resolveVfsExtensionPath', () => {
  it('finds nothing on a platform with no published binary', () => {
    expect(resolveVfsExtensionPath('freebsd', 'x64')).toBeNull()
  })
})

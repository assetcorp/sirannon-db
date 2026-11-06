import type { DatabaseFileSystem } from '../../core/driver/file-system.js'
import { EncryptionError } from '../../core/encryption/errors.js'

const EXPO_FILE_SYSTEM_FLOOR = '19.0.0'

const BASE64_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
const BASE64_VALUES = new Map([...BASE64_ALPHABET].map((char, value) => [char, value]))

type LegacyFileSystem = typeof import('expo-file-system/legacy')

interface ExpoFileModules {
  readonly fs: LegacyFileSystem
  readonly directory: string
}

let modules: Promise<ExpoFileModules> | null = null

async function loadLegacyFileSystem(): Promise<LegacyFileSystem | null> {
  try {
    return await import('expo-file-system/legacy')
  } catch {
    return null
  }
}

function loadModules(): Promise<ExpoFileModules> {
  modules ??= (async () => {
    const [fs, sqlite] = await Promise.all([loadLegacyFileSystem(), import('expo-sqlite')])
    if (fs === null) {
      throw new EncryptionError(
        `A key change on the Expo driver needs expo-file-system ${EXPO_FILE_SYSTEM_FLOOR} or later, whose expo-file-system/legacy entry reads, deletes, and moves the database files`,
        'ENCRYPTION_UNSUPPORTED',
      )
    }
    return { fs, directory: String(sqlite.defaultDatabaseDirectory) }
  })()
  return modules
}

function bytesOfBase64(text: string): Uint8Array {
  const bytes: number[] = []
  let buffer = 0
  let bits = 0
  for (const char of text) {
    const value = BASE64_VALUES.get(char)
    if (value === undefined) continue
    buffer = (buffer << 6) | value
    bits += 6
    if (bits >= 8) {
      bits -= 8
      bytes.push((buffer >> bits) & 0xff)
    }
  }
  return Uint8Array.from(bytes)
}

function fileUri(directory: string, path: string): string {
  const absolute = `${directory.replace(/\/*$/, '')}/${path.replace(/^\/+/, '')}`
  return `file://${absolute.split('/').map(encodeURIComponent).join('/')}`
}

/**
 * The file operations of the Expo driver, which resolve a database name the way `expo-sqlite` does and reach the
 * files through `expo-file-system/legacy`, whose move is one atomic rename on Android and a delete followed by a
 * rename on iOS. Each operation throws an `EncryptionError` with `ENCRYPTION_UNSUPPORTED` when the app carries no
 * expo-file-system 19.0.0 or later, so a key change stops before it writes any file.
 *
 * @internal
 */
export const expoDatabaseFiles: DatabaseFileSystem = {
  async readStart(path, length) {
    const { fs, directory } = await loadModules()
    const uri = fileUri(directory, path)
    const info = await fs.getInfoAsync(uri)
    if (!info.exists) return null
    if (info.size === 0) return new Uint8Array(0)
    const text = await fs.readAsStringAsync(uri, { encoding: fs.EncodingType.Base64, position: 0, length })
    return bytesOfBase64(text)
  },
  async remove(path) {
    const { fs, directory } = await loadModules()
    await fs.deleteAsync(fileUri(directory, path), { idempotent: true })
  },
  async rename(from, to) {
    const { fs, directory } = await loadModules()
    await fs.moveAsync({ from: fileUri(directory, from), to: fileUri(directory, to) })
  },
}

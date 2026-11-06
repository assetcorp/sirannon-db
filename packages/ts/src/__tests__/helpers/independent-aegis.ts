import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type StockDatabase from 'better-sqlite3'
import sodium from 'libsodium-wrappers'

await sodium.ready

const require = createRequire(import.meta.url)

const SQLITE_HEADER = Buffer.from('SQLite format 3\0', 'latin1')
const PAGE_SIZE_OFFSET = 16
const WRITE_VERSION_OFFSET = 18
const READ_VERSION_OFFSET = 19
const RESERVED_BYTES_OFFSET = 20
const LEGACY_JOURNAL_FORMAT = 1
const LARGEST_PAGE_SIZE = 65536
const AEGIS_TAG_BYTES = 32
const AEGIS_NONCE_BYTES = 32
const AEGIS_KEY_BYTES = 32
const AEGIS_RESERVED_BYTES = AEGIS_TAG_BYTES + AEGIS_NONCE_BYTES
const PAGE_ONE_CIPHER_OFFSET = 24
const WAL_HEADER_BYTES = 32
const WAL_PAGE_SIZE_OFFSET = 8
const WAL_FRAME_HEADER_BYTES = 24
const WAL_SALT_OFFSET = 16
const WAL_FRAME_SALT_OFFSET = 8
const WAL_SALT_BYTES = 8

function pageSizeOf(encrypted: Buffer): number {
  const raw = encrypted.readUInt16BE(PAGE_SIZE_OFFSET)
  const pageSize = raw === 1 ? LARGEST_PAGE_SIZE : raw
  if (pageSize < 512 || (pageSize & (pageSize - 1)) !== 0) {
    throw new Error(`The file declares a page size of ${raw}, which SQLite never writes`)
  }
  const reserved = encrypted[RESERVED_BYTES_OFFSET]
  if (reserved !== AEGIS_RESERVED_BYTES) {
    throw new Error(`The file reserves ${reserved} bytes per page, and AEGIS-256 pages reserve ${AEGIS_RESERVED_BYTES}`)
  }
  return pageSize
}

function decryptPage(page: Buffer, pageNumber: number, key: Uint8Array): Buffer {
  const plain = Buffer.from(page)
  const textEnd = page.length - AEGIS_RESERVED_BYTES
  const tag = page.subarray(textEnd, textEnd + AEGIS_TAG_BYTES)
  const pageNonce = page.subarray(textEnd + AEGIS_TAG_BYTES, textEnd + AEGIS_RESERVED_BYTES)
  const keystream = sodium.crypto_aead_aegis256_encrypt_detached(
    new Uint8Array(AEGIS_KEY_BYTES + AEGIS_NONCE_BYTES),
    null,
    null,
    pageNonce,
    key,
  ).ciphertext
  const oneTimeKey = Buffer.from(keystream)
  oneTimeKey.writeUInt32BE(pageNumber, oneTimeKey.length - 4)
  const start = pageNumber === 1 ? PAGE_ONE_CIPHER_OFFSET : 0
  const text = sodium.crypto_aead_aegis256_decrypt_detached(
    null,
    page.subarray(start, textEnd),
    tag,
    null,
    oneTimeKey.subarray(AEGIS_KEY_BYTES),
    oneTimeKey.subarray(0, AEGIS_KEY_BYTES),
  )
  plain.set(text, start)
  if (pageNumber === 1) plain.set(SQLITE_HEADER, 0)
  return plain
}

function committedWalPages(walPath: string, pageSize: number): { pages: Map<number, Buffer>; pageCount: number } {
  const pages = new Map<number, Buffer>()
  if (!existsSync(walPath)) return { pages, pageCount: 0 }
  const wal = readFileSync(walPath)
  if (wal.length < WAL_HEADER_BYTES) return { pages, pageCount: 0 }
  const salt = wal.subarray(WAL_SALT_OFFSET, WAL_SALT_OFFSET + WAL_SALT_BYTES)
  const pending = new Map<number, Buffer>()
  let pageCount = 0
  for (
    let at = WAL_HEADER_BYTES;
    at + WAL_FRAME_HEADER_BYTES + pageSize <= wal.length;
    at += WAL_FRAME_HEADER_BYTES + pageSize
  ) {
    const frameSalt = wal.subarray(at + WAL_FRAME_SALT_OFFSET, at + WAL_FRAME_SALT_OFFSET + WAL_SALT_BYTES)
    if (!frameSalt.equals(salt)) break
    const pageNumber = wal.readUInt32BE(at)
    const committedPageCount = wal.readUInt32BE(at + 4)
    const start = at + WAL_FRAME_HEADER_BYTES
    pending.set(pageNumber, wal.subarray(start, start + pageSize))
    if (committedPageCount === 0) continue
    for (const [number, page] of pending) pages.set(number, page)
    pending.clear()
    pageCount = committedPageCount
  }
  return { pages, pageCount }
}

export function decryptAegisDatabase(path: string, key: Uint8Array): Buffer {
  const file = readFileSync(path)
  const wal = `${path}-wal`
  const pageSize = file.length > 0 ? pageSizeOf(file) : readFileSync(wal).readUInt32BE(WAL_PAGE_SIZE_OFFSET)
  const { pages: walPages, pageCount: walPageCount } = committedWalPages(wal, pageSize)
  const pageCount = walPageCount > 0 ? walPageCount : file.length / pageSize
  const image = Buffer.alloc(pageCount * pageSize)
  for (let number = 1; number <= pageCount; number++) {
    const encrypted = walPages.get(number) ?? file.subarray((number - 1) * pageSize, number * pageSize)
    if (encrypted.length !== pageSize) throw new Error(`Page ${number} is missing from ${path} and its WAL`)
    if (number === 1) pageSizeOf(encrypted)
    image.set(decryptPage(encrypted, number, key), (number - 1) * pageSize)
  }
  image[WRITE_VERSION_OFFSET] = LEGACY_JOURNAL_FORMAT
  image[READ_VERSION_OFFSET] = LEGACY_JOURNAL_FORMAT
  return image
}

export function readWithIndependentAegis<T>(path: string, key: Uint8Array | null, sql: string): T[] {
  const Stock = require('better-sqlite3') as typeof StockDatabase
  if (key === null) {
    const db = new Stock(path, { readonly: true })
    try {
      return db.prepare(sql).all() as T[]
    } finally {
      db.close()
    }
  }
  const dir = mkdtempSync(join(tmpdir(), 'sirannon-aegis-oracle-'))
  try {
    const imagePath = join(dir, 'decrypted.db')
    writeFileSync(imagePath, decryptAegisDatabase(path, key))
    const db = new Stock(imagePath, { readonly: true })
    try {
      return db.prepare(sql).all() as T[]
    } finally {
      db.close()
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}

export function readWithMultipleCiphersAegis<T>(path: string, key: Uint8Array, sql: string): T[] {
  const Reader = require('better-sqlite3-multiple-ciphers') as typeof StockDatabase
  const db = new Reader(path, { readonly: true })
  try {
    db.pragma("cipher = 'aegis'")
    db.pragma("algorithm = 'aegis-256'")
    db.pragma(`key = "x'${Buffer.from(key).toString('hex')}'"`)
    return db.prepare(sql).all() as T[]
  } finally {
    db.close()
  }
}

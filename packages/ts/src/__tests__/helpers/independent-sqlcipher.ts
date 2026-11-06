import { existsSync, readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import type StockDatabase from 'better-sqlite3'

const require = createRequire(import.meta.url)

export function readWithIndependentSqlcipher<T>(path: string, key: Uint8Array | null, sql: string): T[] {
  const Reader = require('better-sqlite3-multiple-ciphers') as typeof StockDatabase
  const db = new Reader(path, { readonly: true })
  try {
    if (key !== null) {
      db.pragma("cipher = 'sqlcipher'")
      db.pragma('legacy = 4')
      db.pragma(`key = "x'${Buffer.from(key).toString('hex')}'"`)
    }
    return db.prepare(sql).all() as T[]
  } finally {
    db.close()
  }
}

export function readWithStockSqlite<T>(path: string, sql: string): T[] {
  const Stock = require('better-sqlite3') as typeof StockDatabase
  const db = new Stock(path, { readonly: true })
  try {
    return db.prepare(sql).all() as T[]
  } finally {
    db.close()
  }
}

export function filesHoldingText(paths: readonly string[], texts: readonly string[]): string[] {
  const found: string[] = []
  for (const path of paths) {
    if (!existsSync(path)) continue
    const bytes = readFileSync(path)
    if (texts.some(text => bytes.includes(Buffer.from(text, 'utf8')))) found.push(path)
  }
  return found
}

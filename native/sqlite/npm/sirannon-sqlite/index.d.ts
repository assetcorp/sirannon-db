import BetterSqlite3 = require('better-sqlite3')

declare namespace Database {
  interface SirannonDatabase extends BetterSqlite3.Database {
    /**
     * Returns up to 16 header bytes through SQLite's open file handle, before a key or a schema read.
     *
     * @returns The header bytes, or `null` for a database without a file handle.
     */
    readFileHeader(): Buffer | null
    key(rawKey: Uint8Array): this
    startChangeCapture(): this
    takeCapturedChanges(): Buffer
    stopChangeCapture(): this
    applyChanges(changes: Uint8Array): this
  }
}

declare const Database: {
  new (filename?: string | Buffer, options?: BetterSqlite3.Options): Database.SirannonDatabase
  (filename?: string | Buffer, options?: BetterSqlite3.Options): Database.SirannonDatabase
  prototype: Database.SirannonDatabase
  SqliteError: typeof BetterSqlite3.SqliteError
  bindingPath(): string | null
}

export = Database

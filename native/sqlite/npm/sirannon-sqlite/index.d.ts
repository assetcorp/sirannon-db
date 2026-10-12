import BetterSqlite3 = require('better-sqlite3')

declare const Database: typeof BetterSqlite3 & {
  bindingPath(): string | null
}

export = Database

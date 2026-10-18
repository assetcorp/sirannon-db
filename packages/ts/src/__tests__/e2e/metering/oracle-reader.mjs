import BetterSqlite3 from 'better-sqlite3'

process.on('message', ({ id, path, sql, params }) => {
  try {
    const db = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
    try {
      db.defaultSafeIntegers(true)
      process.send({ id, rows: db.prepare(sql).all(...params) })
    } finally {
      db.close()
    }
  } catch (err) {
    process.send({ id, error: err instanceof Error ? err.message : String(err) })
  }
})

process.on('disconnect', () => process.exit(0))

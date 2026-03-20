import { mkdirSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout } from 'node:timers/promises'
import { createTenantResolver, Database, Sirannon, sanitizeTenantId, tenantPath } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { loadMigrations } from '@delali/sirannon-db/file-migrations'

const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-example-'))

async function main() {
  console.log('=== Sirannon DB: better-sqlite3 driver example ===\n')

  const driver = betterSqlite3()

  console.log('1. Creating database with Database.create()...')
  const dbPath = join(tempDir, 'example.db')
  const db = await Database.create('main', dbPath, driver, {
    readPoolSize: 4,
    walMode: true,
    cdcPollInterval: 10,
  })
  console.log(`   Database created at: ${dbPath}\n`)

  console.log('2. Creating schema via db.execute()...')
  await db.execute(`
    CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      price REAL NOT NULL,
      stock INTEGER NOT NULL DEFAULT 0
    )
  `)
  console.log('   Products table created.\n')

  console.log('3. Running file-based migrations...')
  const migrationsDir = join(import.meta.dirname, 'migrations')
  const migrations = loadMigrations(migrationsDir)
  const migrationResult = await db.migrate(migrations)
  console.log(
    `   Applied ${migrationResult.applied.length} migration(s): ${migrationResult.applied.map(m => m.name).join(', ')}\n`,
  )

  console.log('4. Inserting data...')
  await db.execute('INSERT INTO users (name, email, age) VALUES (?, ?, ?)', ['Alice Johnson', 'alice@example.com', 30])
  await db.execute('INSERT INTO users (name, email, age) VALUES (?, ?, ?)', ['Bob Smith', 'bob@example.com', 25])
  console.log('   Inserted 2 users.\n')

  console.log('5. Querying data...')
  interface User {
    id: number
    name: string
    email: string
    age: number
    created_at: string
  }
  const users = await db.query<User>('SELECT * FROM users ORDER BY id')
  console.log(`   Found ${users.length} users:`)
  for (const user of users) {
    console.log(`     - ${user.name} (${user.email}), age ${user.age}`)
  }
  console.log()

  console.log('   Using queryOne():')
  const alice = await db.queryOne<User>('SELECT * FROM users WHERE email = ?', ['alice@example.com'])
  console.log(`   Found: ${alice?.name ?? 'not found'}\n`)

  console.log('6. Running a transaction...')
  await db.transaction(async tx => {
    await tx.execute('INSERT INTO orders (user_id, total, status) VALUES (?, ?, ?)', [1, 99.99, 'completed'])
    await tx.execute('INSERT INTO orders (user_id, total, status) VALUES (?, ?, ?)', [2, 149.5, 'pending'])
    const orders = await tx.query<{ id: number; total: number; status: string }>('SELECT * FROM orders ORDER BY id')
    console.log(`   Created ${orders.length} orders inside transaction.`)
    for (const order of orders) {
      console.log(`     - Order #${order.id}: $${order.total} (${order.status})`)
    }
  })
  console.log()

  console.log('7. CDC: watching for changes...')
  await db.watch('users')

  const cdcEvents: string[] = []
  const subscription = db.on('users').subscribe(event => {
    cdcEvents.push(`${event.type} on users: id=${(event.row as Record<string, unknown>).id}`)
  })

  await db.execute('INSERT INTO users (name, email, age) VALUES (?, ?, ?)', [
    'Charlie Brown',
    'charlie@example.com',
    35,
  ])
  await setTimeout(50)

  console.log(`   Received ${cdcEvents.length} CDC event(s):`)
  for (const evt of cdcEvents) {
    console.log(`     - ${evt}`)
  }
  subscription.unsubscribe()
  console.log()

  console.log('8. Connection pool with custom readPoolSize...')
  const db2Path = join(tempDir, 'pool-example.db')
  const db2 = await Database.create('pool-demo', db2Path, driver, {
    readPoolSize: 8,
    walMode: true,
  })
  await db2.execute('CREATE TABLE demo (id INTEGER PRIMARY KEY, value TEXT)')
  await db2.execute('INSERT INTO demo (value) VALUES (?)', ['test'])
  const demoRows = await db2.query('SELECT * FROM demo')
  console.log(`   Pool demo: ${db2.readerCount} readers, ${demoRows.length} row(s) queried.`)
  await db2.close()
  console.log()

  console.log('9. Metrics via Sirannon registry...')
  const queryLog: string[] = []
  const sirannon = new Sirannon({
    driver,
    metrics: {
      onQueryComplete: metrics => {
        queryLog.push(`[${metrics.databaseId}] ${metrics.sql.slice(0, 40)}... (${metrics.durationMs.toFixed(2)}ms)`)
      },
    },
  })

  const metricsDb = await sirannon.open('metrics-demo', join(tempDir, 'metrics.db'))
  await metricsDb.execute('CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)')
  await metricsDb.execute('INSERT INTO kv (key, value) VALUES (?, ?)', ['greeting', 'hello world'])
  await metricsDb.query('SELECT * FROM kv WHERE key = ?', ['greeting'])

  console.log(`   Captured ${queryLog.length} query metrics:`)
  for (const log of queryLog.slice(0, 3)) {
    console.log(`     - ${log}`)
  }
  await sirannon.shutdown()
  console.log()

  console.log('10. Multi-tenant via Sirannon lifecycle...')
  const tenantDir = join(tempDir, 'tenants')
  mkdirSync(tenantDir, { recursive: true })
  const resolver = createTenantResolver({ basePath: tenantDir })

  const tenantSirannon = new Sirannon({
    driver,
    lifecycle: {
      autoOpen: { resolver },
    },
  })

  const tenantIds = ['acme-corp', 'globex-inc']
  for (const tenantId of tenantIds) {
    const sanitized = sanitizeTenantId(tenantId)
    const path = tenantPath(tenantDir, tenantId)
    console.log(`   Tenant '${sanitized}' -> ${path}`)
    const tdb = await tenantSirannon.open(tenantId, path)
    await tdb.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')
    await tdb.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', ['plan', 'enterprise'])
    const setting = await tdb.queryOne<{ value: string }>('SELECT value FROM settings WHERE key = ?', ['plan'])
    console.log(`   Tenant '${tenantId}' plan: ${setting?.value}`)
  }
  console.log(`   Active tenants: ${tenantSirannon.databases().size}`)
  await tenantSirannon.shutdown()
  console.log()

  console.log('11. Hooks: beforeQuery and afterQuery...')
  const hookSirannon = new Sirannon({ driver })
  hookSirannon.onBeforeQuery(ctx => {
    if (ctx.sql.includes('DROP')) {
      throw new Error('DROP statements are blocked by hook')
    }
  })
  hookSirannon.onDatabaseOpen(ctx => {
    console.log(`   [hook] Database opened: ${ctx.databaseId}`)
  })

  const hookDb = await hookSirannon.open('hook-demo', join(tempDir, 'hooks.db'))
  hookDb.onAfterQuery(ctx => {
    if (ctx.durationMs > 100) {
      console.log(`   [hook] Slow query (${ctx.durationMs.toFixed(1)}ms): ${ctx.sql.slice(0, 50)}`)
    }
  })

  await hookDb.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)')
  await hookDb.execute('INSERT INTO test (id) VALUES (?)', [1])
  try {
    await hookDb.execute('DROP TABLE test')
  } catch (err) {
    console.log(`   [hook] Blocked: ${(err as Error).message}`)
  }
  await hookSirannon.shutdown()
  console.log()

  console.log('12. Backup...')
  const backupPath = join(tempDir, 'backup.db')
  await db.backup(backupPath)
  console.log(`   Backup created at: ${backupPath}`)

  const backupDb = await Database.create('backup-verify', backupPath, driver, { readOnly: true })
  const backupUsers = await backupDb.query<User>('SELECT * FROM users')
  console.log(`   Backup contains ${backupUsers.length} users.`)
  await backupDb.close()
  console.log()

  console.log('13. Graceful shutdown...')
  await db.close()
  console.log('   Main database closed.')
  console.log('   Cleaning up temp directory...')
  rmSync(tempDir, { recursive: true, force: true })
  console.log('   Done.\n')

  console.log('=== All features demonstrated ===')
}

main().catch(err => {
  console.error('Fatal error:', err)
  rmSync(tempDir, { recursive: true, force: true })
  process.exit(1)
})

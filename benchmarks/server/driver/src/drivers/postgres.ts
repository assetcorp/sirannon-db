import { Pool } from 'pg'
import type { PostgresConfig } from '../config.ts'
import type { FailureClassifier, FailureKind } from '../failures.ts'
import type { SeedTable } from '../workloads/workload.ts'
import { Driver, type TransactionStatement } from './driver.ts'

const SYNCHRONOUS_COMMIT: Record<string, string> = { full: 'on', matched: 'off' }
const MAX_BIND_PARAMS = 60_000

const SQLSTATE_CLASS_SHED = '53'
const SQLSTATE_CLASS_CONNECTION = '08'
const SQLSTATE_QUERY_CANCELED = '57014'
const SYSCALL_TIMEOUT = new Set(['ETIMEDOUT', 'ESOCKETTIMEDOUT'])
const SYSCALL_CONNECTION = new Set(['ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ENOTFOUND', 'EHOSTUNREACH', 'ENETUNREACH'])

function postgresCodeOf(err: unknown): string {
  const code = (err as { code?: unknown }).code
  if (typeof code === 'string' && code !== '') {
    return code
  }
  return err instanceof Error ? err.name : typeof err
}

function postgresKind(code: string, err: unknown): FailureKind {
  if (SYSCALL_TIMEOUT.has(code)) {
    return 'timeout'
  }
  if (SYSCALL_CONNECTION.has(code)) {
    return 'connection'
  }
  if (code === SQLSTATE_QUERY_CANCELED) {
    return 'timeout'
  }
  if (/^[0-9A-Z]{5}$/.test(code)) {
    if (code.startsWith(SQLSTATE_CLASS_SHED)) {
      return 'shed'
    }
    if (code.startsWith(SQLSTATE_CLASS_CONNECTION)) {
      return 'connection'
    }
    return 'server_error'
  }
  return err instanceof Error ? 'client_error' : 'unknown'
}

export class PostgresDriver extends Driver {
  readonly name = 'postgres'
  readonly delivery = 'socket'
  readonly dialect = 'postgres'

  private readonly config: PostgresConfig
  private readonly durability: string
  private readonly synchronousCommit: string
  private pool: Pool | null = null
  readonly failureClassifier: FailureClassifier = { codeOf: postgresCodeOf, kindOf: postgresKind }

  constructor(config: PostgresConfig, durability: string) {
    super()
    this.config = config
    this.durability = durability
    this.synchronousCommit = SYNCHRONOUS_COMMIT[durability] ?? 'off'
  }

  async connect(): Promise<void> {
    this.pool = new Pool({
      host: this.config.host,
      port: this.config.port,
      user: this.config.user,
      password: this.config.password,
      database: this.config.database,
      max: this.config.poolSize,
      options: `-c synchronous_commit=${this.synchronousCommit}`,
    })
    await this.read('SELECT 1', [])
  }

  private poolOrThrow(): Pool {
    if (this.pool === null) {
      throw new Error('Postgres driver used before connect()')
    }
    return this.pool
  }

  override render(sql: string): string {
    let index = 0
    return sql.replace(/\?/g, () => `$${++index}`)
  }

  async info(): Promise<Record<string, unknown>> {
    const pool = this.poolOrThrow()
    const version = (await pool.query('SELECT version()')).rows[0].version as string
    const sync = (await pool.query('SHOW synchronous_commit')).rows[0].synchronous_commit as string
    return {
      engine: 'postgres',
      delivery: this.delivery,
      durability_requested: this.durability,
      synchronous_commit: String(sync),
      version: String(version),
      pool_size: this.config.poolSize,
      max_concurrent_transactions: this.config.poolSize,
    }
  }

  async executeDdl(statements: string[]): Promise<void> {
    const pool = this.poolOrThrow()
    const client = await pool.connect()
    try {
      for (const statement of statements) {
        await client.query(statement)
      }
    } finally {
      client.release()
    }
  }

  async dropTables(tables: string[]): Promise<void> {
    const pool = this.poolOrThrow()
    const client = await pool.connect()
    try {
      for (const table of tables) {
        await client.query(`DROP TABLE IF EXISTS ${table} CASCADE`)
      }
    } finally {
      client.release()
    }
  }

  async seed(tables: SeedTable[]): Promise<void> {
    const pool = this.poolOrThrow()
    const client = await pool.connect()
    try {
      for (const table of tables) {
        const columnCount = table.columns.length
        const rowsPerBatch = Math.max(1, Math.floor(MAX_BIND_PARAMS / columnCount))
        const columns = table.columns.join(', ')
        let batch: unknown[][] = []
        let began = false
        const flush = async (): Promise<void> => {
          if (batch.length === 0) {
            return
          }
          if (!began) {
            await client.query('BEGIN')
            began = true
          }
          const values: unknown[] = []
          const tuples: string[] = []
          let bind = 0
          for (const row of batch) {
            const marks: string[] = []
            for (const value of row) {
              values.push(value)
              marks.push(`$${++bind}`)
            }
            tuples.push(`(${marks.join(', ')})`)
          }
          batch = []
          await client.query(`INSERT INTO ${table.table} (${columns}) VALUES ${tuples.join(', ')}`, values)
        }
        try {
          for (const row of table.rows) {
            batch.push(row)
            if (batch.length >= rowsPerBatch) {
              await flush()
            }
          }
          await flush()
          if (began) {
            await client.query('COMMIT')
          }
        } catch (err) {
          if (began) {
            await client.query('ROLLBACK')
          }
          throw err
        }
      }
    } finally {
      client.release()
    }
  }

  async read(sql: string, params: unknown[]): Promise<void> {
    await this.poolOrThrow().query(this.render(sql), params)
  }

  async write(sql: string, params: unknown[]): Promise<void> {
    await this.poolOrThrow().query(this.render(sql), params)
  }

  async transaction(statements: TransactionStatement[]): Promise<void> {
    const client = await this.poolOrThrow().connect()
    let open = false
    let poisoned: Error | undefined
    try {
      await client.query('BEGIN')
      open = true
      for (const statement of statements) {
        await client.query(this.render(statement.sql), statement.params)
      }
      await client.query('COMMIT')
      open = false
    } finally {
      if (open) {
        try {
          await client.query('ROLLBACK')
        } catch (err) {
          poisoned = err instanceof Error ? err : new Error(String(err))
        }
      }
      // Passing an error to release() destroys the session instead of returning it to the pool.
      client.release(poisoned)
    }
  }

  transactionRoundTrips(statementCount: number): number {
    return statementCount + 2
  }

  async close(): Promise<void> {
    if (this.pool !== null) {
      await this.pool.end()
      this.pool = null
    }
  }
}

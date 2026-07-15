import type { FailureClassifier, FailureKind } from '../failures.ts'
import type { RemoteDatabase } from '../sirannon-client.ts'
import { SirannonClient } from '../sirannon-client.ts'
import type { SeedTable } from '../workloads/workload.ts'
import { Driver, type TransactionStatement } from './driver.ts'

const SEED_BATCH_ROWS = 25_000

const SIRANNON_SHED = new Set(['WRITE_OVERLOADED'])
const SIRANNON_TIMEOUT = new Set(['TIMEOUT', 'WRITER_WORKER_TIMEOUT'])
const SIRANNON_CONNECTION = new Set(['CONNECTION_ERROR', 'TRANSPORT_ERROR'])
const SIRANNON_CLIENT = new Set(['INVALID_ARGUMENT', 'INVALID_RESPONSE', 'ROUTING_ERROR'])
const SIRANNON_UNKNOWN = new Set(['UNKNOWN_ERROR'])

function remoteCodeOf(err: unknown): string | undefined {
  const code = (err as { code?: unknown }).code
  return typeof code === 'string' && code !== '' ? code : undefined
}

function sirannonKind(code: string, err: unknown): FailureKind {
  // Re-reads err because sirannonCode substitutes err.name when the SDK reported no code at all.
  if (remoteCodeOf(err) === undefined) {
    return 'client_error'
  }
  if (SIRANNON_SHED.has(code)) {
    return 'shed'
  }
  if (SIRANNON_TIMEOUT.has(code)) {
    return 'timeout'
  }
  if (SIRANNON_CONNECTION.has(code)) {
    return 'connection'
  }
  if (SIRANNON_CLIENT.has(code)) {
    return 'client_error'
  }
  if (SIRANNON_UNKNOWN.has(code)) {
    return 'unknown'
  }
  return 'server_error'
}

function sirannonCode(err: unknown): string {
  return remoteCodeOf(err) ?? (err instanceof Error ? err.name : typeof err)
}

export class SirannonDriver extends Driver {
  readonly name = 'sirannon'
  readonly delivery = 'websocket'
  readonly dialect = 'sqlite'

  private readonly baseUrl: string
  private readonly databaseId: string
  private readonly endpoint: string
  private readonly durability: string
  private readonly requestTimeoutMs: number
  private client: SirannonClient | null = null
  private db: RemoteDatabase | null = null
  readonly failureClassifier: FailureClassifier = { codeOf: sirannonCode, kindOf: sirannonKind }

  constructor(baseUrl: string, databaseId: string, durability: string, requestTimeoutMs: number) {
    super()
    this.baseUrl = baseUrl
    this.databaseId = databaseId
    this.endpoint = `${baseUrl.replace(/\/+$/, '')}/db/${encodeURIComponent(databaseId)}`
    this.durability = durability
    this.requestTimeoutMs = requestTimeoutMs
  }

  async connect(): Promise<void> {
    this.client = new SirannonClient(this.baseUrl, { requestTimeout: this.requestTimeoutMs })
    this.db = this.client.database(this.databaseId)
    await this.read('SELECT 1', [])
  }

  private database(): RemoteDatabase {
    if (this.db === null) {
      throw new Error('Sirannon driver used before connect()')
    }
    return this.db
  }

  private async post(path: string, body: unknown): Promise<Record<string, unknown>> {
    const response = await fetch(`${this.endpoint}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!response.ok) {
      throw new Error(`POST ${path} returned ${response.status}: ${await response.text()}`)
    }
    return (await response.json()) as Record<string, unknown>
  }

  async info(): Promise<Record<string, unknown>> {
    // A PRAGMA synchronous read would report the read pool's NORMAL, not the writer's durability.
    const settings: Record<string, unknown> = {
      engine: 'sirannon',
      delivery: this.delivery,
      durability_requested: this.durability,
    }
    const journalRows = (await this.database().query('PRAGMA journal_mode')) as Array<Record<string, unknown>>
    const firstJournal = journalRows[0]
    settings.journal_mode = firstJournal ? String(Object.values(firstJournal)[0] ?? 'unknown') : 'unknown'
    const versionRows = (await this.database().query('SELECT sqlite_version() AS version')) as Array<
      Record<string, unknown>
    >
    settings.version = versionRows[0] ? String(versionRows[0].version) : 'unknown'
    return settings
  }

  async executeDdl(statements: string[]): Promise<void> {
    for (const statement of statements) {
      await this.post('/execute', { sql: statement })
    }
  }

  async dropTables(tables: string[]): Promise<void> {
    for (const table of tables) {
      await this.post('/execute', { sql: `DROP TABLE IF EXISTS ${table}` })
    }
  }

  async seed(tables: SeedTable[]): Promise<void> {
    for (const table of tables) {
      await this.database().loadAll(this.insertSql(table), table.rows, {
        batchSize: SEED_BATCH_ROWS,
        durability: 'off',
      })
    }
  }

  async read(sql: string, params: unknown[]): Promise<void> {
    await this.database().query(sql, params)
  }

  async write(sql: string, params: unknown[]): Promise<void> {
    await this.database().execute(sql, params)
  }

  async transaction(statements: TransactionStatement[]): Promise<void> {
    await this.database().transaction(statements)
  }

  transactionRoundTrips(_statementCount: number): number {
    return 1
  }

  async close(): Promise<void> {
    if (this.client !== null) {
      this.client.close()
      this.client = null
      this.db = null
    }
  }
}

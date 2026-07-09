// Drive Sirannon through the SDK an application ships with: seeding and measured operations both go
// over the WebSocket transport, DDL over HTTP `/execute`. Seeding uses the bulk-load endpoint, which
// relaxes writer durability per batch and restores the configured level, so measured writes still
// run at the requested durability.

import type { RemoteDatabase } from '../sirannon-client.ts'
import { SirannonClient } from '../sirannon-client.ts'
import type { SeedTable } from '../workloads.ts'
import { Driver } from './driver.ts'

const SEED_BATCH_ROWS = 25_000

export class SirannonDriver extends Driver {
  readonly name = 'sirannon'
  readonly delivery = 'websocket'
  readonly dialect = 'sqlite'

  private readonly baseUrl: string
  private readonly databaseId: string
  private readonly endpoint: string
  private readonly durability: string
  private client: SirannonClient | null = null
  private db: RemoteDatabase | null = null

  constructor(baseUrl: string, databaseId: string, durability: string) {
    super()
    this.baseUrl = baseUrl
    this.databaseId = databaseId
    this.endpoint = `${baseUrl.replace(/\/+$/, '')}/db/${encodeURIComponent(databaseId)}`
    this.durability = durability
  }

  async connect(): Promise<void> {
    // A multi-million-row seed load runs longer than the SDK's default request timeout.
    this.client = new SirannonClient(this.baseUrl, { requestTimeout: 0 })
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
    // The read pool always opens at synchronous=NORMAL, so a PRAGMA read would misreport the
    // writer's durability. The authoritative durability is the requested value, which the server
    // applies to the writer connection; journal_mode is persistent per database, so it reads back
    // accurately from any connection.
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
      const insert = this.insertSql(table)
      let batch: unknown[][] = []
      const flush = async (): Promise<void> => {
        if (batch.length === 0) {
          return
        }
        const paramsBatch = batch
        batch = []
        await this.database().load(insert, paramsBatch, 'off')
      }
      for (const row of table.rows) {
        batch.push(row)
        if (batch.length >= SEED_BATCH_ROWS) {
          await flush()
        }
      }
      await flush()
    }
  }

  async read(sql: string, params: unknown[]): Promise<void> {
    await this.database().query(sql, params)
  }

  async write(sql: string, params: unknown[]): Promise<void> {
    await this.database().execute(sql, params)
  }

  async close(): Promise<void> {
    if (this.client !== null) {
      this.client.close()
      this.client = null
      this.db = null
    }
  }
}

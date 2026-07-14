// Sirannon-only characterizations that PostgreSQL has no direct equivalent for.
//
// These are framed as characterizations, never as a win over PostgreSQL, because the two systems
// do not do the same work here. The change-feed measurement reports the lag from a committed write
// to that change arriving at a subscriber over Sirannon's built-in WebSocket feed, driven through
// the same SDK an application uses. PostgreSQL has no built-in change feed, so the honest
// comparison would be against PostgreSQL plus an external Debezium and Kafka pipeline, which is a
// stack comparison, not an engine one. The number here describes Sirannon's feed on its own terms.

import { SirannonClient } from './sirannon-client.ts'
import { maxOf, mean, percentile } from './stats.ts'

const PROBE_TABLE = 'cdc_probe'
const SERVER_POLL_INTERVAL_MS = 50
const SETUP_ATTEMPTS = 5
const SETUP_BACKOFF_MS = 250
const SETUP_TIMEOUT_MS = 10_000

function delay(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms)
  })
}

export async function measureCdcLatency(
  baseUrl: string,
  databaseId: string,
  samples: number,
  warmupSamples: number,
): Promise<Record<string, unknown>> {
  const endpoint = `${baseUrl.replace(/\/+$/, '')}/db/${encodeURIComponent(databaseId)}`
  // The change-feed setup runs after a long measured pass, when the single-threaded server can be
  // briefly unresponsive and refuse or reset a fresh connection. A connection-level failure is
  // retried with backoff; an HTTP error status means the server answered and the fault is logical,
  // so it is surfaced at once without retrying.
  const post = async (path: string, body: unknown): Promise<void> => {
    const url = `${endpoint}${path}`
    const payload = JSON.stringify(body)
    let lastError: unknown
    for (let attempt = 1; attempt <= SETUP_ATTEMPTS; attempt++) {
      let response: Response
      try {
        response = await fetch(url, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: payload,
          signal: AbortSignal.timeout(SETUP_TIMEOUT_MS),
        })
      } catch (err) {
        lastError = err
        if (attempt < SETUP_ATTEMPTS) {
          await delay(SETUP_BACKOFF_MS * attempt)
          continue
        }
        throw err
      }
      if (!response.ok) {
        throw new Error(`POST ${path} returned ${response.status}: ${await response.text()}`)
      }
      return
    }
    throw lastError
  }

  await post('/execute', { sql: `DROP TABLE IF EXISTS ${PROBE_TABLE}` })
  await post('/execute', { sql: `CREATE TABLE ${PROBE_TABLE} (id INTEGER PRIMARY KEY, marker INTEGER NOT NULL)` })

  const client = new SirannonClient(baseUrl)
  const db = client.database(databaseId)
  const latenciesMs: number[] = []

  let awaited: { marker: number; resolve: () => void } | null = null
  const subscription = await db.on(PROBE_TABLE).subscribe(event => {
    const row = (event.row ?? {}) as Record<string, unknown>
    if (awaited && (row.id === awaited.marker || row.marker === awaited.marker)) {
      const resolve = awaited.resolve
      awaited = null
      resolve()
    }
  })

  try {
    const total = warmupSamples + samples
    for (let index = 1; index <= total; index++) {
      const changeSeen = new Promise<void>(resolve => {
        awaited = { marker: index, resolve }
      })
      const sentAt = performance.now()
      await db.execute(`INSERT INTO ${PROBE_TABLE} (id, marker) VALUES (?, ?)`, [index, index])
      await changeSeen
      if (index > warmupSamples) {
        latenciesMs.push(performance.now() - sentAt)
      }
    }
  } finally {
    subscription.unsubscribe()
    client.close()
  }

  return {
    feature: 'cdc-latency',
    description:
      'Lag from a committed write to the change arriving at a subscriber over Sirannon\'s built-in ' +
      'WebSocket change feed. The server polls the change log on a fixed interval, so the floor is ' +
      'that interval.',
    samples: latenciesMs.length,
    server_poll_interval_ms: SERVER_POLL_INTERVAL_MS,
    latency_ms: {
      p50: percentile(latenciesMs, 0.5),
      p95: percentile(latenciesMs, 0.95),
      p99: percentile(latenciesMs, 0.99),
      max: maxOf(latenciesMs),
      mean: mean(latenciesMs),
    },
  }
}

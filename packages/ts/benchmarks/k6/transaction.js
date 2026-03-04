import { check } from 'k6'
import http from 'k6/http'
import { ZipfianGenerator } from './helpers/zipfian.js'

const SIRANNON_URL = __ENV.SIRANNON_URL || 'http://localhost:9876'
const POSTGRES_URL = __ENV.POSTGRES_URL || 'http://localhost:9877'
const TARGET_RPS = parseInt(__ENV.TARGET_RPS || '2000', 10)
const DURATION = __ENV.DURATION || '60s'
const DATA_SIZE = parseInt(__ENV.DATA_SIZE || '10000', 10)
const VUS = parseInt(__ENV.VUS || '100', 10)

const zipf = new ZipfianGenerator(DATA_SIZE)

export const options = {
  scenarios: {
    sirannon: {
      executor: 'constant-arrival-rate',
      rate: TARGET_RPS,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: VUS,
      maxVUs: VUS * 2,
      exec: 'transactionSirannon',
      tags: { engine: 'sirannon' },
    },
    postgres: {
      executor: 'constant-arrival-rate',
      rate: TARGET_RPS,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: VUS,
      maxVUs: VUS * 2,
      exec: 'transactionPostgres',
      startTime: `${parseInt(DURATION, 10) + 10}s`,
      tags: { engine: 'postgres' },
    },
  },
}

const headers = { 'Content-Type': 'application/json' }

function buildTransaction() {
  const customerId = zipf.next() + 1

  return JSON.stringify({
    statements: [
      {
        sql: 'SELECT * FROM users WHERE id = ?',
        params: [customerId],
      },
      {
        sql: 'UPDATE users SET age = ? WHERE id = ?',
        params: [Math.floor(Math.random() * 80) + 18, customerId],
      },
      {
        sql: 'SELECT * FROM users WHERE id = ?',
        params: [customerId],
      },
    ],
  })
}

export function transactionSirannon() {
  const payload = buildTransaction()
  const res = http.post(`${SIRANNON_URL}/db/bench/transaction`, payload, { headers })
  check(res, {
    'sirannon tx status 200': r => r.status === 200,
    'sirannon tx has results': r => {
      const body = r.json()
      return body?.results && body.results.length === 3
    },
  })
}

export function transactionPostgres() {
  const payload = buildTransaction()
  const res = http.post(`${POSTGRES_URL}/db/bench/transaction`, payload, { headers })
  check(res, {
    'postgres tx status 200': r => r.status === 200,
    'postgres tx has results': r => {
      const body = r.json()
      return body?.results && body.results.length === 3
    },
  })
}

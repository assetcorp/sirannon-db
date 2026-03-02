import { check } from 'k6'
import http from 'k6/http'
import { ZipfianGenerator } from './helpers/zipfian.js'

const SIRANNON_URL = __ENV.SIRANNON_URL || 'http://localhost:9876'
const POSTGRES_URL = __ENV.POSTGRES_URL || 'http://localhost:9877'
const TARGET_RPS = parseInt(__ENV.TARGET_RPS || '5000', 10)
const DURATION = __ENV.DURATION || '60s'
const DATA_SIZE = parseInt(__ENV.DATA_SIZE || '10000', 10)
const VUS = parseInt(__ENV.VUS || '200', 10)

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
      exec: 'querySirannon',
      tags: { engine: 'sirannon' },
    },
    postgres: {
      executor: 'constant-arrival-rate',
      rate: TARGET_RPS,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: VUS,
      maxVUs: VUS * 2,
      exec: 'queryPostgres',
      startTime: `${parseInt(DURATION, 10) + 10}s`,
      tags: { engine: 'postgres' },
    },
  },
  thresholds: {
    'http_req_duration{engine:sirannon}': ['p(99)<500'],
    'http_req_duration{engine:postgres}': ['p(99)<500'],
  },
}

const headers = { 'Content-Type': 'application/json' }

export function querySirannon() {
  const id = zipf.next() + 1
  const payload = JSON.stringify({
    sql: 'SELECT * FROM users WHERE id = ?',
    params: [id],
  })

  const res = http.post(`${SIRANNON_URL}/db/bench/query`, payload, { headers })
  check(res, {
    'sirannon status 200': r => r.status === 200,
    'sirannon has rows': r => {
      const body = r.json()
      return body && body.rows && body.rows.length > 0
    },
  })
}

export function queryPostgres() {
  const id = zipf.next() + 1
  const payload = JSON.stringify({
    sql: 'SELECT * FROM users WHERE id = ?',
    params: [id],
  })

  const res = http.post(`${POSTGRES_URL}/db/bench/query`, payload, { headers })
  check(res, {
    'postgres status 200': r => r.status === 200,
    'postgres has rows': r => {
      const body = r.json()
      return body && body.rows && body.rows.length > 0
    },
  })
}

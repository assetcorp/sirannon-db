import { check } from 'k6'
import http from 'k6/http'
import { ZipfianGenerator } from './helpers/zipfian.js'

const SIRANNON_URL = __ENV.SIRANNON_URL || 'http://localhost:9876'
const POSTGRES_URL = __ENV.POSTGRES_URL || 'http://localhost:9877'
const TARGET_RPS = parseInt(__ENV.TARGET_RPS || '5000', 10)
const DURATION = __ENV.DURATION || '60s'
const DATA_SIZE = parseInt(__ENV.DATA_SIZE || '10000', 10)
const VUS = parseInt(__ENV.VUS || '200', 10)
const READ_RATIO = parseFloat(__ENV.READ_RATIO || '0.8')

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
      exec: 'mixedSirannon',
      tags: { engine: 'sirannon' },
    },
    postgres: {
      executor: 'constant-arrival-rate',
      rate: TARGET_RPS,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: VUS,
      maxVUs: VUS * 2,
      exec: 'mixedPostgres',
      startTime: `${parseInt(DURATION, 10) + 10}s`,
      tags: { engine: 'postgres' },
    },
  },
}

const headers = { 'Content-Type': 'application/json' }

function buildPayload() {
  const id = zipf.next() + 1
  if (Math.random() < READ_RATIO) {
    return {
      endpoint: 'query',
      body: JSON.stringify({
        sql: 'SELECT * FROM users WHERE id = ?',
        params: [id],
      }),
    }
  }
  const age = Math.floor(Math.random() * 80) + 18
  return {
    endpoint: 'execute',
    body: JSON.stringify({
      sql: 'UPDATE users SET age = ? WHERE id = ?',
      params: [age, id],
    }),
  }
}

export function mixedSirannon() {
  const { endpoint, body } = buildPayload()
  const res = http.post(`${SIRANNON_URL}/db/bench/${endpoint}`, body, { headers })
  check(res, { 'sirannon status 200': r => r.status === 200 })
}

export function mixedPostgres() {
  const { endpoint, body } = buildPayload()
  const res = http.post(`${POSTGRES_URL}/db/bench/${endpoint}`, body, { headers })
  check(res, { 'postgres status 200': r => r.status === 200 })
}

import { connect } from 'node:net'
import { maxOf, percentile } from './stats.ts'

const LOOPBACK_HOSTS = new Set(['127.0.0.1', 'localhost', '::1'])
const CONNECT_SAMPLES = 200
const CONNECT_TIMEOUT_MS = 5_000

export function transportOf(host: string): 'loopback' | 'network' {
  return LOOPBACK_HOSTS.has(host) ? 'loopback' : 'network'
}

function connectOnce(host: string, port: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const started = performance.now()
    const socket = connect({ host, port })
    socket.setTimeout(CONNECT_TIMEOUT_MS)
    socket.once('connect', () => {
      const elapsed = performance.now() - started
      socket.destroy()
      resolve(elapsed)
    })
    socket.once('timeout', () => {
      socket.destroy()
      reject(new Error(`TCP connect to ${host}:${port} timed out after ${CONNECT_TIMEOUT_MS} ms`))
    })
    socket.once('error', err => {
      socket.destroy()
      reject(err)
    })
  })
}

export async function measureNetwork(host: string, port: number): Promise<Record<string, unknown>> {
  const samples: number[] = []
  for (let index = 0; index < CONNECT_SAMPLES; index++) {
    samples.push(await connectOnce(host, port))
  }
  return {
    probe_host: host,
    probe_port: port,
    method: 'tcp_connect',
    samples: samples.length,
    round_trip_ms: {
      min: Math.min(...samples),
      p50: percentile(samples, 0.5),
      p99: percentile(samples, 0.99),
      max: maxOf(samples),
    },
    note:
      "Each sample is the time from opening a TCP connection to a port on the engine's machine until " +
      'the connection is established, which takes one network round trip. The port belongs to a small ' +
      'service outside the engine, so the probe costs the engine nothing, and the load generator takes ' +
      'the samples before the pass starts.',
  }
}

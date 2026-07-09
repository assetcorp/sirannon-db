import type uWS from 'uWebSockets.js'
import type { CorsOptions } from '../core/types.js'

export interface ResolvedCors {
  origin: string | string[]
  methods: string
  headers: string
}

export function resolveCors(cors: boolean | CorsOptions | undefined): ResolvedCors | null {
  if (!cors) return null
  if (cors === true) {
    return {
      origin: '*',
      methods: 'GET, POST, OPTIONS',
      headers: 'Content-Type, Authorization',
    }
  }
  return {
    origin: cors.origin ?? '*',
    methods: cors.methods?.join(', ') ?? 'GET, POST, OPTIONS',
    headers: cors.headers?.join(', ') ?? 'Content-Type, Authorization',
  }
}

function matchOrigin(cors: ResolvedCors, requestOrigin: string): string | null {
  if (cors.origin === '*') return '*'
  if (typeof cors.origin === 'string') return cors.origin
  if (cors.origin.includes(requestOrigin)) return requestOrigin
  return null
}

export function writeCorsOrigin(res: uWS.HttpResponse, cors: ResolvedCors, requestOrigin: string): void {
  const allowed = matchOrigin(cors, requestOrigin)
  if (!allowed) return
  res.writeHeader('Access-Control-Allow-Origin', allowed)
  if (allowed !== '*') {
    res.writeHeader('Vary', 'Origin')
  }
}

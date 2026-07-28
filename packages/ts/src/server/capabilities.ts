import type { HttpResponse } from 'uWebSockets.js'
import { sendJson } from './http-common.js'

export const REQUIRED_DEVICE_SYNC_CAPABILITIES = [
  'sync.push',
  'sync.echo-suppression',
  'sync.ack',
  'sync.resume',
  'sync.snapshot',
  'sync.migrations',
  'sync.schema-gate',
  'sync.stream-apply',
] as const

export const SERVER_CAPABILITIES: readonly string[] = REQUIRED_DEVICE_SYNC_CAPABILITIES

export const NAMED_QUERY_CAPABILITY = 'query.named'
export const SQL_QUERY_CAPABILITY = 'query.sql'

export type ServerCapability = (typeof REQUIRED_DEVICE_SYNC_CAPABILITIES)[number]

export interface CapabilitiesResponse {
  capabilities: string[]
  registry?: { digest: string }
}

export interface CapabilitiesOptions {
  registryDigest?: string
  acceptSql?: boolean
}

export function buildCapabilitiesResponse(options?: CapabilitiesOptions): CapabilitiesResponse {
  const capabilities = [...SERVER_CAPABILITIES]
  if (options?.registryDigest !== undefined) capabilities.push(NAMED_QUERY_CAPABILITY)
  if (options?.acceptSql !== false) capabilities.push(SQL_QUERY_CAPABILITY)

  const response: CapabilitiesResponse = { capabilities }
  if (options?.registryDigest !== undefined) response.registry = { digest: options.registryDigest }
  return response
}

export function handleCapabilities(options?: CapabilitiesOptions): (res: HttpResponse) => void {
  const response = buildCapabilitiesResponse(options)
  return res => {
    sendJson(res, response)
  }
}

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

/**
 * A server announces this capability when its device stream, for a
 * subscription that asks for `stagedStream`, packs several events into each
 * `changes` frame and paces the delivery window continuously. A device requires
 * every capability in `REQUIRED_DEVICE_SYNC_CAPABILITIES` but not this one, so
 * it can still sync with an older server that sends one event per frame.
 */
export const STAGED_STREAM_CAPABILITY = 'sync.staged-stream'

export const SERVER_CAPABILITIES: readonly string[] = [...REQUIRED_DEVICE_SYNC_CAPABILITIES, STAGED_STREAM_CAPABILITY]

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
  acceptDeviceSync?: boolean
}

export function buildCapabilitiesResponse(options?: CapabilitiesOptions): CapabilitiesResponse {
  const capabilities = options?.acceptDeviceSync === true ? [...SERVER_CAPABILITIES] : []
  if (options?.registryDigest !== undefined) capabilities.push(NAMED_QUERY_CAPABILITY)
  if (options?.acceptSql === true) capabilities.push(SQL_QUERY_CAPABILITY)

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

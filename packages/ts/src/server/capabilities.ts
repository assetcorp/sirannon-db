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

/**
 * The body of `GET /capabilities`, which names what this server accepts.
 *
 * @public
 */
export interface CapabilitiesResponse {
  /**
   * The capabilities that the server announces, such as `query.sql` when it accepts SQL statements and `query.named`
   * when it serves registered operations.
   */
  capabilities: string[]
  /** The digest of the registered operations, which the server sends when it serves any. */
  registry?: {
    /** A hash over every registered database identifier, operation kind, operation name, and argument name. */
    digest: string
  }
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

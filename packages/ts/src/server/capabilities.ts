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
] as const

export const SERVER_CAPABILITIES: readonly string[] = REQUIRED_DEVICE_SYNC_CAPABILITIES

export type ServerCapability = (typeof REQUIRED_DEVICE_SYNC_CAPABILITIES)[number]

export interface CapabilitiesResponse {
  capabilities: string[]
}

export function handleCapabilities(): (res: HttpResponse) => void {
  const response: CapabilitiesResponse = { capabilities: [...SERVER_CAPABILITIES] }
  return res => {
    sendJson(res, response)
  }
}

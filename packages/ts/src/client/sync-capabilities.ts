import type { CapabilitiesResponse } from '../server/capabilities.js'
import { REQUIRED_DEVICE_SYNC_CAPABILITIES } from '../server/capabilities.js'
import { getJson } from './http-json.js'
import { RemoteError } from './types.js'

export interface CapabilityCheckOptions {
  url: string
  headers?: Record<string, string>
  requestTimeoutMs?: number
}

function validateCapabilities(raw: unknown): string[] {
  const record = raw as Partial<CapabilitiesResponse> | null
  if (
    record === null ||
    typeof record !== 'object' ||
    !Array.isArray(record.capabilities) ||
    record.capabilities.some(name => typeof name !== 'string')
  ) {
    throw new RemoteError('INVALID_RESPONSE', 'Capabilities response is malformed')
  }
  return record.capabilities
}

export async function fetchServerCapabilities(options: CapabilityCheckOptions): Promise<string[]> {
  return validateCapabilities(await getJson(`${options.url}/capabilities`, options.headers, options.requestTimeoutMs))
}

export async function verifyDeviceSyncCapabilities(options: CapabilityCheckOptions): Promise<string[]> {
  let capabilities: string[]
  try {
    capabilities = await fetchServerCapabilities(options)
  } catch (err) {
    if (err instanceof RemoteError && err.code === 'NOT_FOUND') {
      throw new RemoteError(
        'SYNC_UNSUPPORTED',
        'The server does not announce capabilities; it predates device sync and cannot sync safely. Upgrade the server.',
      )
    }
    throw err
  }

  const announced = new Set(capabilities)
  const missing = REQUIRED_DEVICE_SYNC_CAPABILITIES.filter(name => !announced.has(name))
  if (missing.length > 0) {
    throw new RemoteError(
      'SYNC_UNSUPPORTED',
      `The server does not support required device sync capabilities: ${missing.join(', ')}. Upgrade the server.`,
    )
  }
  return capabilities
}

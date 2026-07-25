import type { ForwardedTransaction, ReplicationBatch, SyncBatch, SyncRequest } from '../../replication/types.js'
import type { SerializedError } from './node-process.js'

export function serializeError(err: unknown): SerializedError {
  if (!(err instanceof Error)) {
    return {
      name: 'Error',
      message: String(err),
    }
  }
  const withCode = err as Error & {
    code?: string
    details?: Record<string, unknown>
  }
  return {
    name: err.name,
    message: err.message,
    code: withCode.code,
    details: withCode.details,
  }
}

export function stringPayload(payload: Record<string, unknown>, key: string): string {
  const value = payload[key]
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`Payload field '${key}' must be a non-empty string`)
  }
  return value
}

export function optionalArrayPayload(payload: Record<string, unknown>, key: string): unknown[] | undefined {
  const value = payload[key]
  if (value === undefined) return undefined
  if (!Array.isArray(value)) {
    throw new Error(`Payload field '${key}' must be an array when present`)
  }
  return value
}

export function numberPayload(payload: Record<string, unknown>, key: string): number {
  const value = payload[key]
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
    throw new Error(`Payload field '${key}' must be a safe integer`)
  }
  return value
}

export function arrayPayload(payload: Record<string, unknown>, key: string): unknown[] {
  const value = optionalArrayPayload(payload, key)
  if (!value) {
    throw new Error(`Payload field '${key}' is required`)
  }
  return value
}

export function parseBatch(value: unknown): ReplicationBatch {
  const batch = objectPayload(value, 'batch') as unknown as ReplicationBatch & {
    fromSeq: string
    toSeq: string
    primaryTerm?: string
  }
  return {
    ...batch,
    fromSeq: BigInt(batch.fromSeq),
    toSeq: BigInt(batch.toSeq),
    primaryTerm: batch.primaryTerm === undefined ? undefined : BigInt(batch.primaryTerm),
  }
}

export function parseSyncRequest(value: unknown): SyncRequest {
  const request = objectPayload(value, 'request') as unknown as SyncRequest & {
    primaryTerm?: string
  }
  return {
    ...request,
    primaryTerm: request.primaryTerm === undefined ? undefined : BigInt(request.primaryTerm),
  }
}

export function parseSyncBatch(value: unknown): SyncBatch {
  const batch = objectPayload(value, 'sync batch') as unknown as SyncBatch & {
    primaryTerm?: string
  }
  return {
    ...batch,
    primaryTerm: batch.primaryTerm === undefined ? undefined : BigInt(batch.primaryTerm),
  }
}

export function parseForwardRequest(value: unknown): ForwardedTransaction {
  const request = objectPayload(value, 'forward request') as unknown as ForwardedTransaction & {
    primaryTerm?: string
  }
  return {
    ...request,
    primaryTerm: request.primaryTerm === undefined ? undefined : BigInt(request.primaryTerm),
  }
}

function objectPayload(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`Payload field '${name}' must be an object`)
  }
  return value as Record<string, unknown>
}

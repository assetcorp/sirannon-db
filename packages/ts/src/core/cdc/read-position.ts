import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import { CHANGES_TABLE } from '../internal-tables.js'
import { selectMaxChangeSeq, selectTableExists } from '../system-catalog/index.js'

const TOKEN_VERSION = '1'
const HEX_RE = /^[0-9a-f]+$/
const SEQ_RE = /^\d+$/

export interface ReadPosition {
  epoch: string
  seq: bigint
}

export interface PositionedRows<T = Record<string, unknown>> {
  rows: T[]
  position: string
}

export function encodeReadPosition(position: ReadPosition): string {
  if (!HEX_RE.test(position.epoch)) {
    throw new CDCError(`Cannot issue a read position for epoch '${position.epoch}': an epoch is lower-case hex`)
  }
  if (position.seq < 0n) {
    throw new CDCError('Cannot issue a read position for a negative change-log sequence')
  }
  return toHex(`${TOKEN_VERSION}:${position.epoch}:${position.seq.toString()}`)
}

export function decodeReadPosition(token: string): ReadPosition | null {
  const decoded = fromHex(token)
  if (decoded === null) return null

  const parts = decoded.split(':')
  if (parts.length !== 3) return null
  const [version, epoch, seq] = parts
  if (version !== TOKEN_VERSION) return null
  if (epoch.length === 0 || !HEX_RE.test(epoch)) return null
  if (!SEQ_RE.test(seq)) return null
  return { epoch, seq: BigInt(seq) }
}

export async function readAtPosition<T>(
  conn: SQLiteConnection,
  epoch: string,
  read: (conn: SQLiteConnection) => Promise<T>,
): Promise<{ value: T; position: string; seq: bigint }> {
  return conn.transaction(async txConn => {
    const value = await read(txConn)
    const seq = (await selectTableExists(txConn, CHANGES_TABLE)) ? await selectMaxChangeSeq(txConn) : 0n
    return { value, position: encodeReadPosition({ epoch, seq }), seq }
  })
}

function toHex(value: string): string {
  let hex = ''
  for (let i = 0; i < value.length; i++) {
    hex += value.charCodeAt(i).toString(16).padStart(2, '0')
  }
  return hex
}

function fromHex(value: string): string | null {
  if (value.length === 0 || value.length % 2 !== 0 || !HEX_RE.test(value)) return null
  let decoded = ''
  for (let i = 0; i < value.length; i += 2) {
    decoded += String.fromCharCode(Number.parseInt(value.slice(i, i + 2), 16))
  }
  return decoded
}

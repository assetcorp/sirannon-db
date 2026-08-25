import { createDecipheriv } from 'node:crypto'

export const PAGE_RESERVED_BYTES = 128
export const NONCE_BYTES = 12
export const TAG_BYTES = 16
export const SALT_BYTES = 16
export const KEY_BYTES = 32
export const MASTER_NAME_BYTES = 16
export const KEY_RECORD_BYTES = 1 + SALT_BYTES + KEY_BYTES + NONCE_BYTES + TAG_BYTES + MASTER_NAME_BYTES
export const PLAINTEXT_HEADER_BYTES = 100
const WAL_HEADER_BYTES = 32
const WAL_FRAME_HEADER_BYTES = 24

export function pageSizeOf(file) {
  const raw = file.readUInt16BE(16)
  return raw === 1 ? 65536 : raw
}

export function readKeyRecord(file) {
  const pageSize = pageSizeOf(file)
  const tail = file.subarray(pageSize - PAGE_RESERVED_BYTES, pageSize)
  const record = tail.subarray(NONCE_BYTES + TAG_BYTES, NONCE_BYTES + TAG_BYTES + KEY_RECORD_BYTES)
  let at = 1
  const salt = record.subarray(at, (at += SALT_BYTES))
  const wrapped = record.subarray(at, (at += KEY_BYTES))
  const wrapNonce = record.subarray(at, (at += NONCE_BYTES))
  const wrapTag = record.subarray(at, (at += TAG_BYTES))
  const masterName = record.subarray(at, (at += MASTER_NAME_BYTES))
  return {
    pageSize,
    version: record[0],
    salt,
    wrapped,
    wrapNonce,
    wrapTag,
    masterName: masterName.toString().replace(/\0+$/, ''),
    pageNonce: tail.subarray(0, NONCE_BYTES),
    pageTag: tail.subarray(NONCE_BYTES, NONCE_BYTES + TAG_BYTES),
    spare: tail.subarray(NONCE_BYTES + TAG_BYTES + KEY_RECORD_BYTES),
  }
}

function open(key, nonce, aad, ciphertext, tag) {
  const decipher = createDecipheriv('aes-256-gcm', key, nonce)
  decipher.setAAD(aad)
  decipher.setAuthTag(tag)
  return Buffer.concat([decipher.update(ciphertext), decipher.final()])
}

export function unwrapDataKey(masterKey, record) {
  return open(masterKey, record.wrapNonce, record.salt, record.wrapped, record.wrapTag)
}

function aadFor(value) {
  const aad = Buffer.alloc(4)
  aad.writeUInt32BE(value)
  return aad
}

export function decryptPage(dataKey, page, aadValue, plainPrefix) {
  const out = Buffer.from(page)
  const tail = page.length - PAGE_RESERVED_BYTES
  const nonce = page.subarray(tail, tail + NONCE_BYTES)
  const tag = page.subarray(tail + NONCE_BYTES, tail + NONCE_BYTES + TAG_BYTES)
  const body = open(dataKey, nonce, aadFor(aadValue), page.subarray(plainPrefix, tail), tag)
  body.copy(out, plainPrefix)
  out.fill(0, tail)
  return out
}

export function walkLog(wal, walFormat, decryptFrame) {
  const header = walFormat.readLogHeader(new Uint8Array(wal))
  if (!header) return { header, frames: 0, accepted: 0, rejectedAt: 0 }
  const frames = Math.floor((wal.length - WAL_HEADER_BYTES) / header.frameBytes)
  let seed = header.checksum
  let accepted = 0
  for (let frame = 1; frame <= frames; frame++) {
    const offset = walFormat.logFrameOffset(frame, header.frameBytes)
    let bytes = wal
    if (decryptFrame) {
      bytes = Buffer.from(wal)
      const page = wal.subarray(offset + WAL_FRAME_HEADER_BYTES, offset + header.frameBytes)
      decryptFrame(page, frame).copy(bytes, offset + WAL_FRAME_HEADER_BYTES)
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    const result = walFormat.readValidLogFrame(view, offset, header, seed)
    if (!result) return { header, frames, accepted, rejectedAt: frame }
    accepted++
    seed = result.checksum
  }
  return { header, frames, accepted, rejectedAt: 0 }
}

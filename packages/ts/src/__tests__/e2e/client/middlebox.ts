import net from 'node:net'
import { closeListener, listenOnLoopback, relayToTarget } from '../lib/tcp-relay.js'

const HEADER_END = '\r\n\r\n'
const OPCODE_CONTINUATION = 0x0
const OPCODE_TEXT = 0x1
const FIN_BIT = 0x80
const MASK_BIT = 0x80
const LENGTH_16_MARKER = 126
const LENGTH_64_MARKER = 127

export type Direction = 'client' | 'server'

export interface ObservedMessage {
  readonly connection: number
  readonly direction: Direction
  readonly message: Record<string, unknown>
}

interface FrameStream {
  buffer: Buffer
  fragments: Buffer[]
}

interface Pair {
  readonly id: number
  readonly client: net.Socket
  readonly upstream: net.Socket
  mode: 'http' | 'websocket'
  clientHeaderText: string
  upstreamHeaderText: string
  upgraded: boolean
  readonly fromClient: FrameStream
  readonly fromServer: FrameStream
}

interface ParsedFrame {
  readonly raw: Buffer
  readonly opcode: number
  readonly fin: boolean
  readonly payload: Buffer
}

function parseFrame(buffer: Buffer): ParsedFrame | null {
  if (buffer.length < 2) return null
  const first = buffer[0] ?? 0
  const second = buffer[1] ?? 0
  const masked = (second & MASK_BIT) !== 0
  let length = second & 0x7f
  let offset = 2
  if (length === LENGTH_16_MARKER) {
    if (buffer.length < 4) return null
    length = buffer.readUInt16BE(2)
    offset = 4
  } else if (length === LENGTH_64_MARKER) {
    if (buffer.length < 10) return null
    length = Number(buffer.readBigUInt64BE(2))
    offset = 10
  }
  const maskKey = masked ? buffer.subarray(offset, offset + 4) : null
  if (masked) offset += 4
  if (buffer.length < offset + length) return null
  const payload = Buffer.from(buffer.subarray(offset, offset + length))
  if (maskKey !== null) {
    for (let index = 0; index < payload.length; index++) {
      payload[index] = (payload[index] ?? 0) ^ (maskKey[index % 4] ?? 0)
    }
  }
  return { raw: buffer.subarray(0, offset + length), opcode: first & 0x0f, fin: (first & FIN_BIT) !== 0, payload }
}

function textOf(stream: FrameStream, frame: ParsedFrame): string | null {
  if (frame.opcode !== OPCODE_TEXT && frame.opcode !== OPCODE_CONTINUATION) return null
  if (frame.opcode === OPCODE_TEXT) stream.fragments = []
  stream.fragments.push(frame.payload)
  if (!frame.fin) return null
  const text = Buffer.concat(stream.fragments).toString('utf8')
  stream.fragments = []
  return text
}

function asMessage(text: string): Record<string, unknown> | null {
  try {
    const parsed: unknown = JSON.parse(text)
    return typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : null
  } catch {
    return null
  }
}

export function serverTextFrame(text: string): Buffer {
  const payload = Buffer.from(text, 'utf8')
  if (payload.length < LENGTH_16_MARKER)
    return Buffer.concat([Buffer.from([FIN_BIT | OPCODE_TEXT, payload.length]), payload])
  if (payload.length <= 0xffff) {
    const header = Buffer.alloc(4)
    header[0] = FIN_BIT | OPCODE_TEXT
    header[1] = LENGTH_16_MARKER
    header.writeUInt16BE(payload.length, 2)
    return Buffer.concat([header, payload])
  }
  const header = Buffer.alloc(10)
  header[0] = FIN_BIT | OPCODE_TEXT
  header[1] = LENGTH_64_MARKER
  header.writeBigUInt64BE(BigInt(payload.length), 2)
  return Buffer.concat([header, payload])
}

export class WebSocketMiddlebox {
  private server: net.Server | null = null
  private readonly pairs = new Set<Pair>()
  private connectionCount = 0
  private refusals = 0
  private stalled = false
  private dropRule: ((message: Record<string, unknown>) => boolean) | null = null
  private targetPort: number
  readonly observed: ObservedMessage[] = []
  upgrades = 0
  port = 0

  constructor(targetPort: number) {
    this.targetPort = targetPort
  }

  get base(): string {
    return `http://127.0.0.1:${this.port}`
  }

  async listen(): Promise<void> {
    const server = net.createServer(client => this.accept(client))
    this.server = server
    this.port = await listenOnLoopback(server)
  }

  pointAt(targetPort: number): void {
    this.targetPort = targetPort
  }

  refuseNextConnections(count: number): void {
    this.refusals = count
  }

  dropConnectionWhenClientSends(rule: ((message: Record<string, unknown>) => boolean) | null): void {
    this.dropRule = rule
  }

  stallServerFrames(): void {
    this.stalled = true
    for (const pair of this.pairs) pair.upstream.pause()
  }

  releaseServerFrames(): void {
    this.stalled = false
    for (const pair of this.pairs) pair.upstream.resume()
  }

  inject(text: string): number {
    let written = 0
    for (const pair of this.pairs) {
      if (!pair.upgraded || pair.client.destroyed) continue
      pair.client.write(serverTextFrame(text))
      written += 1
    }
    return written
  }

  messages(direction: Direction, type?: string): Array<Record<string, unknown>> {
    return this.observed
      .filter(entry => entry.direction === direction && (type === undefined || entry.message.type === type))
      .map(entry => entry.message)
  }

  killAllConnections(): number {
    let killed = 0
    for (const pair of [...this.pairs]) {
      killed += 1
      this.drop(pair)
    }
    return killed
  }

  async close(): Promise<void> {
    this.killAllConnections()
    const server = this.server
    this.server = null
    await closeListener(server)
  }

  private accept(client: net.Socket): void {
    client.on('error', () => {})
    if (this.refusals > 0) {
      this.refusals -= 1
      client.destroy()
      return
    }
    const { upstream } = relayToTarget(client, this.targetPort, () => this.pairs.delete(pair))
    const pair: Pair = {
      id: ++this.connectionCount,
      client,
      upstream,
      mode: 'http',
      clientHeaderText: '',
      upstreamHeaderText: '',
      upgraded: false,
      fromClient: { buffer: Buffer.alloc(0), fragments: [] },
      fromServer: { buffer: Buffer.alloc(0), fragments: [] },
    }
    this.pairs.add(pair)
    if (this.stalled) upstream.pause()
    client.on('data', (chunk: Buffer) => this.fromClient(pair, chunk))
    upstream.on('data', (chunk: Buffer) => this.fromServer(pair, chunk))
  }

  private drop(pair: Pair): void {
    if (!this.pairs.delete(pair)) return
    pair.client.destroy()
    pair.upstream.destroy()
  }

  private fromClient(pair: Pair, chunk: Buffer): void {
    const upstream = pair.upstream
    if (pair.mode === 'http') {
      pair.clientHeaderText += chunk.toString('latin1')
      const end = pair.clientHeaderText.indexOf(HEADER_END)
      if (end === -1) {
        upstream.write(chunk)
        return
      }
      const headEnd = end + HEADER_END.length
      const upgrading = /^upgrade:\s*websocket/im.test(pair.clientHeaderText.slice(0, end))
      const splitAt = headEnd - (pair.clientHeaderText.length - chunk.length)
      pair.clientHeaderText = pair.clientHeaderText.slice(headEnd)
      if (!upgrading) {
        upstream.write(chunk)
        return
      }
      pair.mode = 'websocket'
      pair.upstreamHeaderText = ''
      upstream.write(chunk.subarray(0, splitAt))
      chunk = chunk.subarray(splitAt)
    }
    pair.fromClient.buffer = Buffer.concat([pair.fromClient.buffer, chunk])
    for (;;) {
      const frame = parseFrame(pair.fromClient.buffer)
      if (frame === null) return
      pair.fromClient.buffer = pair.fromClient.buffer.subarray(frame.raw.length)
      const text = textOf(pair.fromClient, frame)
      const message = text === null ? null : asMessage(text)
      if (message !== null) {
        this.observed.push({ connection: pair.id, direction: 'client', message })
        if (this.dropRule?.(message) === true) {
          this.drop(pair)
          return
        }
      }
      upstream.write(frame.raw)
    }
  }

  private fromServer(pair: Pair, chunk: Buffer): void {
    pair.client.write(chunk)
    if (pair.mode !== 'websocket') return
    let frames = chunk
    if (!pair.upgraded) {
      pair.upstreamHeaderText += chunk.toString('latin1')
      const end = pair.upstreamHeaderText.indexOf(HEADER_END)
      if (end === -1) return
      if (!/^HTTP\/1\.1 101/.test(pair.upstreamHeaderText)) {
        pair.mode = 'http'
        pair.clientHeaderText = ''
        return
      }
      pair.upgraded = true
      this.upgrades += 1
      const headerBytes = Buffer.byteLength(pair.upstreamHeaderText.slice(0, end + HEADER_END.length), 'latin1')
      const alreadyCounted = pair.upstreamHeaderText.length - chunk.length
      frames = chunk.subarray(headerBytes - alreadyCounted)
    }
    pair.fromServer.buffer = Buffer.concat([pair.fromServer.buffer, frames])
    for (;;) {
      const frame = parseFrame(pair.fromServer.buffer)
      if (frame === null) return
      pair.fromServer.buffer = pair.fromServer.buffer.subarray(frame.raw.length)
      const text = textOf(pair.fromServer, frame)
      const message = text === null ? null : asMessage(text)
      if (message !== null) this.observed.push({ connection: pair.id, direction: 'server', message })
    }
  }
}

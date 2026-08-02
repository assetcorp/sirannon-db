import { randomBytes } from 'node:crypto'
import http from 'node:http'
import type { Socket } from 'node:net'

export interface RawWireEvent {
  subscriptionId: string
  seq: bigint
  table: string
  txId: string | undefined
  txEnd: boolean
  row: Record<string, unknown>
}

export interface RawFrameRecord {
  kind: 'change' | 'changes'
  bytes: number
  events: number
}

interface Waiter {
  check: () => boolean
  wake: () => void
  fail: (err: Error) => void
}

export class RawDeviceClient {
  readonly messages: Array<Record<string, unknown>> = []
  readonly events: RawWireEvent[] = []
  readonly frames: RawFrameRecord[] = []
  closeCode: number | null = null
  socketClosed = false

  private buffer: Buffer = Buffer.alloc(0)
  private fragments: Buffer[] = []
  private readonly waiters = new Set<Waiter>()
  private idCounter = 0

  private constructor(private readonly socket: Socket) {
    socket.on('data', (chunk: string | Buffer) => this.onData(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)))
    socket.on('error', () => {})
    socket.on('close', () => {
      this.socketClosed = true
      this.wakeAll()
    })
  }

  static connect(port: number, databaseId: string): Promise<RawDeviceClient> {
    return new Promise((resolve, reject) => {
      const request = http.request({
        host: '127.0.0.1',
        port,
        path: `/db/${databaseId}`,
        agent: false,
        headers: {
          Connection: 'Upgrade',
          Upgrade: 'websocket',
          'Sec-WebSocket-Key': randomBytes(16).toString('base64'),
          'Sec-WebSocket-Version': '13',
        },
      })
      request.on('upgrade', (_response, socket, head) => {
        const client = new RawDeviceClient(socket)
        if (head.length > 0) {
          client.onData(head)
        }
        resolve(client)
      })
      request.on('response', response => reject(new Error(`WebSocket upgrade refused: HTTP ${response.statusCode}`)))
      request.on('error', reject)
      request.end()
    })
  }

  pauseReading(): void {
    this.socket.pause()
  }

  resumeReading(): void {
    this.socket.resume()
  }

  destroy(): void {
    this.socket.destroy()
  }

  close(): void {
    const payload = Buffer.alloc(2)
    payload.writeUInt16BE(1000, 0)
    this.sendFrame(8, payload)
    this.socket.end()
  }

  nextId(): string {
    this.idCounter += 1
    return `raw_${this.idCounter}`
  }

  send(message: Record<string, unknown>): void {
    this.sendFrame(1, Buffer.from(JSON.stringify(message)))
  }

  async request(message: Record<string, unknown>, timeoutMs = 15_000): Promise<Record<string, unknown>> {
    const id = message.id
    this.send(message)
    return this.waitForMessage(
      candidate =>
        candidate.id === id &&
        (candidate.type === 'subscribed' || candidate.type === 'result' || candidate.type === 'error'),
      timeoutMs,
    )
  }

  async subscribeDevice(options: {
    id: string
    tables: string[]
    deviceId: string
    stagedStream?: boolean
    sinceSeq?: bigint
    epoch?: string
  }): Promise<Record<string, unknown>> {
    const response = await this.request({
      type: 'subscribe',
      id: options.id,
      tables: options.tables,
      deviceId: options.deviceId,
      ...(options.stagedStream === true ? { stagedStream: true } : {}),
      ...(options.sinceSeq !== undefined ? { sinceSeq: options.sinceSeq.toString() } : {}),
      ...(options.epoch !== undefined ? { epoch: options.epoch } : {}),
    })
    if (response.type !== 'subscribed') {
      throw new Error(`subscribe failed: ${JSON.stringify(response)}`)
    }
    return response
  }

  sendAck(deviceId: string, seq: bigint): void {
    this.send({ type: 'ack', id: this.nextId(), deviceId, seq: seq.toString() })
  }

  eventsFor(subscriptionId: string): RawWireEvent[] {
    return this.events.filter(event => event.subscriptionId === subscriptionId)
  }

  async waitForMessage(
    matches: (message: Record<string, unknown>) => boolean,
    timeoutMs = 15_000,
  ): Promise<Record<string, unknown>> {
    let scanned = 0
    let found: Record<string, unknown> | undefined
    await this.waitUntil(() => {
      while (scanned < this.messages.length) {
        const candidate = this.messages[scanned]
        scanned += 1
        if (matches(candidate)) {
          found = candidate
          return true
        }
      }
      return false
    }, timeoutMs)
    if (found === undefined) {
      throw new Error('matched message vanished')
    }
    return found
  }

  async waitForEvents(subscriptionId: string, count: number, timeoutMs = 15_000): Promise<void> {
    await this.waitUntil(() => this.eventsFor(subscriptionId).length >= count, timeoutMs)
  }

  async waitForClose(timeoutMs = 15_000): Promise<void> {
    await this.waitUntil(() => this.socketClosed, timeoutMs)
  }

  private waitUntil(check: () => boolean, timeoutMs: number): Promise<void> {
    if (check()) return Promise.resolve()
    return new Promise((resolve, reject) => {
      const waiter: Waiter = {
        check,
        wake: () => {
          clearTimeout(timer)
          this.waiters.delete(waiter)
          resolve()
        },
        fail: err => {
          clearTimeout(timer)
          this.waiters.delete(waiter)
          reject(err)
        },
      }
      const timer = setTimeout(() => waiter.fail(new Error('timed out waiting on the raw WebSocket client')), timeoutMs)
      this.waiters.add(waiter)
      if (this.socketClosed) {
        waiter.check() ? waiter.wake() : waiter.fail(new Error('socket closed before the condition held'))
      }
    })
  }

  private wakeAll(): void {
    for (const waiter of [...this.waiters]) {
      if (waiter.check()) {
        waiter.wake()
      } else if (this.socketClosed) {
        waiter.fail(new Error(`socket closed before the condition held (close code ${this.closeCode ?? 'none'})`))
      }
    }
  }

  private sendFrame(opcode: number, payload: Buffer): void {
    const mask = randomBytes(4)
    const length = payload.length
    let header: Buffer
    if (length < 126) {
      header = Buffer.from([0x80 | opcode, 0x80 | length])
    } else if (length < 65_536) {
      header = Buffer.alloc(4)
      header[0] = 0x80 | opcode
      header[1] = 0x80 | 126
      header.writeUInt16BE(length, 2)
    } else {
      header = Buffer.alloc(10)
      header[0] = 0x80 | opcode
      header[1] = 0x80 | 127
      header.writeBigUInt64BE(BigInt(length), 2)
    }
    const masked = Buffer.allocUnsafe(length)
    for (let i = 0; i < length; i++) {
      masked[i] = payload[i] ^ mask[i & 3]
    }
    this.socket.write(Buffer.concat([header, mask, masked]))
  }

  private onData(chunk: Buffer): void {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk])
    while (this.buffer.length >= 2) {
      const fin = (this.buffer[0] & 0x80) !== 0
      const opcode = this.buffer[0] & 0x0f
      const maskedByServer = (this.buffer[1] & 0x80) !== 0
      let length = this.buffer[1] & 0x7f
      let offset = 2
      if (length === 126) {
        if (this.buffer.length < 4) return
        length = this.buffer.readUInt16BE(2)
        offset = 4
      } else if (length === 127) {
        if (this.buffer.length < 10) return
        length = Number(this.buffer.readBigUInt64BE(2))
        offset = 10
      }
      if (maskedByServer) {
        offset += 4
      }
      if (this.buffer.length < offset + length) return
      const payload = Buffer.from(this.buffer.subarray(offset, offset + length))
      this.buffer = this.buffer.subarray(offset + length)
      this.onFrame(opcode, fin, payload)
    }
  }

  private onFrame(opcode: number, fin: boolean, payload: Buffer): void {
    if (opcode === 9) {
      this.sendFrame(10, payload)
      return
    }
    if (opcode === 10) return
    if (opcode === 8) {
      this.closeCode = payload.length >= 2 ? payload.readUInt16BE(0) : 1005
      this.socket.end()
      return
    }
    if (opcode === 1 || opcode === 0) {
      this.fragments.push(payload)
      if (!fin) return
      const whole = this.fragments.length === 1 ? this.fragments[0] : Buffer.concat(this.fragments)
      this.fragments = []
      this.onMessage(whole)
    }
  }

  private onMessage(payload: Buffer): void {
    const message = JSON.parse(payload.toString('utf-8')) as Record<string, unknown>
    this.messages.push(message)
    if (message.type === 'change') {
      this.recordEvent(String(message.id), message.event as Record<string, unknown>)
      this.frames.push({ kind: 'change', bytes: payload.length, events: 1 })
    } else if (message.type === 'changes') {
      const wireEvents = message.events as Array<Record<string, unknown>>
      for (const wireEvent of wireEvents) {
        this.recordEvent(String(message.id), wireEvent)
      }
      this.frames.push({ kind: 'changes', bytes: payload.length, events: wireEvents.length })
    }
    this.wakeAll()
  }

  private recordEvent(subscriptionId: string, wireEvent: Record<string, unknown>): void {
    this.events.push({
      subscriptionId,
      seq: BigInt(String(wireEvent.seq)),
      table: String(wireEvent.table),
      txId: typeof wireEvent.txId === 'string' ? wireEvent.txId : undefined,
      txEnd: wireEvent.txEnd === true,
      row: wireEvent.row as Record<string, unknown>,
    })
  }
}

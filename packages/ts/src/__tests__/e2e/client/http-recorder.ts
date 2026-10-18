import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import net from 'node:net'

export interface RecordedRequest {
  readonly method: string
  readonly path: string
  readonly headers: Record<string, string | string[] | undefined>
  readonly text: string
  readonly body: unknown
}

export type HostileAnswer =
  | { kind: 'reply'; status: number; text: string; contentType?: string }
  | { kind: 'truncate'; status: number; declaredLength: number; text: string }
  | { kind: 'drop' }
  | { kind: 'stall' }

export type HostilePlan = (request: RecordedRequest) => HostileAnswer | undefined

function parsedOrText(text: string): unknown {
  if (text.length === 0) return undefined
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

async function readBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of request) chunks.push(Buffer.from(chunk))
  return Buffer.concat(chunks).toString('utf8')
}

function answerHostile(request: IncomingMessage, response: ServerResponse, answer: HostileAnswer): void {
  if (answer.kind === 'drop') {
    request.socket.destroy()
    return
  }
  if (answer.kind === 'stall') return
  if (answer.kind === 'truncate') {
    response.writeHead(answer.status, { 'content-type': 'application/json', 'content-length': answer.declaredLength })
    response.write(answer.text)
    setTimeout(() => request.socket.destroy(), 20)
    return
  }
  response.writeHead(answer.status, { 'content-type': answer.contentType ?? 'application/json' })
  response.end(answer.text)
}

export class RecordingHttpProxy {
  readonly requests: RecordedRequest[] = []
  private server: Server | null = null
  private plan: HostilePlan = () => undefined
  private readonly tunnels = new Set<net.Socket>()
  port = 0

  constructor(private targetPort: number | null) {}

  get base(): string {
    return `http://127.0.0.1:${this.port}`
  }

  answerWith(plan: HostilePlan): void {
    this.plan = plan
  }

  pointAt(targetPort: number): void {
    this.targetPort = targetPort
  }

  requestsTo(method: string, pathPattern: RegExp): RecordedRequest[] {
    return this.requests.filter(request => request.method === method && pathPattern.test(request.path))
  }

  async listen(): Promise<void> {
    const server = createServer((request, response) => {
      this.handle(request, response).catch(() => {
        request.socket.destroy()
      })
    })
    server.keepAliveTimeout = 1
    server.on('upgrade', (request: IncomingMessage, socket: net.Socket, head: Buffer) =>
      this.tunnel(request, socket, head),
    )
    this.server = server
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
    const address = server.address()
    if (address !== null && typeof address === 'object') this.port = address.port
  }

  async close(): Promise<void> {
    const server = this.server
    this.server = null
    if (server === null) return
    for (const socket of this.tunnels) socket.destroy()
    this.tunnels.clear()
    server.closeAllConnections()
    await new Promise<void>(resolve => server.close(() => resolve()))
  }

  private tunnel(request: IncomingMessage, socket: net.Socket, head: Buffer): void {
    socket.on('error', () => {})
    if (this.targetPort === null) {
      socket.destroy()
      return
    }
    const upstream = net.connect(this.targetPort, '127.0.0.1')
    this.tunnels.add(socket)
    this.tunnels.add(upstream)
    upstream.on('error', () => socket.destroy())
    socket.on('close', () => {
      this.tunnels.delete(socket)
      upstream.destroy()
    })
    upstream.on('close', () => {
      this.tunnels.delete(upstream)
      socket.destroy()
    })
    const lines = [`${request.method ?? 'GET'} ${request.url ?? '/'} HTTP/1.1`]
    for (let index = 0; index < request.rawHeaders.length; index += 2) {
      lines.push(`${request.rawHeaders[index]}: ${request.rawHeaders[index + 1]}`)
    }
    upstream.write(`${lines.join('\r\n')}\r\n\r\n`)
    if (head.length > 0) upstream.write(head)
    socket.pipe(upstream)
    upstream.pipe(socket)
  }

  private async handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const text = await readBody(request)
    const recorded: RecordedRequest = {
      method: request.method ?? '',
      path: request.url ?? '',
      headers: { ...request.headers },
      text,
      body: parsedOrText(text),
    }
    this.requests.push(recorded)
    const hostile = this.plan(recorded)
    if (hostile !== undefined) {
      answerHostile(request, response, hostile)
      return
    }
    if (this.targetPort === null) {
      response.writeHead(404, { 'content-type': 'application/json' })
      response.end(JSON.stringify({ error: { code: 'NOT_FOUND', message: 'Not found' } }))
      return
    }
    const forwardedHeaders = new Headers()
    for (const [name, value] of Object.entries(request.headers)) {
      if (value === undefined || name === 'host' || name === 'connection' || name === 'content-length') continue
      forwardedHeaders.set(name, Array.isArray(value) ? value.join(', ') : value)
    }
    const upstream = await fetch(`http://127.0.0.1:${this.targetPort}${recorded.path}`, {
      method: recorded.method,
      headers: forwardedHeaders,
      body: recorded.method === 'GET' || recorded.method === 'HEAD' ? undefined : text,
    })
    const answer = Buffer.from(await upstream.arrayBuffer())
    response.writeHead(upstream.status, { 'content-type': upstream.headers.get('content-type') ?? 'application/json' })
    response.end(answer)
  }
}

export class DroppingListener {
  private server: net.Server | null = null
  attempts = 0
  port = 0

  get base(): string {
    return `http://127.0.0.1:${this.port}`
  }

  async listen(): Promise<void> {
    const server = net.createServer(socket => {
      this.attempts += 1
      socket.on('error', () => {})
      socket.destroy()
    })
    this.server = server
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
    const address = server.address()
    if (address !== null && typeof address === 'object') this.port = address.port
  }

  async close(): Promise<void> {
    const server = this.server
    this.server = null
    if (server !== null) await new Promise<void>(resolve => server.close(() => resolve()))
  }
}

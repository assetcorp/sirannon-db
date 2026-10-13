import http from 'node:http'
import net from 'node:net'
import { track } from './harness.js'

export interface HostileRequest {
  readonly method: string
  readonly path: string
  readonly body: string
}

export type HostileAnswer =
  | { kind: 'forward' }
  | { kind: 'json'; status: number; body: unknown }
  | { kind: 'text'; status: number; text: string }
  | { kind: 'cut'; status: number; text: string }
  | { kind: 'stall'; status: number; text: string }
  | { kind: 'swallow' }
  | { kind: 'rewrite'; rewrite: (body: string) => string }

export type HostileRoute = (request: HostileRequest) => HostileAnswer

export interface HostileServerOptions {
  holdUpgrades?: () => boolean
}

export interface HostileServer {
  readonly url: string
  readonly requests: HostileRequest[]
  upgradeCount(): number
  downstreamFrames(): string
  dropUpgrades(): void
}

const FORWARD: HostileAnswer = { kind: 'forward' }

export function forwardEverything(): HostileAnswer {
  return FORWARD
}

function readBody(request: http.IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    request.on('data', chunk => chunks.push(chunk as Buffer))
    request.on('end', () => resolve(Buffer.concat(chunks)))
    request.on('error', reject)
  })
}

type Relay = { kind: 'forward' } | { kind: 'swallow' } | { kind: 'rewrite'; rewrite: (body: string) => string }

function relay(
  targetPort: number,
  request: http.IncomingMessage,
  body: Buffer,
  response: http.ServerResponse,
  how: Relay,
): void {
  const upstream = http.request(
    { host: '127.0.0.1', port: targetPort, method: request.method, path: request.url, headers: request.headers },
    answer => {
      if (how.kind === 'forward') {
        response.writeHead(answer.statusCode ?? 502, answer.headers)
        answer.pipe(response)
        return
      }
      const chunks: Buffer[] = []
      answer.on('data', chunk => chunks.push(chunk as Buffer))
      answer.on('end', () => {
        if (how.kind === 'swallow') {
          response.socket?.destroy()
          return
        }
        const text = how.rewrite(Buffer.concat(chunks).toString())
        response.writeHead(answer.statusCode ?? 502, {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(text),
        })
        response.end(text)
      })
    },
  )
  upstream.on('error', () => response.destroy())
  upstream.end(body)
}

function rawHead(request: http.IncomingMessage): string {
  const lines = [`${request.method} ${request.url} HTTP/1.1`]
  for (let at = 0; at < request.rawHeaders.length; at += 2) {
    lines.push(`${request.rawHeaders[at]}: ${request.rawHeaders[at + 1]}`)
  }
  return `${lines.join('\r\n')}\r\n\r\n`
}

function answerWith(answer: Exclude<HostileAnswer, Relay>, response: http.ServerResponse): void {
  if (answer.kind === 'json') {
    const text = JSON.stringify(answer.body)
    response.writeHead(answer.status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) })
    response.end(text)
    return
  }
  if (answer.kind === 'text') {
    response.writeHead(answer.status, { 'content-type': 'application/json' })
    response.end(answer.text)
    return
  }
  response.writeHead(answer.status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(answer.text),
  })
  response.write(answer.text.slice(0, Math.floor(answer.text.length / 2)))
  if (answer.kind === 'cut') response.socket?.destroy()
}

export async function startHostileServer(
  targetPort: () => number,
  route: HostileRoute,
  options: HostileServerOptions = {},
): Promise<HostileServer> {
  const requests: HostileRequest[] = []
  const sockets = new Set<net.Socket>()
  const upgrades = new Set<net.Socket>()
  const downstream: string[] = []
  let upgradeCount = 0

  const server = http.createServer(async (request, response) => {
    const body = await readBody(request)
    const seen: HostileRequest = { method: request.method ?? '', path: request.url ?? '', body: body.toString() }
    requests.push(seen)
    const answer = route(seen)
    if (answer.kind === 'forward' || answer.kind === 'swallow' || answer.kind === 'rewrite') {
      relay(targetPort(), request, body, response, answer)
      return
    }
    answerWith(answer, response)
  })

  server.on('connection', socket => {
    sockets.add(socket)
    socket.on('close', () => sockets.delete(socket))
  })

  server.on('upgrade', (request, socket, head) => {
    socket.on('error', () => {})
    upgradeCount += 1
    if (options.holdUpgrades?.() === true) return
    const client = socket as net.Socket
    upgrades.add(client)
    client.on('close', () => upgrades.delete(client))
    const upstream = net.connect(targetPort(), '127.0.0.1', () => {
      upstream.write(rawHead(request))
      if (head.length > 0) upstream.write(head)
      upstream.on('data', chunk => {
        downstream.push(chunk.toString('latin1'))
      })
      upstream.pipe(socket)
      socket.pipe(upstream)
    })
    sockets.add(upstream)
    upstream.on('error', () => socket.destroy())
    upstream.on('close', () => {
      sockets.delete(upstream)
      socket.destroy()
    })
    socket.on('close', () => upstream.destroy())
  })

  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', () => resolve()))
  const address = server.address()
  const port = address !== null && typeof address === 'object' ? address.port : 0
  track(
    () =>
      new Promise<void>(resolve => {
        for (const socket of sockets) socket.destroy()
        server.close(() => resolve())
      }),
  )

  return {
    url: `http://127.0.0.1:${port}`,
    requests,
    upgradeCount: () => upgradeCount,
    downstreamFrames: () => downstream.join(''),
    dropUpgrades() {
      for (const client of upgrades) client.destroy()
      upgrades.clear()
    },
  }
}

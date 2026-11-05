import { type ChildProcess, spawn } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterEach, describe, expect, it } from 'vitest'

const SERVER_SCRIPT = join(dirname(fileURLToPath(import.meta.url)), 'authenticating-server.ts')
const ACCEPTED_TOKEN = 'Bearer inspector-token'
const SUSPENDED_TOKEN = 'Bearer suspended-inspector-token'
const HOOK_WAIT_HEADER = 'x-hook-wait'
const NO_WAIT = { [HOOK_WAIT_HEADER]: 'none' }
const WEBCRYPTO_WAIT = { [HOOK_WAIT_HEADER]: 'webcrypto' }
const WS_CLOSE_UNAUTHENTICATED = 4401
const WS_CLOSE_FORBIDDEN = 4403
const START_DEADLINE_MS = 20_000
const REFUSED_UPGRADE_CLOSE_DEADLINE_MS = 2_000
const STILL_OPEN_AT_DEADLINE = `still open after ${REFUSED_UPGRADE_CLOSE_DEADLINE_MS} ms`

interface RunningServer {
  child: ChildProcess
  port: number
  stderr: () => string
  stop: () => Promise<void>
}

type WebSocketWithHeadersConstructor = new (url: string, init: { headers: Record<string, string> }) => WebSocket

const WebSocketWithHeaders = WebSocket as unknown as WebSocketWithHeadersConstructor

let running: RunningServer | null = null

function startServer(): Promise<RunningServer> {
  const child = spawn(process.execPath, ['--import', 'tsx', SERVER_SCRIPT], { stdio: ['pipe', 'pipe', 'pipe'] })
  let stdout = ''
  let stderr = ''
  child.stderr?.on('data', chunk => {
    stderr += chunk.toString()
  })
  const exited = new Promise<void>(resolve => child.once('exit', () => resolve()))
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`The server did not start: ${stderr}`)), START_DEADLINE_MS)
    child.stdout?.on('data', chunk => {
      stdout += chunk.toString()
      const match = stdout.match(/listening (\d+)/)
      if (!match) return
      clearTimeout(timer)
      resolve({
        child,
        port: Number(match[1]),
        stderr: () => stderr,
        stop: async () => {
          child.stdin?.write('stop\n')
          await exited
        },
      })
    })
    child.once('exit', code => reject(new Error(`The server exited with ${code}: ${stderr}`)))
  })
}

function closeOfRefusedUpgradeWithinDeadline(port: number, headers: Record<string, string>): Promise<number | string> {
  return new Promise(resolve => {
    const ws = new WebSocketWithHeaders(`ws://127.0.0.1:${port}/db/council`, { headers })
    const deadline = setTimeout(() => resolve(STILL_OPEN_AT_DEADLINE), REFUSED_UPGRADE_CLOSE_DEADLINE_MS)
    ws.addEventListener('error', () => {})
    ws.addEventListener('close', event => {
      clearTimeout(deadline)
      resolve(event.code)
    })
  })
}

function firstReplyToSubscribe(port: number): Promise<string> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocketWithHeaders(`ws://127.0.0.1:${port}/db/council`, {
      headers: { authorization: ACCEPTED_TOKEN },
    })
    ws.addEventListener('open', () => ws.send(JSON.stringify({ id: 'minutes', type: 'subscribe', table: 'minutes' })))
    ws.addEventListener('message', event => {
      ws.close()
      resolve(String(event.data))
    })
    ws.addEventListener('error', () => reject(new Error('The accepted WebSocket failed')))
  })
}

afterEach(async () => {
  await running?.stop()
  running = null
})

describe('requests that an asynchronous hook refuses', () => {
  it('answers each refusal without writing outside a corked callback', async () => {
    running = await startServer()
    const { port } = running

    const migrations = await fetch(`http://127.0.0.1:${port}/db/council/migrations`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    })
    const refusedUpgradeCode = await closeOfRefusedUpgradeWithinDeadline(port, {})
    const subscribeReply = JSON.parse(await firstReplyToSubscribe(port))
    await running.stop()

    expect(migrations.status).toBe(401)
    expect(await migrations.json()).toEqual({
      error: { code: 'UNAUTHENTICATED', message: 'The inspector token was refused' },
    })
    expect(refusedUpgradeCode).toBe(WS_CLOSE_UNAUTHENTICATED)
    expect(subscribeReply).toMatchObject({ type: 'error', id: 'minutes', error: { code: 'HOOK_DENIED' } })
    expect(running.stderr()).not.toMatch(/corked callback/)
    running = null
  })

  it('closes every upgrade that the hook refuses at once, after a timer, or after a WebCrypto call within the deadline', async () => {
    running = await startServer()
    const { port } = running
    const suspended = { authorization: SUSPENDED_TOKEN }

    const unauthenticatedAtOnce = await closeOfRefusedUpgradeWithinDeadline(port, NO_WAIT)
    const unauthenticatedAfterTimer = await closeOfRefusedUpgradeWithinDeadline(port, {})
    const unauthenticatedAfterWebCrypto = await closeOfRefusedUpgradeWithinDeadline(port, WEBCRYPTO_WAIT)
    const forbiddenAtOnce = await closeOfRefusedUpgradeWithinDeadline(port, { ...NO_WAIT, ...suspended })
    const forbiddenAfterTimer = await closeOfRefusedUpgradeWithinDeadline(port, suspended)
    const forbiddenAfterWebCrypto = await closeOfRefusedUpgradeWithinDeadline(port, { ...WEBCRYPTO_WAIT, ...suspended })
    await running.stop()

    expect({
      unauthenticatedAtOnce,
      unauthenticatedAfterTimer,
      unauthenticatedAfterWebCrypto,
      forbiddenAtOnce,
      forbiddenAfterTimer,
      forbiddenAfterWebCrypto,
    }).toEqual({
      unauthenticatedAtOnce: WS_CLOSE_UNAUTHENTICATED,
      unauthenticatedAfterTimer: WS_CLOSE_UNAUTHENTICATED,
      unauthenticatedAfterWebCrypto: WS_CLOSE_UNAUTHENTICATED,
      forbiddenAtOnce: WS_CLOSE_FORBIDDEN,
      forbiddenAfterTimer: WS_CLOSE_FORBIDDEN,
      forbiddenAfterWebCrypto: WS_CLOSE_FORBIDDEN,
    })
    expect(running.stderr()).not.toMatch(/corked callback/)
    running = null
  })
})

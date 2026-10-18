import { spawn } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const SCRIPT = join(dirname(fileURLToPath(import.meta.url)), 'server-without-uwebsockets.ts')
const INSTALL_COMMAND = 'pnpm add -E "uWebSockets.js@github:uNetworking/uWebSockets.js#v20.69.0"'
const CHILD_DEADLINE_MS = 20_000

interface ChildReport {
  first: { code: string | null; message: string }
  second: { code: string | null; message: string }
  portAfterFailure: number
  closed: boolean
  rows: Array<{ n: number }>
}

function runWithoutUWebSockets(): Promise<{ exitCode: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--import', 'tsx', SCRIPT], { stdio: ['ignore', 'pipe', 'pipe'] })
    let stdout = ''
    let stderr = ''
    const timer = setTimeout(() => {
      child.kill()
      reject(new Error(`The child process did not finish: ${stderr}`))
    }, CHILD_DEADLINE_MS)
    child.stdout.on('data', chunk => {
      stdout += chunk.toString()
    })
    child.stderr.on('data', chunk => {
      stderr += chunk.toString()
    })
    child.once('exit', exitCode => {
      clearTimeout(timer)
      resolve({ exitCode, stdout, stderr })
    })
  })
}

describe('a process that cannot load uWebSockets.js', () => {
  it('builds the server, fails each listen with SERVER_DEPENDENCY_MISSING naming the install command, and still closes', async () => {
    const { exitCode, stdout, stderr } = await runWithoutUWebSockets()
    expect([exitCode, stderr]).toEqual([0, ''])
    const report = JSON.parse(stdout.trim()) as ChildReport

    for (const outcome of [report.first, report.second]) {
      expect(outcome.code).toBe('SERVER_DEPENDENCY_MISSING')
      expect(outcome.message).toContain('uWebSockets.js')
      expect(outcome.message).toContain(INSTALL_COMMAND)
    }
    expect(report.portAfterFailure).toBe(-1)
    expect(report.closed).toBe(true)
    expect(report.rows).toEqual([{ n: 0 }])
  })
})

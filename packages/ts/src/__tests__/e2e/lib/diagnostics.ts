import { cpSync, mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { TestContext } from 'vitest'
import type { ReplicationErrorEvent } from '../../../replication/types.js'
import type { ManagedNode } from './factory.js'

const PRESERVED_FAILURE_ROOT = join(tmpdir(), 'sirannon-e2e-failures')

const preserveFlags = new WeakSet<ManagedNode>()

export interface DiagnosticsHandle {
  dumpIfFailed(ctx: TestContext): void
  cleanup(): void
}

export function attachDiagnostics(...nodes: ManagedNode[]): DiagnosticsHandle {
  return {
    dumpIfFailed(ctx: TestContext): void {
      const failed = ctx.task.result?.state === 'fail'
      if (failed) {
        dumpFailureSnapshot(ctx.task.name, nodes)
      }
    },
    cleanup(): void {
      for (const node of nodes) {
        if (preserveFlags.has(node)) continue
        try {
          rmSync(node.tempDir, { recursive: true, force: true })
        } catch {
          /* best-effort */
        }
      }
    },
  }
}

function dumpFailureSnapshot(testName: string, nodes: ManagedNode[]): void {
  const safeName = testName.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 80)
  const failureDir = join(PRESERVED_FAILURE_ROOT, `${safeName}-${Date.now()}`)

  try {
    mkdirSync(failureDir, { recursive: true })
  } catch {
    /* best-effort */
  }

  const lines: string[] = [`Failure snapshot: ${testName}`, `Captured: ${new Date().toISOString()}`, '']

  for (const node of nodes) {
    const status = node.engine.status()
    lines.push(`# Node: ${node.nodeId}`)
    lines.push(`  role: ${status.role}`)
    lines.push(`  replicating: ${status.replicating}`)
    lines.push(`  syncState.phase: ${status.syncState?.phase ?? 'unknown'}`)
    lines.push(`  currentSeq: ${node.engine.getCurrentSeq()}`)

    for (const peer of status.peers) {
      const applied = node.engine.getAppliedSeq(peer.nodeId)
      lines.push(`  peer ${peer.nodeId}: appliedSeq=${applied} lastAckedSeq=${peer.lastAckedSeq}`)
    }

    if (node.recentErrors.length > 0) {
      lines.push('  recent errors:')
      for (const err of node.recentErrors) {
        lines.push(`    - ${formatErrorEvent(err)}`)
      }
    } else {
      lines.push('  recent errors: (none captured)')
    }
    lines.push('')

    preserveFlags.add(node)
    try {
      const dest = join(failureDir, node.nodeId)
      cpSync(node.tempDir, dest, { recursive: true })
    } catch (err: unknown) {
      lines.push(`  (temp-dir copy failed: ${err instanceof Error ? err.message : String(err)})`)
    }
  }

  try {
    writeFileSync(join(failureDir, 'summary.txt'), lines.join('\n'), 'utf-8')
  } catch {
    /* best-effort */
  }

  console.error(`\n[sirannon-e2e] preserved failure artefacts at: ${failureDir}`)
  console.error(lines.join('\n'))
}

function formatErrorEvent(event: ReplicationErrorEvent): string {
  const peerPart = event.peerId ? ` peer=${event.peerId}` : ''
  return `op=${event.operation}${peerPart} recoverable=${event.recoverable} msg=${event.error.message}`
}

import type { ReplicationEngine } from './engine.js'

const ackDelayByEngine = new WeakMap<ReplicationEngine, number>()

export function installTestHooks(engine: ReplicationEngine): void {
  Object.defineProperty(engine, '__setAckDelayMs', {
    value(ms: number): void {
      if (!Number.isFinite(ms) || ms < 0) {
        throw new RangeError('ACK delay must be a non-negative finite number')
      }
      ackDelayByEngine.set(engine, Math.floor(ms))
    },
    enumerable: false,
  })
}

export async function delayAckIfConfigured(engine: ReplicationEngine): Promise<void> {
  const delayMs = ackDelayByEngine.get(engine) ?? 0
  if (delayMs === 0) {
    return
  }

  await new Promise<void>(resolve => {
    const timer = setTimeout(resolve, delayMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
    timer.unref?.()
  })
}

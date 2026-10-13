import type { WriterContext } from './driver/types.js'

const UNTRACKED: WriterContext = {
  run: operation => operation(),
  isActive: () => false,
  exit: operation => operation(),
}

export class WriterLock {
  private tail: Promise<unknown> = Promise.resolve()
  private readonly context: WriterContext
  private repair: (() => Promise<void>) | null = null

  constructor(context?: WriterContext) {
    this.context = context ?? UNTRACKED
  }

  run<T>(operation: () => Promise<T>): Promise<T> {
    if (this.context.isActive()) {
      return operation()
    }
    const enter = () => this.context.run(this.repair === null ? operation : () => this.repairThen(operation))
    const ticket = this.tail.then(enter, enter)
    this.tail = ticket.then(swallow, swallow)
    return ticket
  }

  repairBeforeNextHold(repair: () => Promise<void>): void {
    this.repair = repair
  }

  private async repairThen<T>(operation: () => Promise<T>): Promise<T> {
    const repair = this.repair
    if (repair !== null) {
      await repair()
      if (this.repair === repair) this.repair = null
    }
    return operation()
  }

  isHeld(): boolean {
    return this.context.isActive()
  }

  async settle(): Promise<void> {
    await this.run(() => Promise.resolve())
  }

  detached<T>(operation: () => T): T {
    return this.context.exit(operation)
  }
}

function swallow(): void {}

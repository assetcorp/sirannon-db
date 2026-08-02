import type { WriterContext } from './driver/types.js'

const UNTRACKED: WriterContext = {
  run: operation => operation(),
  isActive: () => false,
  exit: operation => operation(),
}

export class WriterLock {
  private tail: Promise<unknown> = Promise.resolve()
  private readonly context: WriterContext

  constructor(context?: WriterContext) {
    this.context = context ?? UNTRACKED
  }

  run<T>(operation: () => Promise<T>): Promise<T> {
    if (this.context.isActive()) {
      return operation()
    }
    const enter = () => this.context.run(operation)
    const ticket = this.tail.then(enter, enter)
    this.tail = ticket.then(swallow, swallow)
    return ticket
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

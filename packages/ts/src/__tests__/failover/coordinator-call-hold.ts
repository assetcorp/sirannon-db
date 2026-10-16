import type { UpdateInSyncSetInput } from '../../replication/coordinator/types.js'

export interface CoordinatorCallHoldFilter {
  removesNodeId?: string
}

export class CoordinatorCallHold {
  private filter: CoordinatorCallHoldFilter | null = null
  private gate: Promise<void> = Promise.resolve()
  private openGate: () => void = () => undefined
  private heldCount = 0

  hold(filter: CoordinatorCallHoldFilter): void {
    this.openGate()
    this.filter = filter
    this.heldCount = 0
    this.gate = new Promise(resolve => {
      this.openGate = resolve
    })
  }

  release(): void {
    this.filter = null
    this.openGate()
  }

  held(): number {
    return this.heldCount
  }

  async pass(input: UpdateInSyncSetInput): Promise<void> {
    const filter = this.filter
    if (filter === null) return
    if (filter.removesNodeId !== undefined && input.inSyncNodeIds.includes(filter.removesNodeId)) return
    this.heldCount += 1
    await this.gate
  }
}

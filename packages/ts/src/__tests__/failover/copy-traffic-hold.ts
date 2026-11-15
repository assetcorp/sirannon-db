export class CopyTrafficHold {
  private kind: string | null = null
  private peerId: string | null = null
  private gate: Promise<void> = Promise.resolve()
  private releaseGate: () => void = () => undefined
  private count = 0

  configure(action: string, kind?: string, peerId?: string): number {
    if (action === 'release') {
      this.kind = null
      this.releaseGate()
    } else if (action === 'hold') {
      if (kind !== 'request' && kind !== 'batch' && kind !== 'ack') throw new Error('Invalid traffic kind')
      if (!peerId) throw new Error('A traffic hold needs a peer')
      this.releaseGate()
      this.kind = kind
      this.peerId = peerId
      this.count = 0
      this.gate = new Promise(resolve => {
        this.releaseGate = resolve
      })
    } else if (action !== 'count') {
      throw new Error('Invalid traffic action')
    }
    return this.count
  }

  async pass(kind: string, peerId: string): Promise<void> {
    if (this.kind !== kind || this.peerId !== peerId) return
    this.count++
    await this.gate
  }
}

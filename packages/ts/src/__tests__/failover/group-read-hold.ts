import { setImmediate } from 'node:timers/promises'

export class GroupReadHold {
  private armed = false
  private gate: Promise<void> = Promise.resolve()
  private releaseGate: () => void = () => undefined
  private delivered: Promise<void> = Promise.resolve()
  private markDelivered: () => void = () => undefined
  private count = 0

  async configure(action: string): Promise<number> {
    if (action === 'release') {
      this.armed = false
      this.releaseGate()
      if (this.count > 0) {
        await this.delivered
        await setImmediate()
      }
    } else if (action === 'hold') {
      this.releaseGate()
      this.armed = true
      this.count = 0
      this.gate = new Promise(resolve => {
        this.releaseGate = resolve
      })
      this.delivered = new Promise(resolve => {
        this.markDelivered = resolve
      })
    } else if (action !== 'count') {
      throw new Error('Invalid group read action')
    }
    return this.count
  }

  async pass(): Promise<void> {
    if (!this.armed) return
    this.armed = false
    this.count++
    await this.gate
    this.markDelivered()
  }
}

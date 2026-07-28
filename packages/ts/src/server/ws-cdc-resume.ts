import { PrimedSubscription as CorePrimedSubscription } from '../core/cdc/primed-subscription.js'
import type { ChangeEvent } from '../core/types.js'
import type { WSSendOutcome } from './ws-connection.js'

export { needsResync } from '../core/cdc/primed-subscription.js'

export class PrimedSubscription extends CorePrimedSubscription {
  constructor(
    sinceSeq: bigint,
    deliver: (event: ChangeEvent) => WSSendOutcome,
    onOverload: () => void,
    bufferLimit?: number,
    bufferByteLimit?: number,
  ) {
    super(sinceSeq, event => deliver(event) !== 'dropped', onOverload, bufferLimit, bufferByteLimit)
  }
}

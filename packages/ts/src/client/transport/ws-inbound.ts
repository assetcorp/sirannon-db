import type { WSServerMessage } from '../../server/protocol.js'
import { RemoteError } from '../types.js'
import type { LiveQueryRegistry } from './ws-live-state.js'
import type { PendingRequests } from './ws-pending.js'
import type { ActiveSubscription } from './ws-subscription-state.js'
import { applySubscribedMessage, deliverChangeMessage } from './ws-subscription-state.js'

export interface InboundSinks {
  pending: PendingRequests
  subscriptions: Map<string, ActiveSubscription>
  live: LiveQueryRegistry
}

export function routeServerMessage(raw: string, sinks: InboundSinks): void {
  let msg: WSServerMessage
  try {
    msg = JSON.parse(raw) as WSServerMessage
  } catch {
    return
  }

  switch (msg.type) {
    case 'result':
      sinks.pending.resolve(msg.id, msg.data)
      return

    case 'subscribed': {
      const sub = sinks.subscriptions.get(msg.id)
      if (sub) applySubscribedMessage(sub, msg)
      sinks.live.deliverSubscribed(msg.id, msg.rows ?? [])
      sinks.pending.resolve(msg.id, undefined)
      return
    }

    case 'error': {
      const failure = new RemoteError(msg.error.code, msg.error.message)
      if (!sinks.pending.reject(msg.id, failure)) sinks.live.fail(msg.id, failure)
      return
    }

    case 'change': {
      const sub = sinks.subscriptions.get(msg.id)
      if (sub) deliverChangeMessage(sub, msg)
      return
    }

    case 'live':
      sinks.live.deliver(msg.id, msg)
      return

    case 'unsubscribed':
      return
  }
}

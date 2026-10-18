import { AsyncLocalStorage } from 'node:async_hooks'
import type { IOptions } from 'etcd3'

type FaultHandling = NonNullable<IOptions['faultHandling']>
type MemberPolicyFactory = NonNullable<FaultHandling['host']>
type MemberPolicy = ReturnType<MemberPolicyFactory>
type CallPolicy = NonNullable<FaultHandling['global']>
type SuccessEvent = MemberPolicy['onSuccess']
type FailureEvent = MemberPolicy['onFailure']

const FAILURES_BEFORE_MEMBER_SKIP = 3
const MEMBER_SKIP_MS = 5_000
const ETCD3_RETRIES_AFTER_RECOVERABLE_FAILURE = 3

interface MemberHealth {
  failures: number
  skipUntilMs: number
  trialInFlight: boolean
}

interface CallSkipBudget {
  remaining: number
}

class EtcdMemberSkippedError extends Error {
  constructor() {
    super('The etcd client skipped a member that failed its recent calls, and it sends the call to another member')
    this.name = 'EtcdMemberSkippedError'
  }
}

const noSuccessEvents: SuccessEvent = () => ({ dispose: () => undefined })
const noFailureEvents: FailureEvent = () => ({ dispose: () => undefined })

function skips(member: MemberHealth, nowMs: number): boolean {
  if (member.failures < FAILURES_BEFORE_MEMBER_SKIP) return false
  return member.trialInFlight || nowMs < member.skipUntilMs
}

class MemberHealthPolicy implements MemberPolicy {
  declare readonly _altReturn: never
  readonly onSuccess = noSuccessEvents
  readonly onFailure = noFailureEvents

  constructor(
    private readonly member: MemberHealth,
    private readonly members: readonly MemberHealth[],
    private readonly isRecoverableError: (error: Error) => boolean,
    private readonly callSkips: AsyncLocalStorage<CallSkipBudget>,
  ) {}

  async execute<T>(fn: (context: { signal: AbortSignal }) => PromiseLike<T> | T, signal?: AbortSignal): Promise<T> {
    const member = this.member
    if (this.skipsMember(Date.now())) throw new EtcdMemberSkippedError()
    const trial = member.failures >= FAILURES_BEFORE_MEMBER_SKIP
    if (trial) member.trialInFlight = true
    try {
      const result = await fn({ signal: signal ?? new AbortController().signal })
      member.failures = 0
      member.skipUntilMs = 0
      return result
    } catch (err: unknown) {
      if (err instanceof Error && this.isRecoverableError(err)) {
        member.failures += 1
        if (member.failures >= FAILURES_BEFORE_MEMBER_SKIP) member.skipUntilMs = Date.now() + MEMBER_SKIP_MS
      }
      throw err
    } finally {
      if (trial) member.trialInFlight = false
    }
  }

  private skipsMember(nowMs: number): boolean {
    if (!skips(this.member, nowMs)) return false
    if (!this.members.some(other => !skips(other, nowMs))) return false
    const budget = this.callSkips.getStore()
    if (budget === undefined) return true
    if (budget.remaining === 0) return false
    budget.remaining -= 1
    return true
  }
}

class CallRetryPolicy implements CallPolicy {
  declare readonly _altReturn: never
  readonly onSuccess = noSuccessEvents
  readonly onFailure = noFailureEvents

  constructor(
    private readonly members: readonly MemberHealth[],
    private readonly isRecoverableError: (error: Error) => boolean,
    private readonly callSkips: AsyncLocalStorage<CallSkipBudget>,
  ) {}

  async execute<T>(fn: (context: { signal: AbortSignal }) => PromiseLike<T> | T, signal?: AbortSignal): Promise<T> {
    const context = { signal: signal ?? new AbortController().signal }
    for (let retries = 0; ; retries++) {
      try {
        return await this.callSkips.run({ remaining: Math.max(0, this.members.length - 1) }, () => fn(context))
      } catch (err: unknown) {
        const retryable =
          err instanceof Error &&
          this.isRecoverableError(err) &&
          !context.signal.aborted &&
          retries < ETCD3_RETRIES_AFTER_RECOVERABLE_FAILURE
        if (!retryable) throw err
      }
    }
  }
}

/**
 * Returns the fault handling for one etcd client. After three failed calls in a row, the client skips a member for
 * five seconds and sends its calls to the members that answer. It then sends one trial call to the skipped member.
 * When every member has failed, the client calls them anyway, so that it reaches etcd as soon as a member answers
 * again. A call can skip every member but the last one that etcd3 offers it, so that each try of a call reaches a
 * member. The client retries a call three times after a failure that another member or a later call can avoid, as
 * etcd3 does by default.
 *
 * @param isRecoverableError - The etcd3 check for a failure that another member or a later call can avoid, which is
 * the only kind of failure that counts against a member or earns a retry.
 * @returns The per-member and per-call policies, which etcd3 takes as its `faultHandling` option.
 *
 * @internal
 */
export function memberHealthFaultHandling(isRecoverableError: (error: Error) => boolean): FaultHandling {
  const members: MemberHealth[] = []
  const callSkips = new AsyncLocalStorage<CallSkipBudget>()
  return {
    host: () => {
      const member: MemberHealth = { failures: 0, skipUntilMs: 0, trialInFlight: false }
      members.push(member)
      return new MemberHealthPolicy(member, members, isRecoverableError, callSkips)
    },
    global: new CallRetryPolicy(members, isRecoverableError, callSkips),
  }
}

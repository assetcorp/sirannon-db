import type { IOptions } from 'etcd3'

type MemberPolicyFactory = NonNullable<NonNullable<IOptions['faultHandling']>['host']>
type MemberPolicy = ReturnType<MemberPolicyFactory>
type SuccessEvent = MemberPolicy['onSuccess']
type FailureEvent = MemberPolicy['onFailure']

const FAILURES_BEFORE_MEMBER_SKIP = 3
const MEMBER_SKIP_MS = 5_000

interface MemberHealth {
  failures: number
  skipUntilMs: number
  trialInFlight: boolean
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
  ) {}

  async execute<T>(fn: (context: { signal: AbortSignal }) => PromiseLike<T> | T, signal?: AbortSignal): Promise<T> {
    const member = this.member
    const nowMs = Date.now()
    if (skips(member, nowMs) && this.members.some(other => !skips(other, nowMs))) {
      throw new EtcdMemberSkippedError()
    }
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
}

/**
 * Returns the per-member fault policy for one etcd client. After three failed calls in a row, the client skips a
 * member for five seconds and sends its calls to the members that answer. It then sends one trial call to the skipped
 * member. When every member has failed, the client calls them anyway, so that it reaches etcd as soon as a member
 * answers again.
 *
 * @param isRecoverableError - The etcd3 check for a failure that another member or a later call can avoid, which is
 * the only kind of failure that counts against a member.
 * @returns The factory that etcd3 calls once for each member.
 *
 * @internal
 */
export function memberHealthPolicies(isRecoverableError: (error: Error) => boolean): MemberPolicyFactory {
  const members: MemberHealth[] = []
  return () => {
    const member: MemberHealth = { failures: 0, skipUntilMs: 0, trialInFlight: false }
    members.push(member)
    return new MemberHealthPolicy(member, members, isRecoverableError)
  }
}

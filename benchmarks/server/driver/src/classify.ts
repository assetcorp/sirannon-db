// Decide what a pass that fell short of its offered rate proves, and refuse to decide when it
// proves nothing.
//
// A shortfall has two possible causes and they look alike from the outside: the server cannot keep
// up, or the generator cannot. Both end with the generator behind schedule, so the question is
// where the requests are waiting. The in-flight cap answers it. When the server is the bottleneck,
// requests it has not answered pile up until the generator is pinned against its cap and can only
// send as fast as replies come back; the generator then spends essentially the whole window at the
// cap. When the generator is the bottleneck, the server answers what little it is sent quickly, so
// in-flight work stays well under the cap however hard the generator is pushed. Occupancy separates
// the two, and it does so without reference to the client ceiling.
//
// The client ceiling is used only in the one direction it is sound. It is measured on a trivial
// statement, so it overstates what the client can do on a workload returning real rows; it is an
// upper bound and nothing more. Reaching it proves the client is at its limit. Falling short of it
// proves nothing, because the workload's own lower ceiling may be what was hit. This is the fault
// the old rule made: it compared the offered rate against the ceiling and charged any shortfall
// above a fraction of it to the client, so a server pinned flat at 89k while being offered 140k,
// 170k and 200k was reported client-bound at every point, on the strength of a number the client
// never came close to.
//
// The two causes are reported independently because they are not exclusive: a generator at its own
// ceiling in front of a saturated server is both. When neither shows, the pass is marked
// indeterminate rather than assigned to whichever cause is left over.
//
// What occupancy proves has a limit worth stating. It proves the generator was waiting on replies
// rather than failing to send, which is what rules out a slow send loop. It cannot separate a
// server too busy to answer from a client too slow to read the answers, because responses left
// unread hold their slots exactly as unanswered ones do. Nor can it know the in-flight cap was set
// high enough to be worth pinning: a cap far below what the pair can carry binds on its own, though
// that shows up as the ceiling being reached at the same time and so is reported as both rather
// than silently as the server. Settling those needs the offered load spread across more than one
// generator process and the totals compared, which this driver does not do; a plateau that holds as
// generators are added is the server's, and one that lifts was the client's all along.

import type { LoadResult } from './loadgen.ts'

// A shortfall is charged to the server only when the generator sat against its cap for essentially
// the whole window. A brief touch is a queue that formed and cleared; it is not a bottleneck.
const CAP_OCCUPANCY_THRESHOLD = 0.9

// The ceiling is a measured rate with its own run-to-run spread, so achieving it is read with a
// margin rather than as an exact equality.
const CEILING_REACHED_FRACTION = 0.9

export type PassVerdict = 'sustained' | 'server_saturated' | 'client_bound' | 'both' | 'indeterminate'

export interface PassClassification {
  serverSaturated: boolean
  clientBound: boolean
  indeterminate: boolean
  verdict: PassVerdict
}

export function classifyPass(pass: LoadResult, ceilingOps: number): PassClassification {
  if (pass.sustained) {
    return { serverSaturated: false, clientBound: false, indeterminate: false, verdict: 'sustained' }
  }

  const serverSaturated = pass.capOccupancy >= CAP_OCCUPANCY_THRESHOLD
  const clientBound = ceilingOps > 0 && pass.achievedRate >= ceilingOps * CEILING_REACHED_FRACTION

  if (serverSaturated && clientBound) {
    return { serverSaturated, clientBound, indeterminate: false, verdict: 'both' }
  }
  if (serverSaturated) {
    return { serverSaturated, clientBound, indeterminate: false, verdict: 'server_saturated' }
  }
  if (clientBound) {
    return { serverSaturated, clientBound, indeterminate: false, verdict: 'client_bound' }
  }
  return { serverSaturated: false, clientBound: false, indeterminate: true, verdict: 'indeterminate' }
}

const VERDICT_ORDER: PassVerdict[] = ['sustained', 'server_saturated', 'client_bound', 'both', 'indeterminate']

// The headline for a rate is the verdict most of its passes reached. With no majority the rate is
// indeterminate, because passes that disagree are not evidence for either cause.
export function majorityVerdict(verdicts: PassVerdict[]): PassVerdict {
  const counts = new Map<PassVerdict, number>()
  for (const verdict of verdicts) {
    counts.set(verdict, (counts.get(verdict) ?? 0) + 1)
  }
  const majority = verdicts.length / 2.0
  for (const verdict of VERDICT_ORDER) {
    if ((counts.get(verdict) ?? 0) > majority) {
      return verdict
    }
  }
  return 'indeterminate'
}

import type { LoadResult } from './loadgen.ts'

const CAP_OCCUPANCY_THRESHOLD = 0.9
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

/**
 * Holds what a restarted primary needs to prove that it lost no acknowledged write before it keeps primary duty.
 *
 * @internal
 */
export interface RestartProof {
  /** Holds the highest sequence that the restarted node has assigned in its change log. */
  highestAssignedSeq: bigint
  /** Holds the time, in milliseconds since the Unix epoch, after which the node gives up and marks itself repairing while other in-sync voters stay silent. */
  deadlineMs: number
  /** Holds the latest applied sequence of this node's log that each other voter has reported. */
  readonly reports: Map<string, bigint>
  /** Is true while the node writes the outcome of the proof to the coordinator. */
  settling: boolean
}

/**
 * Records one sample of the primary's newest assigned sequence, which the lag check compares with each replica's
 * acknowledgements.
 *
 * @internal
 */
export interface AssignedSeqSample {
  /** Holds the newest sequence that the primary had assigned when it took the sample. */
  seq: bigint
  /** Holds the time, in milliseconds since the Unix epoch, at which the primary took the sample. */
  atMs: number
}

/**
 * Holds the primary's view of the in-sync set between coordinator writes: the admissions that it has requested, the
 * highest sequence that it has acknowledged, the samples that time each replica's lag, and the timers that keep the
 * set current.
 *
 * @internal
 */
export class InSyncState {
  /** Holds the timer that admits caught-up replicas and checks the restart proof. */
  reconcileTimer: ReturnType<typeof setInterval> | null = null
  /** Holds the timer that removes lagging replicas. */
  lagTimer: ReturnType<typeof setInterval> | null = null
  /** Is true while an admission pass runs. */
  reconciling = false
  /** Is true while a removal of lagging replicas waits for the coordinator. */
  removing = false
  /** Lists the replicas whose admission this node has requested and the coordinator has not yet answered. */
  readonly pendingAdmissions = new Set<string>()
  /** Holds the highest sequence that this node has acknowledged as primary in its current term. */
  acknowledgedSeq = 0n
  /** Holds the samples of this node's newest assigned sequence, oldest first. */
  readonly assignedSamples: AssignedSeqSample[] = []
  /** Holds the restart proof that this node waits for, or null while it waits for none. */
  restartProof: RestartProof | null = null
}

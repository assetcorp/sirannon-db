/**
 * Holds this node's progress through its rejoin after a repair or a fault, so that the node starts one rebuild at a
 * time, admits itself to the in-sync set only after the copy that rejoins it, and records its quarantined writes once.
 *
 * @internal
 */
export class RejoinState {
  /** Is true while this node starts the full copy that rebuilds it, so that it starts one copy at a time. */
  syncStarting = false
  /** Is true while this node compares its history as a demoted primary, so that it starts no rebuild until it has found whether it must quarantine its rows. */
  evaluatingFormerPrimary = false
  /** Holds the primary term in which this node finished the full copy that rejoins it, or null before it finishes one. */
  copiedTerm: bigint | null = null
  /** Is true while this node asks the coordinator to admit it after that copy, so that it sends one request at a time. */
  admissionInFlight = false
  /** Is true once this node has recorded in its database that it holds quarantined writes, so that it writes the record once. */
  quarantineRecorded = false
}

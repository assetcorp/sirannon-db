/**
 * Holds the sync request in which this replica names its newest history transaction to the primary, while the
 * replica waits for the answer.
 *
 * @internal
 */
export class ResumeRequest {
  /** Identifies the pending request, or is null when none is pending. */
  id: string | null = null
  /** Holds the primary term in which this replica sent the pending request. */
  term: bigint | null = null
  /** Holds the time in milliseconds at which this replica sent the pending request, so that it resends a lost one. */
  requestedAt = 0
  /** Is true when the replica sent the pending request only to find which changes to keep before a full copy. */
  keepsBeforeCopy = false

  /**
   * Records a request that this replica sends now.
   *
   * @param id - The request ID.
   * @param term - The primary term in which the replica sends it.
   * @param keepsBeforeCopy - True when the replica asks only to find which changes to keep before a full copy.
   */
  start(id: string, term: bigint, keepsBeforeCopy: boolean): void {
    this.id = id
    this.term = term
    this.requestedAt = Date.now()
    this.keepsBeforeCopy = keepsBeforeCopy
  }

  /** Forgets the pending request, so that an answer to it no longer matches. */
  clear(): void {
    this.id = null
    this.term = null
    this.requestedAt = 0
    this.keepsBeforeCopy = false
  }

  /**
   * Returns true when a message carries the ID of the pending request.
   *
   * @param requestId - The request ID that the message carries.
   * @returns True while a request is pending and the IDs are equal.
   */
  matches(requestId: string): boolean {
    return this.id !== null && this.id === requestId
  }

  /**
   * Returns true when this replica sent a request in the given term less than `timeoutMs` ago, so that it waits for
   * that answer before it sends another request.
   *
   * @param term - The current primary term.
   * @param timeoutMs - How long, in milliseconds, the replica waits for an answer.
   * @returns True while that answer can still arrive.
   */
  awaitsAnswerIn(term: bigint, timeoutMs: number): boolean {
    return this.id !== null && this.term === term && Date.now() - this.requestedAt < timeoutMs
  }
}

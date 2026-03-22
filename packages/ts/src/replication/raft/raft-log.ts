interface RaftEntry {
  term: number
  data: unknown
}

export class RaftLog {
  private entries: RaftEntry[] = []
  private _commitIndex = 0

  append(entry: { term: number; data: unknown }): number {
    this.entries.push({ term: entry.term, data: entry.data })
    return this.entries.length
  }

  getEntry(index: number): { term: number; data: unknown } | undefined {
    if (index < 1 || index > this.entries.length) {
      return undefined
    }
    return this.entries[index - 1]
  }

  getLastIndex(): number {
    return this.entries.length
  }

  getLastTerm(): number {
    if (this.entries.length === 0) {
      return 0
    }
    return this.entries[this.entries.length - 1].term
  }

  getEntriesFrom(startIndex: number): Array<{ term: number; data: unknown }> {
    if (startIndex < 1) {
      return [...this.entries]
    }
    return this.entries.slice(startIndex - 1)
  }

  truncateFrom(index: number): void {
    if (index < 1) {
      return
    }
    this.entries = this.entries.slice(0, index - 1)
  }

  get commitIndex(): number {
    return this._commitIndex
  }

  setCommitIndex(index: number): void {
    if (index > this._commitIndex) {
      this._commitIndex = index
    }
  }
}

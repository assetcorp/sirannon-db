export type FailureKind = 'shed' | 'timeout' | 'connection' | 'server_error' | 'client_error' | 'unknown'

export interface FailureCategory {
  readonly kind: FailureKind
  readonly code: string
  readonly exemplar: string
}

const MAX_DISTINCT_CODES = 32
const EXEMPLAR_MAX_CHARS = 160

const OVERFLOW_CATEGORY: FailureCategory = {
  kind: 'unknown',
  code: 'OTHER',
  exemplar: `more than ${MAX_DISTINCT_CODES} distinct error codes were seen; the rest are counted here`,
}

export const OPERATION_REJECTED: FailureCategory = {
  kind: 'client_error',
  code: 'OPERATION_REJECTED',
  exemplar: 'the operation rejected instead of resolving its failure category, which is a load generator bug',
}

function truncateExemplar(message: string): string {
  const collapsed = message.replace(/\s+/g, ' ').trim()
  if (collapsed.length <= EXEMPLAR_MAX_CHARS) {
    return collapsed
  }
  return `${collapsed.slice(0, EXEMPLAR_MAX_CHARS)}...`
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

export interface FailureClassifier {
  codeOf(err: unknown): string
  kindOf(code: string, err: unknown): FailureKind
}

export class FailureInterner {
  private readonly byCode = new Map<string, FailureCategory>()
  private readonly classifier: FailureClassifier

  constructor(classifier: FailureClassifier) {
    this.classifier = classifier
  }

  categorize(err: unknown): FailureCategory {
    const code = this.classifier.codeOf(err)
    const known = this.byCode.get(code)
    if (known !== undefined) {
      return known
    }
    if (this.byCode.size >= MAX_DISTINCT_CODES) {
      return OVERFLOW_CATEGORY
    }
    const category: FailureCategory = {
      kind: this.classifier.kindOf(code, err),
      code,
      exemplar: truncateExemplar(errorMessage(err)),
    }
    this.byCode.set(code, category)
    return category
  }
}

interface FailureCount {
  readonly category: FailureCategory
  count: number
}

export class FailureTally {
  private readonly byCategory = new Map<FailureCategory, FailureCount>()

  record(category: FailureCategory): void {
    const existing = this.byCategory.get(category)
    if (existing === undefined) {
      this.byCategory.set(category, { category, count: 1 })
      return
    }
    existing.count += 1
  }

  counts(): FailureCount[] {
    return [...this.byCategory.values()]
  }
}

const NOTE =
  'A shed failure is the server refusing work to protect itself, not a defect: a rate whose failures ' +
  'are all shed measured a server holding its limit. total counts every failure across all passes at ' +
  'this rate, while the error_rate beside it is one pass’s median, so the two are not comparable.'

export function summarizeFailures(tallies: FailureTally[]): Record<string, unknown> {
  const merged = new Map<string, { category: FailureCategory; count: number }>()
  let total = 0
  let capped = false
  for (const tally of tallies) {
    for (const entry of tally.counts()) {
      total += entry.count
      capped = capped || entry.category === OVERFLOW_CATEGORY
      const existing = merged.get(entry.category.code)
      if (existing === undefined) {
        merged.set(entry.category.code, { category: entry.category, count: entry.count })
        continue
      }
      existing.count += entry.count
    }
  }

  const byKind: Record<string, number> = {}
  const categories = [...merged.values()]
    .sort((left, right) => right.count - left.count)
    .map(entry => {
      byKind[entry.category.kind] = (byKind[entry.category.kind] ?? 0) + entry.count
      return {
        kind: entry.category.kind,
        code: entry.category.code,
        count: entry.count,
        share: total > 0 ? entry.count / total : 0.0,
        exemplar: entry.category.exemplar,
      }
    })

  return {
    total,
    by_kind: byKind,
    categories,
    distinct_codes_capped: capped,
    note: NOTE,
  }
}

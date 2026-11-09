import type { DatabaseManifest, OperationManifest } from './manifest.js'

/**
 * Identifies who asked for a generated name, so that the error for a collision gives both operations.
 *
 * @internal
 */
export interface NameClaimant {
  /** The key that two claims of the same name share only when they come from the same operation. */
  owner: string
  /** How an error message refers to the operation. */
  label: string
}

/**
 * Records each generated identifier with the operation that claimed it, and throws when two operations would
 * generate the same one.
 *
 * @internal
 */
export class GeneratedNames {
  private readonly declared = new Map<string, NameClaimant>()

  /**
   * Claims `generated` for the claimant and returns it unchanged.
   *
   * @param generated - The identifier to declare.
   * @param claimant - The operation that the identifier belongs to.
   * @returns The identifier, unchanged.
   * @throws An `Error` when a different operation has already claimed the identifier.
   */
  claim(generated: string, claimant: NameClaimant): string {
    const taken = this.declared.get(generated)
    if (taken !== undefined && taken.owner !== claimant.owner) {
      throw new Error(
        `'${claimant.label}' and '${taken.label}' both generate the identifier '${generated}'. Rename one of them so the generated file declares each name once.`,
      )
    }
    this.declared.set(generated, claimant)
    return generated
  }
}

/**
 * One group of operations to render: a database from the registry, or the shared operations.
 *
 * @internal
 */
export interface OperationGroup {
  /** The PascalCase stem of every name that the generator derives for the group. */
  stem: string
  /** The operations of the group. */
  operations: DatabaseManifest
  /** Whether the group holds the operations that every database serves. */
  shared: boolean
  /** How an error message refers to the group, and the owner key of its claims. */
  claimant: NameClaimant
}

const SHARED_STEM = 'Shared'

/**
 * Lists the groups of a manifest in the order that a generated file declares them.
 *
 * @param manifest - The manifest to render.
 * @returns One group per database, sorted by identifier, and the shared operations last.
 *
 * @internal
 */
export function operationGroups(manifest: OperationManifest): OperationGroup[] {
  const groups = Object.keys(manifest.databases)
    .sort()
    .map(databaseId => ({
      stem: pascalCase(databaseId) || 'Database',
      operations: manifest.databases[databaseId],
      shared: false,
      claimant: { owner: `database/${databaseId}`, label: databaseId },
    }))
  if (manifest.shared !== undefined) {
    groups.push({
      stem: SHARED_STEM,
      operations: manifest.shared,
      shared: true,
      claimant: { owner: 'shared', label: 'sharedOperations' },
    })
  }
  return groups
}

/**
 * Returns the claimant for one operation of a group.
 *
 * @param group - The group that holds the operation.
 * @param kind - Whether the operation is a read or a write.
 * @param name - The registered name of the operation.
 * @returns A claimant whose label gives the group, the kind, and the operation.
 *
 * @internal
 */
export function operationClaimant(group: OperationGroup, kind: 'reads' | 'writes', name: string): NameClaimant {
  return { owner: `${group.claimant.owner}/${kind}/${name}`, label: `${group.claimant.label}.${kind}.${name}` }
}

/**
 * Joins the ASCII letters and digits of a name into PascalCase, dropping every other character.
 *
 * @param value - The name to convert.
 * @returns The PascalCase form, which is empty when the name holds no ASCII letter or digit.
 *
 * @internal
 */
export function pascalCase(value: string): string {
  return value
    .split(/[^A-Za-z0-9]+/)
    .filter(part => part.length > 0)
    .map(part => part[0].toUpperCase() + part.slice(1))
    .join('')
}

/**
 * Returns the names in their first order with each repeat removed.
 *
 * @param names - The names to filter.
 * @returns The distinct names.
 *
 * @internal
 */
export function distinct(names: readonly string[]): string[] {
  return [...new Set(names)]
}

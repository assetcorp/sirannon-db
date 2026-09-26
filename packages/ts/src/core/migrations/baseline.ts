import { MigrationError } from '../errors.js'
import type { Migration } from './types.js'

export const SQLITE_USER_VERSION_MAX = 2_147_483_647

export function resolveEffectiveBaseline(sorted: Migration[]): Migration | undefined {
  const baselines: Migration[] = []
  for (const migration of sorted) {
    const baseline = migration.baseline
    if (baseline === undefined) continue

    if (!Number.isSafeInteger(baseline.through) || baseline.through < 1) {
      throw new MigrationError(
        `Baseline migration ${migration.version}_${migration.name} has an invalid through version: ${baseline.through}`,
        migration.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    if (baseline.through >= migration.version) {
      throw new MigrationError(
        `Baseline migration ${migration.version}_${migration.name} must use a version above the history it supersedes (through ${baseline.through})`,
        migration.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    baselines.push(migration)
  }

  if (baselines.length === 0) return undefined

  for (const baseline of baselines) {
    const through = baseline.baseline
    if (through === undefined) continue
    for (const migration of sorted) {
      if (migration === baseline) continue
      if (migration.version > through.through && migration.version < baseline.version) {
        throw new MigrationError(
          `Migration ${migration.version}_${migration.name} falls inside baseline ${baseline.version}_${baseline.name}'s superseded range (above ${through.through}, below ${baseline.version})`,
          migration.version,
          'MIGRATION_VALIDATION_ERROR',
        )
      }
    }
  }

  return baselines[baselines.length - 1]
}

/**
 * Marks one migration file as a baseline, which a new database runs in place of every version up to and including `through`.
 *
 * A database that already has migration history skips the baseline and applies only the versions that it lacks.
 *
 * @public
 */
export interface BaselineFileOption {
  /**
   * The version of the migration that acts as the baseline.
   */
  version: number
  /**
   * The highest earlier version that a new database skips in favour of the baseline.
   */
  through: number
}

export function applyBaselineOption(migrations: Migration[], baseline: BaselineFileOption | undefined): Migration[] {
  if (baseline === undefined) return migrations

  const target = migrations.find(m => m.version === baseline.version)
  if (target === undefined) {
    throw new MigrationError(
      `Baseline version ${baseline.version} does not match any migration in the set`,
      baseline.version,
      'MIGRATION_VALIDATION_ERROR',
    )
  }

  target.baseline = { through: baseline.through }
  return migrations
}

export function planPendingMigrations(
  sorted: Migration[],
  effectiveBaseline: Migration | undefined,
  appliedVersions: ReadonlySet<number>,
): Migration[] {
  const baseline = effectiveBaseline?.baseline
  if (effectiveBaseline === undefined || baseline === undefined) {
    return sorted.filter(m => !appliedVersions.has(m.version))
  }

  const through = baseline.through

  if (appliedVersions.size === 0) {
    return [effectiveBaseline, ...sorted.filter(m => m.baseline === undefined && m.version > through)]
  }

  if (appliedVersions.has(effectiveBaseline.version)) {
    return sorted.filter(m => m.baseline === undefined && m.version > through && !appliedVersions.has(m.version))
  }

  let highestApplied = 0
  for (const version of appliedVersions) {
    if (version > highestApplied) highestApplied = version
  }

  const pendingBelow =
    highestApplied >= through
      ? []
      : sorted.filter(m => m.baseline === undefined && m.version <= through && !appliedVersions.has(m.version))

  let highestReachable = highestApplied
  for (const migration of pendingBelow) {
    if (migration.version > highestReachable) highestReachable = migration.version
  }

  if (highestReachable < through) {
    throw new MigrationError(
      `Database history reaches version ${highestReachable} but baseline ${effectiveBaseline.version}_${effectiveBaseline.name} supersedes history through version ${through}; the migrations bridging the gap are missing from the input`,
      effectiveBaseline.version,
      'MIGRATION_BASELINE_GAP',
    )
  }

  const pendingAbove = sorted.filter(
    m => m.baseline === undefined && m.version > through && !appliedVersions.has(m.version),
  )

  return [...pendingBelow, ...pendingAbove]
}

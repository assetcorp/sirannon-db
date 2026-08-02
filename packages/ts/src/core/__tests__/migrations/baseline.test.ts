import { describe, expect, it } from 'vitest'
import { MigrationError } from '../../errors.js'
import {
  applyBaselineOption,
  planPendingMigrations,
  resolveEffectiveBaseline,
  SQLITE_USER_VERSION_MAX,
} from '../../migrations/baseline.js'
import type { Migration } from '../../migrations/types.js'

function migration(version: number, extra?: Partial<Migration>): Migration {
  return { version, name: `m${version}`, up: `CREATE TABLE t${version} (id INTEGER PRIMARY KEY)`, ...extra }
}

function expectMigrationError(fn: () => unknown, code: string): MigrationError {
  try {
    fn()
  } catch (err) {
    expect(err).toBeInstanceOf(MigrationError)
    expect((err as MigrationError).code).toBe(code)
    return err as MigrationError
  }
  return expect.unreachable('should have thrown') as never
}

describe('resolveEffectiveBaseline', () => {
  it('returns undefined when no migration is a baseline', () => {
    expect(resolveEffectiveBaseline([migration(1), migration(2)])).toBeUndefined()
  })

  it('returns the baseline migration', () => {
    const baseline = migration(4, { baseline: { through: 3 } })
    expect(resolveEffectiveBaseline([baseline, migration(5)])).toBe(baseline)
  })

  it('returns the highest-version baseline when several exist', () => {
    const first = migration(4, { baseline: { through: 3 } })
    const second = migration(9, { baseline: { through: 8 } })
    expect(resolveEffectiveBaseline([first, migration(5), second])).toBe(second)
  })

  it('rejects a non-integer, zero, or negative through version', () => {
    for (const through of [0, -1, 1.5, Number.NaN]) {
      expectMigrationError(
        () => resolveEffectiveBaseline([migration(4, { baseline: { through } })]),
        'MIGRATION_VALIDATION_ERROR',
      )
    }
  })

  it('rejects a through version at or above the baseline version', () => {
    expectMigrationError(
      () => resolveEffectiveBaseline([migration(4, { baseline: { through: 4 } })]),
      'MIGRATION_VALIDATION_ERROR',
    )
    expectMigrationError(
      () => resolveEffectiveBaseline([migration(4, { baseline: { through: 5 } })]),
      'MIGRATION_VALIDATION_ERROR',
    )
  })

  it('rejects a migration whose version falls inside a superseded range', () => {
    const err = expectMigrationError(
      () => resolveEffectiveBaseline([migration(4), migration(10, { baseline: { through: 3 } })]),
      'MIGRATION_VALIDATION_ERROR',
    )
    expect(err.version).toBe(4)
  })
})

describe('planPendingMigrations', () => {
  const applied = (...versions: number[]) => new Set(versions)

  it('filters applied versions when no baseline exists', () => {
    const plan = planPendingMigrations([migration(1), migration(2), migration(3)], undefined, applied(1, 2))
    expect(plan.map(m => m.version)).toEqual([3])
  })

  it('runs the baseline first on a fresh database and ignores superseded migrations', () => {
    const baseline = migration(4, { baseline: { through: 3 } })
    const plan = planPendingMigrations(
      [migration(1), migration(2), migration(3), baseline, migration(5)],
      baseline,
      applied(),
    )
    expect(plan.map(m => m.version)).toEqual([4, 5])
  })

  it('ignores older baselines on a fresh database', () => {
    const older = migration(4, { baseline: { through: 3 } })
    const newer = migration(9, { baseline: { through: 8 } })
    const plan = planPendingMigrations([older, migration(5), migration(8), newer, migration(10)], newer, applied())
    expect(plan.map(m => m.version)).toEqual([9, 10])
  })

  it('continues past an applied baseline without revisiting superseded versions', () => {
    const baseline = migration(4, { baseline: { through: 3 } })
    const plan = planPendingMigrations([migration(1), migration(3), baseline, migration(5)], baseline, applied(4))
    expect(plan.map(m => m.version)).toEqual([5])
  })

  it('never runs the baseline on a database with existing history', () => {
    const baseline = migration(4, { baseline: { through: 3 } })
    const plan = planPendingMigrations(
      [migration(1), migration(2), migration(3), baseline, migration(5)],
      baseline,
      applied(1, 2, 3),
    )
    expect(plan.map(m => m.version)).toEqual([5])
  })

  it('bridges a mid-history database with the original files before continuing', () => {
    const baseline = migration(4, { baseline: { through: 3 } })
    const plan = planPendingMigrations(
      [migration(1), migration(2), migration(3), baseline, migration(5)],
      baseline,
      applied(1),
    )
    expect(plan.map(m => m.version)).toEqual([2, 3, 5])
  })

  it('throws MIGRATION_BASELINE_GAP when history cannot reach the superseded head', () => {
    const baseline = migration(4, { baseline: { through: 3 } })
    const err = expectMigrationError(
      () => planPendingMigrations([baseline, migration(5)], baseline, applied(1)),
      'MIGRATION_BASELINE_GAP',
    )
    expect(err.version).toBe(4)
  })

  it('accepts history whose highest applied version passed the superseded head', () => {
    const baseline = migration(4, { baseline: { through: 3 } })
    const plan = planPendingMigrations([baseline, migration(5)], baseline, applied(3))
    expect(plan.map(m => m.version)).toEqual([5])
  })

  it('never reruns superseded migrations once history passed the superseded head', () => {
    const baseline = migration(10, { baseline: { through: 5 } })
    const plan = planPendingMigrations(
      [migration(1), migration(2), migration(5), baseline, migration(11)],
      baseline,
      applied(7),
    )
    expect(plan.map(m => m.version)).toEqual([11])
  })
})

describe('applyBaselineOption', () => {
  it('marks the matching migration as a baseline', () => {
    const migrations = [migration(1), migration(4)]
    applyBaselineOption(migrations, { version: 4, through: 3 })
    expect(migrations[1].baseline).toEqual({ through: 3 })
    expect(migrations[0].baseline).toBeUndefined()
  })

  it('returns the set unchanged when no baseline option is given', () => {
    const migrations = [migration(1)]
    expect(applyBaselineOption(migrations, undefined)).toBe(migrations)
    expect(migrations[0].baseline).toBeUndefined()
  })

  it('throws when the baseline version matches no migration', () => {
    expectMigrationError(
      () => applyBaselineOption([migration(1)], { version: 9, through: 3 }),
      'MIGRATION_VALIDATION_ERROR',
    )
  })
})

describe('version bounds', () => {
  it('exposes the PRAGMA user_version ceiling', () => {
    expect(SQLITE_USER_VERSION_MAX).toBe(2_147_483_647)
  })
})

import { MigrationError } from '../errors.js'
import type { Migration } from './types.js'

const FNV_OFFSET_BASIS = 0xcbf29ce484222325n
const FNV_PRIME = 0x100000001b3n
const MASK_64 = 0xffffffffffffffffn

export function migrationChecksum(sql: string): string {
  const bytes = new TextEncoder().encode(sql.replace(/\r\n?/g, '\n').trim())
  let hash = FNV_OFFSET_BASIS
  for (let i = 0; i < bytes.length; i++) {
    hash = ((hash ^ BigInt(bytes[i])) * FNV_PRIME) & MASK_64
  }
  return hash.toString(16).padStart(16, '0')
}

export function migrationContentChecksum(migration: Pick<Migration, 'up'>): string | null {
  return typeof migration.up === 'string' ? migrationChecksum(migration.up) : null
}

export interface AppliedChecksumRow {
  checksum: string | null
}

export interface ChecksumBackfill {
  version: number
  checksum: string
}

export function reconcileMigrationChecksums(
  migrations: Migration[],
  applied: Map<number, AppliedChecksumRow>,
): ChecksumBackfill[] {
  const backfills: ChecksumBackfill[] = []
  for (const migration of migrations) {
    const row = applied.get(migration.version)
    if (!row) continue

    const computed = migrationContentChecksum(migration)
    if (computed === null) continue

    if (row.checksum === null) {
      backfills.push({ version: migration.version, checksum: computed })
      continue
    }

    if (row.checksum !== computed) {
      throw new MigrationError(
        `Migration ${migration.version}_${migration.name} was modified after it was applied`,
        migration.version,
        'MIGRATION_CHECKSUM_MISMATCH',
      )
    }
  }
  return backfills
}

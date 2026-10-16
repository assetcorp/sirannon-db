import { validateDdlSafety } from '../../core/sync/validators.js'
import { ReplicationError } from '../errors.js'

const LEADING_COMMENTS_RE = /^(?:\s|--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/)*/

const SCHEMA_STATEMENT_RE = /^(?:CREATE|ALTER|DROP)\b/i

const SCHEMA_STATEMENT_PREVIEW_LENGTH = 120

/**
 * Returns the schema change that a replica will replay for a statement, or `null` when the statement changes no schema.
 *
 * A statement that opens with `CREATE`, `ALTER`, or `DROP` changes the schema, so the function checks it against the
 * replication allowlist before the primary runs it. Every replica refuses a schema change outside that allowlist, so
 * the primary refuses it too, and the replicas keep the same schema as the primary.
 *
 * @param sql - The statement that the caller wants to run, which can open with whitespace and comments.
 * @returns The statement without its leading whitespace and comments when it is a schema change on the allowlist, or
 * `null` for every statement that changes no schema.
 * @throws ReplicationError when the statement changes the schema in a way that replicas cannot replay, such as a
 * view, a trigger, a rename, `CREATE TABLE ... AS SELECT`, or a statement that contains a semicolon.
 *
 * @internal
 */
export function replicatedSchemaChangeOf(sql: string): string | null {
  const statement = sql.replace(LEADING_COMMENTS_RE, '')
  if (!SCHEMA_STATEMENT_RE.test(statement)) return null
  if (statement.includes(';')) {
    throw new ReplicationError('DDL statements containing semicolons are not allowed for replication safety')
  }
  if (!validateDdlSafety(statement)) {
    throw new ReplicationError(
      `This node refuses the schema change, because replicas apply only CREATE TABLE, ALTER TABLE ... ADD COLUMN, DROP TABLE, CREATE INDEX, CREATE UNIQUE INDEX, and DROP INDEX: ${statement.slice(0, SCHEMA_STATEMENT_PREVIEW_LENGTH)}`,
    )
  }
  return statement
}

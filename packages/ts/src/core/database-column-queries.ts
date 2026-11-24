import { encodeWireArrayRowsInPlace } from './cdc/encoding.js'
import { DatabaseEncryption } from './database-encryption.js'
import { readColumnRows } from './database-reads.js'
import type { Params, QueryWithColumnsOptions, QueryWithColumnsResult } from './types.js'

/**
 * Gives an open database the read that returns each result column's details with every row as an array, and that
 * stops at a row or byte limit.
 *
 * Open a database through {@link Sirannon.open}, which returns a {@link Database} that extends this class.
 *
 * @public
 */
export class DatabaseColumnQueries extends DatabaseEncryption {
  /**
   * Executes a read and returns the name, declared type, and source of each result column, with each row as an
   * array of its values in column order.
   *
   * Two columns of the same name, such as `SELECT a.id, b.id`, each keep their value, and a column named `"1"` keeps
   * its place. Sirannon reads one row at a time and stops after `maxRows` rows, or before the first row that would
   * take the values past `maxBytes`, so that a large table or a recursive query costs no more memory than the rows
   * that the call returns. The read runs on a reader connection as {@link Database.query} does, so SQLite refuses a
   * write, and it fires the same query hooks and metrics.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @param options - The read concern, and the `maxRows` and `maxBytes` limits.
   * @returns The columns, the rows, and `truncated`, which names the limit that stopped the read or is `null` when the
   * read returned every row.
   * @throws A `SirannonError` with `INVALID_READ_LIMIT` when a limit is not a non-negative safe integer, and with
   * `COLUMN_DETAILS_UNSUPPORTED` when the driver's `columnDetails` capability is false.
   */
  async queryWithColumns(
    sql: string,
    params?: Params,
    options?: QueryWithColumnsOptions,
  ): Promise<QueryWithColumnsResult> {
    this.ensureOpen()
    return readColumnRows(this.runtime.reads, sql, params, options, false)
  }

  /**
   * Executes the read of {@link DatabaseColumnQueries.queryWithColumns} and returns its rows with every value encoded
   * for a network response, which the server sends without encoding them again.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @param options - The read concern, and the `maxRows` and `maxBytes` limits.
   * @returns The columns, the encoded rows, and the limit that stopped the read.
   * @internal
   */
  async queryWithColumnsForWire(
    sql: string,
    params?: Params,
    options?: QueryWithColumnsOptions,
  ): Promise<QueryWithColumnsResult> {
    this.ensureOpen()
    const result = await readColumnRows(this.runtime.reads, sql, params, options, true)
    encodeWireArrayRowsInPlace(result.rows)
    return result
  }
}

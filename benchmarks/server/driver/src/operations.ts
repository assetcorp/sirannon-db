// Turn a workload's operation mix into the single function the load generator calls, and describe
// what one operation of that mix costs.
//
// Each operation is bound to its driver once, before any load is offered, so the dialect's SQL is
// chosen and the statement list is shaped at setup rather than on every request. What remains on
// the request path is drawing the parameters and making the call.
//
// The cost description exists because ops/sec is not self-explanatory. One operation of
// `point-select` is one statement and one round trip; one operation of `tpc-c-new-order` is six
// statements, which Sirannon's client sends in one round trip and node-postgres sends in eight.
// Comparing those rates without the postage each pays is how a benchmark misleads by arithmetic, so
// the postage is recorded next to the rate.

import type { Driver, TransactionStatement } from './drivers/driver.ts'
import type { RunOp } from './loadgen.ts'
import type { SeededRng, ZipfianGenerator } from './rng.ts'
import type { Operation, OperationContext, Workload } from './workloads/workload.ts'
import { pickWeighted, statementsPerOperation } from './workloads/workload.ts'

type BoundOperation = (ctx: OperationContext) => Promise<void>

interface WeightedRunner {
  weight: number
  run: BoundOperation
}

function bindOperation(driver: Driver, operation: Operation): BoundOperation {
  const sqlite = driver.dialect === 'sqlite'
  switch (operation.kind) {
    case 'read': {
      const sql = sqlite ? operation.sqliteSql : operation.postgresSql
      return async ctx => {
        await driver.read(sql, operation.params(ctx))
      }
    }
    case 'write': {
      const sql = sqlite ? operation.sqliteSql : operation.postgresSql
      return async ctx => {
        await driver.write(sql, operation.params(ctx))
      }
    }
    case 'rmw': {
      const readSql = sqlite ? operation.readSqliteSql : operation.readPostgresSql
      const writeSql = sqlite ? operation.writeSqliteSql : operation.writePostgresSql
      return async ctx => {
        const params = operation.params(ctx)
        await driver.read(readSql, params.read)
        await driver.write(writeSql, params.write)
      }
    }
    case 'transaction': {
      const sqls = operation.statements.map(statement => (sqlite ? statement.sqliteSql : statement.postgresSql))
      return async ctx => {
        const sets = operation.params(ctx)
        const statements: TransactionStatement[] = []
        for (const [index, sql] of sqls.entries()) {
          const params = sets[index]
          if (params === undefined) {
            throw new Error(
              `operation ${operation.name} drew ${sets.length} parameter sets for ${sqls.length} statements`,
            )
          }
          statements.push({ sql, params })
        }
        await driver.transaction(statements)
      }
    }
  }
}

export function makeRunOp(
  driver: Driver,
  workload: Workload,
  rng: SeededRng,
  zipf: ZipfianGenerator,
  dataSize: number,
): RunOp {
  const runners: WeightedRunner[] = workload.operations.map(operation => ({
    weight: operation.weight,
    run: bindOperation(driver, operation),
  }))
  const ctx: OperationContext = { rng, zipf, dataSize }

  return async (): Promise<boolean> => {
    const runner = pickWeighted(rng, runners)
    try {
      await runner.run(ctx)
      return true
    } catch {
      return false
    }
  }
}

function roundTripsPerOperation(driver: Driver, operation: Operation): number {
  switch (operation.kind) {
    case 'transaction':
      return driver.transactionRoundTrips(operation.statements.length)
    case 'rmw':
      return 2
    default:
      return 1
  }
}

function weightedBy(operations: Operation[], cost: (operation: Operation) => number): number {
  let total = 0
  let weight = 0
  for (const operation of operations) {
    total += operation.weight * cost(operation)
    weight += operation.weight
  }
  return weight > 0 ? total / weight : 0
}

export function operationCost(driver: Driver, workload: Workload): Record<string, unknown> {
  return {
    kinds: [...new Set(workload.operations.map(operation => operation.kind))].sort(),
    statements_per_operation: weightedBy(workload.operations, statementsPerOperation),
    round_trips_per_operation: weightedBy(workload.operations, operation => roundTripsPerOperation(driver, operation)),
    note:
      'One operation is one unit of offered load and one unit of the reported rate. Statements and ' +
      'round trips per operation are the weighted mean across this workload\'s operation mix. Round ' +
      'trips are a property of each engine\'s client, so a rate on a workload costing several round ' +
      'trips per operation is not comparable with a rate on a workload costing one.',
  }
}

import { SeededRng, ZipfianGenerator } from '../rng.ts'
import { microWorkloads } from './micro.ts'
import { oltpWorkloads } from './oltp.ts'
import type { Workload } from './workload.ts'
import { ycsbWorkloads } from './ycsb.ts'

const VALIDATION_DATA_SIZE = 64

function checkTransactionShapes(workload: Workload): void {
  const ctx = {
    rng: new SeededRng(1),
    zipf: new ZipfianGenerator(VALIDATION_DATA_SIZE),
    dataSize: VALIDATION_DATA_SIZE,
  }
  for (const operation of workload.operations) {
    if (operation.kind !== 'transaction') {
      continue
    }
    if (operation.statements.length === 0) {
      throw new Error(`workload ${workload.name} operation ${operation.name} declares no statements`)
    }
    const sets = operation.params(ctx)
    if (sets.length !== operation.statements.length) {
      throw new Error(
        `workload ${workload.name} operation ${operation.name} draws ${sets.length} parameter sets ` +
          `for ${operation.statements.length} statements`,
      )
    }
  }
}

export function buildWorkloads(): Map<string, Workload> {
  const catalogue = [...microWorkloads(), ...ycsbWorkloads(), ...oltpWorkloads()]
  const byName = new Map<string, Workload>()
  for (const workload of catalogue) {
    if (byName.has(workload.name)) {
      throw new Error(`workload ${workload.name} is declared twice`)
    }
    checkTransactionShapes(workload)
    byName.set(workload.name, workload)
  }
  return byName
}

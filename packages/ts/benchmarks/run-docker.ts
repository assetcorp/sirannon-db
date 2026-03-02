import { execSync } from 'node:child_process'
import { join } from 'node:path'

const BENCHMARKS_DIR = import.meta.dirname

function run(script: string) {
  const cmd = `node --expose-gc --import tsx ${join(BENCHMARKS_DIR, script)}`
  console.log(`\n${'='.repeat(60)}`)
  console.log(`Running: ${script}`)
  console.log('='.repeat(60))
  execSync(cmd, { stdio: 'inherit', cwd: join(BENCHMARKS_DIR, '..') })
}

const mode = process.argv[2] ?? 'all'

async function main() {
  switch (mode) {
    case 'e2e':
      run('run-e2e.ts')
      break
    case 'engine':
      run('run-engine.ts')
      break
    case 'all':
      console.log('Running engine benchmarks first, then e2e benchmarks.\n')
      run('run-engine.ts')
      run('run-e2e.ts')
      break
    default:
      console.error(`Unknown mode: ${mode}. Use: all, e2e, or engine`)
      process.exit(1)
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})

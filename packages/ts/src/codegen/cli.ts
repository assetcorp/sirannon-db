import { runCodegen } from './run.js'

runCodegen(process.argv.slice(2)).catch((err: unknown) => {
  process.exitCode = 1
  console.error(err instanceof Error ? err.message : String(err))
})

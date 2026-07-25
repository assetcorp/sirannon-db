import { existsSync } from 'node:fs'
import { dirname, join, sep } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { SirannonError } from '../errors.js'

export interface ResolvedWorkerScript {
  url: URL
  execArgv: string[]
}

function findPackageRoot(startDir: string): string {
  let dir = startDir
  for (;;) {
    if (existsSync(join(dir, 'package.json'))) return dir
    const parent = dirname(dir)
    if (parent === dir) {
      throw new SirannonError(
        'Could not locate the package root to spawn the writer worker',
        'WRITER_WORKER_UNRESOLVED',
      )
    }
    dir = parent
  }
}

export function resolveWorkerScript(): ResolvedWorkerScript {
  const selfPath = fileURLToPath(import.meta.url)
  const root = findPackageRoot(dirname(selfPath))

  const bootstrap = join(root, 'src', 'core', 'worker', 'dev-bootstrap.mjs')
  const built = join(root, 'dist', 'core', 'writer-worker.mjs')

  const runningFromSource = selfPath.startsWith(`${join(root, 'src')}${sep}`)
  const preferred = runningFromSource ? bootstrap : built
  const fallback = runningFromSource ? built : bootstrap

  if (existsSync(preferred)) return { url: pathToFileURL(preferred), execArgv: [] }
  if (existsSync(fallback)) return { url: pathToFileURL(fallback), execArgv: [] }

  throw new SirannonError(
    `Writer-worker script not found under '${root}'. Build the package or run from source.`,
    'WRITER_WORKER_UNRESOLVED',
  )
}

import { spawnSync } from 'node:child_process'
import { existsSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const compiler = join(packageRoot, 'node_modules/typescript/bin/tsc')
const nodeTypes = join(packageRoot, 'node_modules/@types')

const CONSUMERS = [
  {
    fixture: 'device.ts',
    description: 'a browser device carrying no Node type declarations',
    types: [],
  },
  {
    fixture: 'server.ts',
    description: 'a Node server',
    types: ['node'],
  },
]

function tsconfigFor(consumer) {
  return {
    compilerOptions: {
      strict: true,
      target: 'ES2022',
      module: 'ESNext',
      moduleResolution: 'Bundler',
      lib: ['ES2022', 'DOM'],
      skipLibCheck: false,
      noEmit: true,
      typeRoots: [nodeTypes],
      types: consumer.types,
    },
    include: [consumer.fixture],
  }
}

function typecheck(consumer) {
  const project = mkdtempSync(join(tmpdir(), 'sirannon-consumer-types-'))
  try {
    mkdirSync(join(project, 'node_modules/@delali'), { recursive: true })
    symlinkSync(packageRoot, join(project, 'node_modules/@delali/sirannon-db'), 'dir')
    symlinkSync(join(packageRoot, 'scripts/consumer-types', consumer.fixture), join(project, consumer.fixture), 'file')
    writeFileSync(join(project, 'tsconfig.json'), `${JSON.stringify(tsconfigFor(consumer), null, 2)}\n`)

    const result = spawnSync(process.execPath, [compiler, '--noEmit'], {
      cwd: project,
      encoding: 'utf8',
    })

    return { ok: result.status === 0, output: `${result.stdout ?? ''}${result.stderr ?? ''}` }
  } finally {
    rmSync(project, { recursive: true, force: true })
  }
}

if (!existsSync(join(packageRoot, 'dist/core/index.d.ts'))) {
  console.error('Build the package first; dist/core/index.d.ts is absent.')
  process.exit(2)
}

let failed = false
for (const consumer of CONSUMERS) {
  const { ok, output } = typecheck(consumer)
  if (ok) {
    console.log(`ok  ${consumer.fixture} compiles as ${consumer.description}`)
    continue
  }
  failed = true
  console.error(`FAIL ${consumer.fixture} does not compile as ${consumer.description}:\n`)
  console.error(output)
}

if (failed) {
  console.error(
    'Every entry must emit its declarations into one shared chunk, so that a type crossing two entries stays one type, and an entry a browser imports must declare no Node types.',
  )
  process.exit(1)
}

console.log(`\n${CONSUMERS.length} consumers of the published types compile.`)

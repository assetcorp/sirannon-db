import { existsSync, readFileSync } from 'node:fs'
import { builtinModules } from 'node:module'
import { dirname, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')

/**
 * Every export a browser or React Native runtime can load. A Node builtin
 * reachable from any of these is either dead weight the consumer bundles for
 * nothing, or a crash when it runs. `driver/bun` is absent on purpose: Bun
 * implements the Node builtins.
 */
const BROWSER_ENTRIES = [
  'dist/core/index.mjs',
  'dist/client/index.mjs',
  'dist/driver/wa-sqlite.mjs',
  'dist/driver/expo.mjs',
]

const SPECIFIER = /(?:\bfrom\s*|\bimport\s*\(?\s*|\brequire\s*\(\s*)["']([^"']+)["']/g
const builtins = new Set([...builtinModules, ...builtinModules.map(name => `node:${name}`)])

function specifiersOf(file) {
  const source = readFileSync(file, 'utf8')
  const found = new Set()
  for (const match of source.matchAll(SPECIFIER)) {
    found.add(match[1])
  }
  return found
}

function walk(entry) {
  const violations = []
  const seen = new Set()
  const queue = [{ file: resolve(packageRoot, entry), from: [entry] }]

  while (queue.length > 0) {
    const { file, from } = queue.pop()
    if (seen.has(file)) continue
    seen.add(file)

    for (const specifier of specifiersOf(file)) {
      if (builtins.has(specifier)) {
        violations.push({ specifier, chain: [...from, specifier] })
        continue
      }
      if (!specifier.startsWith('.')) continue
      const next = resolve(dirname(file), specifier)
      if (existsSync(next)) {
        queue.push({ file: next, from: [...from, relative(packageRoot, next)] })
      }
    }
  }

  return violations
}

const missing = BROWSER_ENTRIES.filter(entry => !existsSync(resolve(packageRoot, entry)))
if (missing.length > 0) {
  console.error(`Build the package first; these entries are absent:\n  ${missing.join('\n  ')}`)
  process.exit(2)
}

let failed = false
for (const entry of BROWSER_ENTRIES) {
  const violations = walk(entry)
  if (violations.length === 0) {
    console.log(`ok  ${entry}`)
    continue
  }
  failed = true
  console.error(`FAIL ${entry} reaches ${violations.length} Node builtin import(s):`)
  for (const violation of violations) {
    console.error(`  ${violation.chain.join('\n    -> ')}`)
  }
}

if (failed) {
  console.error(
    '\nA browser entry must not reach a Node builtin. Move the runtime-specific code behind a driver that only that runtime loads.',
  )
  process.exit(1)
}

console.log(`\n${BROWSER_ENTRIES.length} browser entries carry no Node builtins.`)

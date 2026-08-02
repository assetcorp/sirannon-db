import { existsSync, readFileSync } from 'node:fs'
import { builtinModules } from 'node:module'
import { dirname, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')

const BROWSER_ENTRIES = [
  'dist/core/index.mjs',
  'dist/client/index.mjs',
  'dist/react/index.mjs',
  'dist/driver/wa-sqlite.mjs',
  'dist/driver/expo.mjs',
]

const TOPOLOGY_RULE = {
  reason:
    'The browser-facing client must not carry cluster topology routing, so an application cannot discover or connect to internal node addresses. Topology routing belongs to dist/client/topology.mjs.',
  markers: ['_getReadEndpoint', '_getWriteEndpoint', 'parseClusterRouting', '/cluster'],
}

const FORBIDDEN_SOURCE = {
  'dist/client/index.mjs': TOPOLOGY_RULE,
  'dist/react/index.mjs': TOPOLOGY_RULE,
}

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
  const reached = []
  const seen = new Set()
  const queue = [{ file: resolve(packageRoot, entry), from: [entry] }]

  while (queue.length > 0) {
    const { file, from } = queue.pop()
    if (seen.has(file)) continue
    seen.add(file)
    reached.push({ file, from })

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

  return { violations, reached }
}

function forbiddenSourceFindings(entry, reached) {
  const rule = FORBIDDEN_SOURCE[entry]
  if (!rule) return []

  const findings = []
  for (const { file, from } of reached) {
    const source = readFileSync(file, 'utf8')
    for (const marker of rule.markers) {
      if (source.includes(marker)) {
        findings.push({ marker, chain: from })
      }
    }
  }
  return findings
}

const missing = BROWSER_ENTRIES.filter(entry => !existsSync(resolve(packageRoot, entry)))
if (missing.length > 0) {
  console.error(`Build the package first; these entries are absent:\n  ${missing.join('\n  ')}`)
  process.exit(2)
}

let failedBuiltins = false
let failedSource = false
for (const entry of BROWSER_ENTRIES) {
  const { violations, reached } = walk(entry)
  const findings = forbiddenSourceFindings(entry, reached)

  if (violations.length === 0 && findings.length === 0) {
    console.log(`ok  ${entry}`)
    continue
  }

  if (violations.length > 0) {
    failedBuiltins = true
    console.error(`FAIL ${entry} reaches ${violations.length} Node builtin import(s):`)
    for (const violation of violations) {
      console.error(`  ${violation.chain.join('\n    -> ')}`)
    }
  }

  if (findings.length > 0) {
    failedSource = true
    console.error(`FAIL ${entry} reaches ${findings.length} forbidden marker(s):`)
    console.error(`  ${FORBIDDEN_SOURCE[entry].reason}`)
    for (const finding of findings) {
      console.error(`  '${finding.marker}' in ${finding.chain.join('\n    -> ')}`)
    }
  }
}

if (failedBuiltins) {
  console.error(
    '\nA browser entry must not reach a Node builtin. Move the runtime-specific code behind a driver that only that runtime loads.',
  )
}

if (failedBuiltins || failedSource) {
  process.exit(1)
}

console.log(`\n${BROWSER_ENTRIES.length} browser entries carry no Node builtins and no forbidden code.`)

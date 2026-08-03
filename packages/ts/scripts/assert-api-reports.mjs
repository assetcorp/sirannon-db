import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Extractor, ExtractorConfig } from '@microsoft/api-extractor'
import { packageName, packageRoot, readEntryPoints, reportFolder } from './api-entry-points.mjs'

const update = process.argv.includes('--update')

const { analysed, withoutDeclarations } = readEntryPoints()

if (!existsSync(join(packageRoot, 'dist'))) {
  console.error('Build the package first; dist is absent.')
  process.exit(2)
}

mkdirSync(reportFolder, { recursive: true })

const expected = new Set(analysed.map(entry => entry.reportFileName))
const actual = new Set(readdirSync(reportFolder).filter(name => name.endsWith('.api.md')))

const missing = [...expected].filter(name => !actual.has(name)).sort()
const orphaned = [...actual].filter(name => !expected.has(name)).sort()

if (missing.length > 0 && !update) {
  for (const name of missing) {
    const entry = analysed.find(candidate => candidate.reportFileName === name)
    console.error(`FAIL ${entry.subpath} has no API report. Run 'pnpm run api:update' to create etc/${name}.`)
  }
}

for (const name of orphaned) {
  console.error(`FAIL etc/${name} matches no export path. Delete it, or restore the export it belongs to.`)
}

const failures = []
const undocumented = []
const deprecations = []
const coverage = []

const workspace = mkdtempSync(join(tmpdir(), 'sirannon-api-extractor-'))

function configFor(entry) {
  return {
    projectFolder: packageRoot,
    mainEntryPointFilePath: entry.declarationPath,
    bundledPackages: [],
    compiler: {
      overrideTsconfig: {
        compilerOptions: {
          target: 'ES2022',
          module: 'ESNext',
          moduleResolution: 'Bundler',
          strict: true,
          skipLibCheck: true,
          types: [],
          lib: ['ES2022', 'DOM'],
        },
        include: [entry.declarationPath],
      },
    },
    apiReport: {
      enabled: true,
      reportFolder,
      reportTempFolder: join(workspace, 'report'),
      reportFileName: entry.reportFileName,
    },
    docModel: {
      enabled: true,
      apiJsonFilePath: join(workspace, 'model', `${entry.reportFileName.replace('.api.md', '')}.api.json`),
    },
    dtsRollup: { enabled: false },
    tsdocMetadata: { enabled: false },
    messages: {
      compilerMessageReporting: { default: { logLevel: 'warning' } },
      extractorMessageReporting: {
        default: { logLevel: 'warning' },
        'ae-forgotten-export': { logLevel: 'none' },
        'ae-undocumented': { logLevel: 'none' },
        'ae-internal-missing-underscore': { logLevel: 'none' },
        'ae-missing-release-tag': { logLevel: 'error', addToApiReportFile: false },
      },
      tsdocMessageReporting: { default: { logLevel: 'warning' } },
    },
  }
}

function walkModel(item, path, visit) {
  const name = item.name ?? ''
  const here = item.kind === 'EntryPoint' || item.kind === 'Package' ? path : path.concat(name)
  if (item.kind !== 'EntryPoint' && item.kind !== 'Package') visit(item, here)
  for (const member of item.members ?? []) walkModel(member, here, visit)
}

function commentLines(docComment) {
  if (!docComment) return []
  return docComment
    .replace(/\/\*\*/g, '')
    .replace(/\*\//g, '')
    .split('\n')
    .map(line => line.replace(/^\s*\*/, '').trim())
}

function summaryOf(docComment) {
  const summary = []
  for (const line of commentLines(docComment)) {
    if (line.startsWith('@')) break
    summary.push(line)
  }
  return summary.join(' ').trim()
}

function deprecationOf(docComment) {
  const lines = commentLines(docComment)
  const start = lines.findIndex(line => line.startsWith('@deprecated'))
  if (start === -1) return null
  const body = [lines[start].slice('@deprecated'.length).trim()]
  for (const line of lines.slice(start + 1)) {
    if (line.startsWith('@')) break
    body.push(line)
  }
  return body.join(' ').trim()
}

for (const entry of analysed) {
  const configObject = configFor(entry)
  let prepared
  try {
    prepared = ExtractorConfig.prepare({
      configObject,
      configObjectFullPath: undefined,
      packageJsonFullPath: join(packageRoot, 'package.json'),
    })
  } catch (error) {
    failures.push(`${entry.subpath}: ${error.message}`)
    continue
  }

  const messages = []
  const result = Extractor.invoke(prepared, {
    localBuild: update,
    showVerboseMessages: false,
    messageCallback: message => {
      message.handled = true
      if (message.category === 'console') return
      if (message.logLevel === 'error' || message.logLevel === 'warning') {
        messages.push(`${message.messageId}: ${message.text} (${message.sourceFilePath ?? entry.subpath})`)
      }
    },
  })

  for (const message of messages) failures.push(`${entry.subpath} ${message}`)

  if (!update && result.apiReportChanged) {
    failures.push(
      `${entry.subpath} API report is out of date. Run 'pnpm run api:update' and commit etc/${entry.reportFileName}.`,
    )
  }

  const modelPath = configObject.docModel.apiJsonFilePath
  if (!existsSync(modelPath)) continue
  const model = JSON.parse(readFileSync(modelPath, 'utf8'))
  const counts = { subpath: entry.subpath, public: 0 }
  walkModel(model, [], (item, path) => {
    const canonical = `${entry.subpath} ${path.join('.')}`
    if (item.releaseTag !== 'Public') return
    counts.public += 1
    if (summaryOf(item.docComment).length === 0) {
      undocumented.push(canonical)
    }
    const deprecation = deprecationOf(item.docComment)
    if (deprecation === null) return
    const namesReplacement = /\{@link\s+[^}]+\}/.test(deprecation)
    const namesRemoval = /\bremoved in\s+v?\d+\.\d+/i.test(deprecation)
    if (!namesReplacement || !namesRemoval) {
      deprecations.push(canonical)
    }
  })
  coverage.push(counts)
}

rmSync(workspace, { recursive: true, force: true })

for (const name of undocumented) {
  console.error(`FAIL ${name} is public and carries no summary.`)
}
for (const name of deprecations) {
  console.error(
    `FAIL ${name} is deprecated without naming a replacement with {@link} and a removal version ('removed in 2.0').`,
  )
}
for (const failure of failures) {
  console.error(`FAIL ${failure}`)
}

for (const entry of coverage) {
  console.log(`ok  ${entry.subpath.padEnd(32)} ${String(entry.public).padStart(5)} documented public items`)
}

for (const subpath of withoutDeclarations) {
  console.log(`skip ${subpath} publishes no types condition, so no API report covers it.`)
}

const failed =
  undocumented.length + deprecations.length + failures.length + orphaned.length + (update ? 0 : missing.length)

if (failed > 0) {
  console.error(`\n${failed} problem(s) in the published API of ${packageName}.`)
  process.exit(1)
}

console.log(
  update
    ? `\n${analysed.length} API reports written to etc/.`
    : `\n${analysed.length} API reports match the published types of ${packageName}.`,
)

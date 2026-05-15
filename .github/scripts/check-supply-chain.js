const { existsSync, lstatSync, readFileSync, readdirSync } = require('node:fs')
const { join } = require('node:path')

const WORKFLOW_DIR = '.github/workflows'
const MINIMUM_RELEASE_AGE_DAYS = 4
const MINIMUM_RELEASE_AGE_MINUTES = MINIMUM_RELEASE_AGE_DAYS * 24 * 60
const PACKAGE_SECTIONS = ['dependencies', 'devDependencies', 'optionalDependencies']
const ALLOWED_REMOTE_DEPENDENCIES = new Map([['uWebSockets.js', 'github:uNetworking/uWebSockets.js#v20.58.0']])
const ALLOWED_REMOTE_LOCKFILE_REFERENCES = [
  'https://codeload.github.com/uNetworking/uWebSockets.js/tar.gz/ba110e817908b56c61d625b02f367b4ec9cfc8ab',
]
const TANSTACK_MALWARE_IOCS = [
  '@tanstack/setup',
  'github:tanstack/router#79ac49eedf774dd4b0cfa308722bc463cfe5885c',
  'router_init.js',
  'tanstack_runner.js',
  '79ac49eedf774dd4b0cfa308722bc463cfe5885c',
]

const main = () => {
  const failures = [...checkWorkflows(), ...checkPackageManifests(), ...checkDependencyCooldowns(), ...checkLockfile()]

  if (failures.length === 0) {
    console.log('Supply-chain gate passed.')
    return
  }

  console.error(`Supply-chain gate failed with ${failures.length} issue(s).`)

  for (const failure of failures) {
    console.error(`- ${failure}`)
    console.error(`::error title=Supply-chain gate failed::${escapeAnnotationValue(failure)}`)
  }

  process.exitCode = 1
}

const checkWorkflows = () => {
  const failures = []

  for (const file of listFiles(WORKFLOW_DIR, /\.(ya?ml)$/)) {
    const content = readFileSync(file, 'utf8')

    if (hasPullRequestTargetTrigger(content)) {
      failures.push(`${file}: pull_request_target is not allowed for this repository`)
    }

    for (const action of findWorkflowActions(content)) {
      if (action.startsWith('./')) {
        continue
      }

      const ref = action.includes('@') ? action.slice(action.lastIndexOf('@') + 1) : ''

      if (!/^[a-f0-9]{40}$/i.test(ref)) {
        failures.push(`${file}: action '${action}' must be pinned to a full commit SHA`)
      }
    }

    if (file !== '.github/workflows/publish.yml' && /^\s*id-token:\s*write\s*$/m.test(content)) {
      failures.push(`${file}: id-token: write is only allowed in publish.yml`)
    }
  }

  return failures
}

const checkPackageManifests = () => {
  const failures = []

  for (const file of listFiles('.', /(^|\/)package\.json$/)) {
    const manifest = JSON.parse(readFileSync(file, 'utf8'))

    for (const section of PACKAGE_SECTIONS) {
      for (const [name, specifier] of Object.entries(manifest[section] || {})) {
        if (specifier === 'latest') {
          failures.push(`${file}: ${section}.${name} must not use latest`)
        }

        if (/^(github:|git\+|https?:\/\/)/.test(specifier) && ALLOWED_REMOTE_DEPENDENCIES.get(name) !== specifier) {
          failures.push(`${file}: ${section}.${name} must not use an unapproved remote git or URL dependency`)
        }
      }
    }
  }

  return failures
}

const checkDependencyCooldowns = () => {
  const failures = []
  const workspace = readRequiredFile(
    'pnpm-workspace.yaml',
    failures,
    'pnpm-workspace.yaml is required for the supply-chain gate',
  )

  if (workspace) {
    const workspaceMatch = workspace.match(/^minimumReleaseAge:\s*(\d+)\s*$/m)
    const workspaceMinutes = workspaceMatch ? Number.parseInt(workspaceMatch[1], 10) : undefined

    if (!Number.isSafeInteger(workspaceMinutes) || workspaceMinutes < MINIMUM_RELEASE_AGE_MINUTES) {
      failures.push(`pnpm-workspace.yaml: minimumReleaseAge must be at least ${MINIMUM_RELEASE_AGE_MINUTES} minutes`)
    }
  }

  const dependabot = readRequiredFile(
    '.github/dependabot.yml',
    failures,
    '.github/dependabot.yml is required for the supply-chain gate',
  )

  if (!dependabot) {
    return failures
  }

  const updates = parseDependabotUpdates(dependabot)

  if (updates.length === 0) {
    failures.push('.github/dependabot.yml: at least one update entry is required')
  }

  for (const update of updates) {
    if (!Number.isSafeInteger(update.cooldownDays) || update.cooldownDays < MINIMUM_RELEASE_AGE_DAYS) {
      failures.push(
        `.github/dependabot.yml: update entry on line ${update.line} must use cooldown.default-days >= ${MINIMUM_RELEASE_AGE_DAYS}`,
      )
    }
  }

  return failures
}

const checkLockfile = () => {
  const failures = []
  const content = readRequiredFile('pnpm-lock.yaml', failures, 'pnpm-lock.yaml is required for the supply-chain gate')

  if (!content) {
    return failures
  }

  for (const indicator of TANSTACK_MALWARE_IOCS) {
    if (content.includes(indicator)) {
      failures.push(`pnpm-lock.yaml contains known TanStack malware indicator '${indicator}'`)
    }
  }

  for (const match of content.matchAll(/https:\/\/codeload\.github\.com\/[^\s}:'"]+/g)) {
    if (!ALLOWED_REMOTE_LOCKFILE_REFERENCES.includes(match[0])) {
      failures.push(`pnpm-lock.yaml contains unapproved remote tarball '${match[0]}'`)
    }
  }

  return failures
}

const readRequiredFile = (path, failures, message) => {
  if (!existsSync(path)) {
    failures.push(message)
    return undefined
  }

  return readFileSync(path, 'utf8')
}

const hasPullRequestTargetTrigger = content => {
  const lines = content.split(/\r?\n/)

  for (let index = 0; index < lines.length; index++) {
    const line = stripYamlComment(lines[index])
    const match = line.match(/^(['"]?)on\1:\s*(.*)$/)

    if (!match) {
      continue
    }

    const inlineValue = match[2].trim()

    if (inlineValue && /\bpull_request_target\b/.test(inlineValue)) {
      return true
    }

    for (let nestedIndex = index + 1; nestedIndex < lines.length; nestedIndex++) {
      const nestedLine = stripYamlComment(lines[nestedIndex])

      if (nestedLine.trim() === '') {
        continue
      }

      if (indentOf(nestedLine) === 0) {
        break
      }

      if (/\bpull_request_target\b/.test(nestedLine)) {
        return true
      }
    }
  }

  return false
}

const findWorkflowActions = content => {
  const actions = []

  for (const line of removeBlockScalars(content)) {
    const match = stripYamlComment(line).match(/^\s*(?:-\s*)?uses:\s*([^\s#]+).*$/)

    if (match) {
      actions.push(unquote(match[1]))
    }
  }

  return actions
}

const parseDependabotUpdates = content => {
  const updates = []
  const lines = content.split(/\r?\n/)
  let inUpdates = false
  let current
  let cooldownIndent

  for (let index = 0; index < lines.length; index++) {
    const line = stripYamlComment(lines[index])

    if (line.trim() === '') {
      continue
    }

    const indent = indentOf(line)
    const trimmed = line.trim()

    if (indent === 0) {
      if (current) {
        updates.push(current)
        current = undefined
      }

      inUpdates = trimmed === 'updates:'
      cooldownIndent = undefined
      continue
    }

    if (!inUpdates) {
      continue
    }

    if (indent === 2 && trimmed.startsWith('- ')) {
      if (current) {
        updates.push(current)
      }

      current = { line: index + 1, cooldownDays: undefined }
      cooldownIndent = undefined
      continue
    }

    if (!current) {
      continue
    }

    if (trimmed === 'cooldown:') {
      cooldownIndent = indent
      continue
    }

    if (cooldownIndent !== undefined && indent > cooldownIndent) {
      const match = trimmed.match(/^default-days:\s*(\d+)\s*$/)

      if (match) {
        current.cooldownDays = Number.parseInt(match[1], 10)
      }
    }
  }

  if (current) {
    updates.push(current)
  }

  return updates
}

const removeBlockScalars = content => {
  const visibleLines = []
  let blockIndent

  for (const line of content.split(/\r?\n/)) {
    const indent = indentOf(line)

    if (blockIndent !== undefined) {
      if (line.trim() === '' || indent > blockIndent) {
        continue
      }

      blockIndent = undefined
    }

    visibleLines.push(line)

    if (/^\s*(?:-\s*)?[\w-]+:\s*[|>]/.test(stripYamlComment(line))) {
      blockIndent = indent
    }
  }

  return visibleLines
}

const listFiles = (directory, pattern) => {
  const files = []

  for (const entry of readdirSync(directory)) {
    const path = join(directory, entry)

    if (entry === 'node_modules' || entry === '.git') {
      continue
    }

    const stats = lstatSync(path)

    if (stats.isSymbolicLink()) {
      continue
    }

    if (stats.isDirectory()) {
      files.push(...listFiles(path, pattern))
      continue
    }

    if (pattern.test(path)) {
      files.push(path)
    }
  }

  return files.sort()
}

const stripYamlComment = line => line.replace(/\s+#.*$/, '')

const indentOf = line => line.match(/^\s*/)[0].length

const unquote = value => value.replace(/^['"]|['"]$/g, '')

const escapeAnnotationValue = value => value.replace(/%/g, '%25').replace(/\r/g, '%0D').replace(/\n/g, '%0A')

main()

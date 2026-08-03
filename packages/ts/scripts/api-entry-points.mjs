import { readdirSync, readFileSync } from 'node:fs'
import { dirname, join, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

export const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
export const reportFolder = join(packageRoot, 'etc')

const packageJson = JSON.parse(readFileSync(join(packageRoot, 'package.json'), 'utf8'))

export const packageName = packageJson.name

function reportFileNameFor(subpath) {
  const slug = subpath === '.' ? '' : `.${subpath.replace(/^\.\//, '').replaceAll('/', '-')}`
  return `sirannon-db${slug}.api.md`
}

function listBuiltDeclarations() {
  const root = join(packageRoot, 'dist')
  const found = []
  const walk = directory => {
    for (const item of readdirSync(directory, { withFileTypes: true })) {
      const full = join(directory, item.name)
      if (item.isDirectory()) {
        walk(full)
        continue
      }
      if (item.name.endsWith('.d.ts')) {
        found.push(`./${relative(packageRoot, full).replaceAll('\\', '/')}`)
      }
    }
  }
  walk(root)
  return found
}

function expandWildcard(subpath, typesPattern) {
  const [prefix, suffix] = typesPattern.split('*')
  const declarations = listBuiltDeclarations()
  const [subpathPrefix, subpathSuffix] = subpath.split('*')
  const expanded = []
  for (const declaration of declarations) {
    if (!declaration.startsWith(prefix) || !declaration.endsWith(suffix)) continue
    const star = declaration.slice(prefix.length, declaration.length - suffix.length)
    if (star.length === 0) continue
    expanded.push({ subpath: `${subpathPrefix}${star}${subpathSuffix ?? ''}`, types: declaration })
  }
  return expanded
}

export function readEntryPoints() {
  const analysed = []
  const withoutDeclarations = []

  for (const [subpath, condition] of Object.entries(packageJson.exports)) {
    if (typeof condition !== 'object' || condition === null) continue
    const types = condition.types
    if (!types) {
      withoutDeclarations.push(subpath)
      continue
    }
    const resolved = types.includes('*') ? expandWildcard(subpath, types) : [{ subpath, types }]
    for (const entry of resolved) {
      analysed.push({
        subpath: entry.subpath,
        declarationPath: join(packageRoot, entry.types),
        reportFileName: reportFileNameFor(entry.subpath),
      })
    }
  }

  analysed.sort((a, b) => a.subpath.localeCompare(b.subpath))
  return { analysed, withoutDeclarations }
}

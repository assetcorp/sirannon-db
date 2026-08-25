import { execFileSync } from 'node:child_process'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const referenceManifest = join(here, '..', '..', 'packages', 'ts', 'package.json')
const opensslPrefix = process.env.OPENSSL_PREFIX ?? '/opt/homebrew/opt/openssl@3'

function sqliteHeaderDirectory() {
  const require = createRequire(pathToFileURL(referenceManifest))
  return join(dirname(require.resolve('better-sqlite3/package.json')), 'deps', 'sqlite3')
}

export const outputPath = join(here, 'encryption-prototype.dylib')

export function build() {
  const sources = ['prototype-crypto.c', 'prototype-keys.c', 'prototype-passthrough.c', 'prototype-vfs.c'].map(name => join(here, name))
  execFileSync(
    'cc',
    ['-O2', '-shared', '-fPIC', '-I', sqliteHeaderDirectory(), '-I', join(opensslPrefix, 'include'),
     '-L', join(opensslPrefix, 'lib'), '-lcrypto', '-o', outputPath, ...sources],
    { stdio: 'inherit' },
  )
  return outputPath
}

if (process.argv[1] === fileURLToPath(import.meta.url)) console.log(`Built ${build()}`)

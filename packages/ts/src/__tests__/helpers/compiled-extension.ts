import { execFileSync } from 'node:child_process'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

export const EXTENSION_PROBE_FUNCTION = 'sirannon_extension_probe'
export const EXTENSION_PROBE_VALUE = 'sirannon-extension-loaded'

const SOURCE = `#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
static void probe(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  (void)argc; (void)argv;
  sqlite3_result_text(ctx, "${EXTENSION_PROBE_VALUE}", -1, SQLITE_STATIC);
}
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_sirannonprobe_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi){
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;
  return sqlite3_create_function(db, "${EXTENSION_PROBE_FUNCTION}", 0, SQLITE_UTF8, 0, probe, 0, 0);
}
`

const SHARED_LIBRARY_SUFFIX: Record<string, string> = {
  darwin: '.dylib',
  win32: '.dll',
}

function sqliteHeaderDirectory(): string {
  const require = createRequire(import.meta.url)
  return join(dirname(require.resolve('better-sqlite3/package.json')), 'deps', 'sqlite3')
}

let built: string | null | undefined

/**
 * Compiles a loadable SQLite extension registering one function, and returns its
 * path. Returns null where the host has no C compiler, so a caller can skip the
 * test rather than fail it.
 */
export function compiledExtensionPath(): string | null {
  if (built !== undefined) return built
  const directory = mkdtempSync(join(tmpdir(), 'sirannon-extension-'))
  const sourcePath = join(directory, 'sirannonprobe.c')
  const libraryPath = join(directory, `sirannonprobe${SHARED_LIBRARY_SUFFIX[process.platform] ?? '.so'}`)
  writeFileSync(sourcePath, SOURCE)
  try {
    execFileSync('cc', ['-O1', '-fPIC', '-shared', '-I', sqliteHeaderDirectory(), '-o', libraryPath, sourcePath], {
      stdio: 'pipe',
    })
    built = libraryPath
  } catch {
    built = null
  }
  return built
}

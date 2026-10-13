import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'

const SOURCE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../../..')
export const PACKAGE_ROOT = resolve(SOURCE_ROOT, '..')
export const CLI_ENTRY = join(SOURCE_ROOT, 'codegen', 'cli.ts')

export interface Diagnostic {
  file: string
  code: number
  line: number
  message: string
}

export interface CodegenWorkspace {
  readonly dir: string
  path(name: string): string
  write(name: string, contents: string): string
  compile(files: readonly string[]): Diagnostic[]
  dispose(): void
}

export function codegenWorkspace(): CodegenWorkspace {
  const dir = mkdtempSync(join(tmpdir(), 'sirannon-codegen-'))
  return {
    dir,
    path: name => join(dir, name),
    write(name, contents) {
      const target = join(dir, name)
      writeFileSync(target, contents)
      return target
    },
    compile(files) {
      const options: ts.CompilerOptions = {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.ESNext,
        moduleResolution: ts.ModuleResolutionKind.Bundler,
        lib: ['lib.es2022.d.ts', 'lib.dom.d.ts'],
        strict: true,
        noEmit: true,
        skipLibCheck: true,
        isolatedModules: true,
        types: ['node'],
        typeRoots: [join(PACKAGE_ROOT, 'node_modules', '@types')],
        paths: {
          '@delali/sirannon-db': [join(SOURCE_ROOT, 'core', 'index.ts')],
          '@delali/sirannon-db/client': [join(SOURCE_ROOT, 'client', 'index.ts')],
        },
      }
      const roots = files.map(file => join(dir, file))
      const program = ts.createProgram(roots, options)
      const diagnostics: Diagnostic[] = []
      for (const root of roots) {
        const source = program.getSourceFile(root)
        if (!source) throw new Error(`the compiler did not load ${root}`)
        for (const found of [...program.getSyntacticDiagnostics(source), ...program.getSemanticDiagnostics(source)]) {
          const start = found.start ?? 0
          diagnostics.push({
            file: root.slice(dir.length + 1),
            code: found.code,
            line: source.getLineAndCharacterOfPosition(start).line + 1,
            message: ts.flattenDiagnosticMessageText(found.messageText, '\n'),
          })
        }
      }
      return diagnostics
    },
    dispose() {
      rmSync(dir, { recursive: true, force: true })
    },
  }
}

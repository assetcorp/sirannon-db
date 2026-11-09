import { resolve } from 'node:path'
import ts from 'typescript'

export type JsonSchema = Record<string, unknown>

const BINARY_TYPE_NAMES = new Set([
  'Uint8Array',
  'Int8Array',
  'Uint16Array',
  'Int16Array',
  'Uint32Array',
  'Int32Array',
  'Float32Array',
  'Float64Array',
  'Buffer',
])
const UNSENDABLE_TYPE_NAMES = new Set(['Map', 'Set', 'Date', 'Promise', ...BINARY_TYPE_NAMES])

function documentation(symbol: ts.Symbol | undefined, checker: ts.TypeChecker): string | undefined {
  if (symbol === undefined) return undefined
  const text = symbol
    .getDocumentationComment(checker)
    .filter(part => part.kind !== 'link' || part.text !== '{@link ')
    .map(part => (part.kind === 'link' ? '' : part.text))
    .join('')
    .replace(/\s*\n\s*/g, ' ')
    .trim()
  return text.length === 0 ? undefined : text
}

function withDescription(schema: JsonSchema, description: string | undefined): JsonSchema {
  return description === undefined ? schema : { ...schema, description }
}

export class SchemaBuilder {
  readonly components = new Map<string, JsonSchema>()
  private readonly typeByName = new Map<string, ts.Type>()

  constructor(
    private readonly program: ts.Program,
    private readonly checker: ts.TypeChecker,
    private readonly anchor: ts.Node,
  ) {}

  declareRoot(name: string, type: ts.Type): void {
    this.typeByName.set(name, type)
  }

  addRoot(name: string, type: ts.Type): void {
    this.typeByName.set(name, type)
    this.components.set(name, {})
    const body = this.bodyOf(type, name)
    this.components.set(name, withDescription(body, documentation(this.namedSymbolOf(type), this.checker)))
  }

  propertiesOf(type: ts.Type, path: string): Array<{ name: string; required: boolean; schema: JsonSchema }> {
    return this.checker.getPropertiesOfType(type).map(property => ({
      name: property.getName(),
      required: (property.flags & ts.SymbolFlags.Optional) === 0,
      schema: withDescription(
        this.schemaOf(this.checker.getTypeOfSymbolAtLocation(property, this.anchor), `${path}.${property.getName()}`),
        documentation(property, this.checker),
      ),
    }))
  }

  private isLibraryDeclared(symbol: ts.Symbol): boolean {
    return (symbol.declarations ?? []).some(declaration => {
      const source = declaration.getSourceFile()
      return this.program.isSourceFileDefaultLibrary(source) || this.program.isSourceFileFromExternalLibrary(source)
    })
  }

  private namedSymbolOf(type: ts.Type): ts.Symbol | undefined {
    const alias = type.aliasSymbol
    if (alias !== undefined) return this.isLibraryDeclared(alias) ? undefined : alias
    const symbol = type.getSymbol()
    if (symbol === undefined || (symbol.flags & (ts.SymbolFlags.Interface | ts.SymbolFlags.Class)) === 0) {
      return undefined
    }
    return this.isLibraryDeclared(symbol) ? undefined : symbol
  }

  private schemaOf(type: ts.Type, path: string): JsonSchema {
    const named = this.namedSymbolOf(type)
    if (named === undefined) return this.bodyOf(type, path)
    const name = named.getName()
    const known = this.typeByName.get(name)
    if (known !== undefined && known !== type) {
      throw new Error(`Two different types take the schema name ${name}, which ${path} refers to`)
    }
    if (known === undefined) this.addRoot(name, type)
    return { $ref: `#/components/schemas/${name}` }
  }

  private bodyOf(type: ts.Type, path: string): JsonSchema {
    if (type.flags & ts.TypeFlags.Unknown || type.flags & ts.TypeFlags.Any) return {}
    if (type.flags & ts.TypeFlags.String) return { type: 'string' }
    if (type.flags & ts.TypeFlags.Number) return { type: 'number' }
    if (type.flags & ts.TypeFlags.Boolean) return { type: 'boolean' }
    if (type.flags & ts.TypeFlags.BooleanLiteral) {
      return { type: 'boolean', const: this.checker.typeToString(type) === 'true' }
    }
    if (type.flags & ts.TypeFlags.Null) return { type: 'null' }
    if (type.flags & (ts.TypeFlags.BigInt | ts.TypeFlags.BigIntLiteral)) {
      throw new Error(`${path} is a bigint, which JSON cannot carry, so send it as a decimal string`)
    }
    if (type.isStringLiteral()) return { type: 'string', const: type.value }
    if (type.isNumberLiteral()) return { type: 'number', const: type.value }
    if (type.isUnion()) return this.unionSchema(type, path)
    if (this.checker.isArrayType(type)) return this.arraySchema(type, path)
    if (this.checker.isTupleType(type)) {
      throw new Error(`${path} is a tuple, which the generated clients cannot describe, so use an array`)
    }
    if (type.flags & ts.TypeFlags.Object || type.isIntersection()) return this.objectSchema(type, path)
    throw new Error(`${path} has the type ${this.checker.typeToString(type)}, which JSON cannot carry`)
  }

  private splitNamedUnions(type: ts.UnionType, members: ts.Type[]): { refs: JsonSchema[]; rest: ts.Type[] } {
    const refs: JsonSchema[] = []
    let rest = members
    for (const [name, known] of this.typeByName) {
      if (known === type || !known.isUnion() || known.aliasSymbol === undefined) continue
      const parts = known.types
      if (parts.every(part => rest.includes(part))) {
        refs.push({ $ref: `#/components/schemas/${name}` })
        rest = rest.filter(member => !parts.includes(member))
      }
    }
    return { refs, rest }
  }

  private unionSchema(type: ts.UnionType, path: string): JsonSchema {
    const { refs, rest: members } = this.splitNamedUnions(
      type,
      type.types.filter(member => !(member.flags & ts.TypeFlags.Undefined)),
    )
    const booleans = members.filter(member => member.flags & ts.TypeFlags.BooleanLiteral)
    const strings = members.filter(member => member.isStringLiteral())
    const numbers = members.filter(member => member.isNumberLiteral())
    const others = members.filter(
      member => !(member.flags & ts.TypeFlags.BooleanLiteral) && !member.isStringLiteral() && !member.isNumberLiteral(),
    )
    const alternatives: JsonSchema[] = [...refs]
    if (strings.length > 0) {
      alternatives.push({ type: 'string', enum: strings.map(member => (member as ts.StringLiteralType).value) })
    }
    if (numbers.length > 0) {
      alternatives.push({ type: 'number', enum: numbers.map(member => (member as ts.NumberLiteralType).value) })
    }
    if (booleans.length === 2) alternatives.push({ type: 'boolean' })
    for (const member of booleans.length === 1 ? booleans : []) alternatives.push(this.bodyOf(member, path))
    for (const member of others) alternatives.push(this.schemaOf(member, path))
    const [only] = alternatives
    if (alternatives.length === 1 && only !== undefined) return only
    return { anyOf: alternatives }
  }

  private arraySchema(type: ts.Type, path: string): JsonSchema {
    const [element] = this.checker.getTypeArguments(type as ts.TypeReference)
    if (element === undefined) throw new Error(`${path} is an array with no element type`)
    return { type: 'array', items: this.schemaOf(element, `${path}[]`) }
  }

  private objectSchema(type: ts.Type, path: string): JsonSchema {
    if (type.getCallSignatures().length > 0 || type.getConstructSignatures().length > 0) {
      throw new Error(`${path} is a function, so leave it out of the type that the route sends or takes`)
    }
    const symbolName = type.getSymbol()?.getName() ?? ''
    if (UNSENDABLE_TYPE_NAMES.has(symbolName)) throw new Error(`${path} is a ${symbolName}, which JSON cannot carry`)
    const schema: JsonSchema = { type: 'object' }
    const properties = this.propertiesOf(type, path)
    if (properties.length > 0) {
      schema.properties = Object.fromEntries(properties.map(property => [property.name, property.schema]))
    }
    const required = properties.filter(property => property.required).map(property => property.name)
    if (required.length > 0) schema.required = required
    const index = this.checker.getIndexInfoOfType(type, ts.IndexKind.String)
    if (index !== undefined) {
      const value = this.schemaOf(index.type, `${path}{}`)
      schema.additionalProperties = Object.keys(value).length === 0 ? true : value
    }
    return schema
  }
}

export interface LoadedRoots<Name extends string> {
  builder: SchemaBuilder
  names: Name[]
  typeOf: (name: Name) => ts.Type
  checker: ts.TypeChecker
}

export function loadRoots<Name extends string>(
  packageDirectory: string,
  sourceFile: string,
  interfaceName: string,
): LoadedRoots<Name> {
  const configPath = resolve(packageDirectory, 'tsconfig.json')
  const config = ts.getParsedCommandLineOfConfigFile(configPath, undefined, {
    ...ts.sys,
    onUnRecoverableConfigFileDiagnostic: diagnostic => {
      throw new Error(ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n'))
    },
  })
  if (config === undefined) throw new Error(`Cannot read ${configPath}`)
  const sourcePath = resolve(packageDirectory, sourceFile)
  const program = ts.createProgram([sourcePath], { ...config.options, noEmit: true })
  const checker = program.getTypeChecker()
  const source = program.getSourceFile(sourcePath)
  if (source === undefined) throw new Error(`Cannot read ${sourcePath}`)
  const declaration = source.statements.find(
    (statement): statement is ts.InterfaceDeclaration =>
      ts.isInterfaceDeclaration(statement) && statement.name.text === interfaceName,
  )
  if (declaration === undefined) throw new Error(`${sourcePath} declares no ${interfaceName}`)
  const rootsType = checker.getTypeAtLocation(declaration)
  const builder = new SchemaBuilder(program, checker, declaration)
  const roots = checker.getPropertiesOfType(rootsType).map(property => ({
    name: property.getName() as Name,
    type: checker.getTypeOfSymbolAtLocation(property, declaration),
  }))
  for (const { name, type } of roots) builder.declareRoot(name, type)
  for (const { name, type } of roots) builder.addRoot(name, type)
  return {
    builder,
    checker,
    names: roots.map(root => root.name),
    typeOf(name) {
      const property = rootsType.getProperty(name)
      if (property === undefined) throw new Error(`${interfaceName} holds no ${name}`)
      return checker.getTypeOfSymbolAtLocation(property, declaration)
    },
  }
}

export function sortedSchemas(builder: SchemaBuilder): Record<string, JsonSchema> {
  return Object.fromEntries([...builder.components].sort(([a], [b]) => a.localeCompare(b)))
}

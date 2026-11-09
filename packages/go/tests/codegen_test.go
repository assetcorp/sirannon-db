package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const codegenDeadline = 60 * time.Second

const hostileRegistryModule = `import { readFileSync } from 'node:fs'
const spec = JSON.parse(readFileSync(new URL('./registry.json', import.meta.url), 'utf8'))
const build = group => ({
  reads: Object.fromEntries(Object.entries(group.reads).map(([name, read]) => [name, {
    args: read.args,
    ...(read.columns === null ? {} : { columns: read.columns }),
    statement: () => ({ sql: 'SELECT * FROM items' }),
  }])),
  writes: Object.fromEntries(Object.entries(group.writes).map(([name, write]) => [name, {
    args: write.args,
    statements: () => ({ sql: 'DELETE FROM items' }),
  }])),
})
export const operations = Object.fromEntries(Object.entries(spec.databases).map(([id, group]) => [id, build(group)]))
export const sharedOperations = build(spec.shared)
`

const hostileRegistry = `{
  "databases": {
    "items": {
      "reads": {
        "itemById": {"args": ["id"], "columns": ["id", "label"]},
        "class": {"args": ["from", "__x", "a b", "ключ", "quo\"te", "\"\"\"", "😀"], "columns": ["Any", "def", "ok", " "]}
      },
      "writes": {"__secret": {"args": ["id"]}, "😀": {"args": []}}
    },
    "9-lives": {"reads": {"naïve": {"args": [], "columns": null}}, "writes": {}},
    "émoji-db": {"reads": {}, "writes": {"ok": {"args": ["x", "x"]}}},
    "tri\"\"\"ple": {"reads": {}, "writes": {"mark": {"args": ["id"]}}}
  },
  "shared": {
    "reads": {"listAll": {"args": [], "columns": ["id"]}, "constructor": {"args": ["id"], "columns": ["id"]}},
    "writes": {"with space": {"args": ["v"]}}
  }
}`

type generatedReference struct {
	Kind string
	Name string
	Args string
	Row  string
}

func runCodegen(t *testing.T, directory string, registry string, output string) ([]byte, error) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, "registry.json"), []byte(registry), 0o600); err != nil {
		t.Fatalf("write the registry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "registry.mjs"), []byte(hostileRegistryModule), 0o600); err != nil {
		t.Fatalf("write the registry module: %v", err)
	}
	cli, err := filepath.Abs(filepath.Join("..", "..", "ts", "dist", "codegen", "cli.mjs"))
	if err != nil {
		t.Fatalf("resolve the code generator: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), codegenDeadline)
	defer cancel()
	command := exec.CommandContext(ctx, "node", cli, "--registry", filepath.Join(directory, "registry.mjs"), "--go", output, "--go-package", "hostile")
	return command.CombinedOutput()
}

func shapeOf(expression ast.Expr, structs map[string][]string) string {
	switch typed := expression.(type) {
	case *ast.SelectorExpr:
		if typed.Sel.Name == "NoArgs" {
			return "none"
		}
		return "open"
	case *ast.MapType:
		return "open"
	case *ast.Ident:
		tags, found := structs[typed.Name]
		if !found {
			return "undeclared " + typed.Name
		}
		return strings.Join(tags, ",")
	}
	return "unexpected"
}

func parseGenerated(t *testing.T, source []byte) []generatedReference {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "operations.go", source, 0)
	if err != nil {
		t.Fatalf("the generated Go does not parse: %v", err)
	}
	structs := map[string][]string{}
	var specs []*ast.ValueSpec
	for _, declaration := range file.Decls {
		general, isGeneral := declaration.(*ast.GenDecl)
		if !isGeneral {
			continue
		}
		for _, spec := range general.Specs {
			switch typed := spec.(type) {
			case *ast.TypeSpec:
				body, isStruct := typed.Type.(*ast.StructType)
				if !isStruct {
					continue
				}
				var tags []string
				for _, field := range body.Fields.List {
					unquoted, err := strconv.Unquote(field.Tag.Value)
					if err != nil {
						t.Fatalf("unquote the tag of %s: %v", typed.Name.Name, err)
					}
					tags = append(tags, reflect.StructTag(unquoted).Get("json"))
				}
				slices.Sort(tags)
				structs[typed.Name.Name] = tags
			case *ast.ValueSpec:
				if general.Tok == token.VAR {
					specs = append(specs, typed)
				}
			}
		}
	}
	var references []generatedReference
	for _, spec := range specs {
		literal := spec.Values[0].(*ast.CompositeLit)
		name, err := strconv.Unquote(literal.Elts[0].(*ast.KeyValueExpr).Value.(*ast.BasicLit).Value)
		if err != nil {
			t.Fatalf("unquote the name of %s: %v", spec.Names[0].Name, err)
		}
		switch typed := literal.Type.(type) {
		case *ast.IndexListExpr:
			references = append(references, generatedReference{"read", name, shapeOf(typed.Indices[0], structs), shapeOf(typed.Indices[1], structs)})
		case *ast.IndexExpr:
			references = append(references, generatedReference{"write", name, shapeOf(typed.Index, structs), ""})
		}
	}
	return references
}

func vetInScratchModule(t *testing.T, source []byte) {
	t.Helper()
	module := t.TempDir()
	client, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("resolve the client module: %v", err)
	}
	sums, err := os.ReadFile(filepath.Join(client, "go.sum"))
	if err != nil {
		t.Fatalf("read the client go.sum: %v", err)
	}
	goMod := "module hostilecheck\n\ngo 1.26\n\nrequire github.com/assetcorp/sirannon-db/packages/go v0.0.0\n\nreplace github.com/assetcorp/sirannon-db/packages/go => " + strconv.Quote(client) + "\n"
	files := map[string][]byte{"go.mod": []byte(goMod), "go.sum": sums, filepath.Join("hostile", "operations.go"): source}
	for name, contents := range files {
		path := filepath.Join(module, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	command := exec.Command("go", "vet", "-mod=mod", "./...")
	command.Dir = module
	command.Env = append(os.Environ(), "GOFLAGS=", "GOPROXY=off", "GOWORK=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("go vet refused the generated package: %v\n%s", err, output)
	}
}

func TestGeneratedGoReferencesCarryEveryHostileOperationNameArgumentAndColumn(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "operations.go")
	if log, err := runCodegen(t, directory, hostileRegistry, output); err != nil {
		t.Fatalf("sirannon-codegen failed: %v\n%s", err, log)
	}
	source, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read the generated Go: %v", err)
	}
	formatted, err := format.Source(source)
	if err != nil || !bytes.Equal(formatted, source) {
		t.Fatalf("the generated Go is not gofmt output: %v", err)
	}
	vetInScratchModule(t, source)

	want := []generatedReference{
		{"read", "itemById", "id", "id,label"},
		{"read", "class", "open", "open"},
		{"write", "__secret", "id", ""},
		{"write", "\U0001F600", "none", ""},
		{"read", "naïve", "none", "open"},
		{"write", "ok", "x", ""},
		{"read", "listAll", "none", "id"},
		{"read", "constructor", "id", "id"},
		{"write", "mark", "id", ""},
		{"write", "with space", "v", ""},
	}
	got := parseGenerated(t, source)
	ordering := func(a, b generatedReference) int { return strings.Compare(a.Kind+"/"+a.Name, b.Kind+"/"+b.Name) }
	slices.SortFunc(got, ordering)
	slices.SortFunc(want, ordering)
	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		t.Fatalf("generated references\n got %s\nwant %s", gotJSON, wantJSON)
	}
}

func TestCodegenRefusesTwoOperationsThatGenerateOneGoIdentifierAndLeavesTheOldFile(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "operations.go")
	previous := []byte("package hostile\n")
	if err := os.WriteFile(output, previous, 0o600); err != nil {
		t.Fatalf("write the previous file: %v", err)
	}
	colliding := `{"databases": {}, "shared": {"reads": {"user-id": {"args": [], "columns": ["id"]}, "userId": {"args": [], "columns": ["id"]}}, "writes": {}}}`

	log, err := runCodegen(t, directory, colliding, output)

	if err == nil {
		t.Fatalf("sirannon-codegen accepted two reads that both generate SharedUserID")
	}
	if !strings.Contains(string(log), "user-id") || !strings.Contains(string(log), "userId") {
		t.Fatalf("the refusal names neither read: %s", log)
	}
	kept, readErr := os.ReadFile(output)
	if readErr != nil || !bytes.Equal(kept, previous) {
		t.Fatalf("the refusal changed the previous file: %v, %q", readErr, kept)
	}
}

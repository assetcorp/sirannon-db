package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	schemaPrefix          = "#/components/schemas/"
	responsePrefix        = "#/components/responses/"
	sqlStatementExtension = "x-sirannon-sql-statement"
	jsonContent           = "application/json"
)

var httpMethods = []string{"get", "put", "post", "patch", "delete"}

var unionKeywords = []string{"anyOf", "oneOf"}

type typeKind int

const (
	kindString typeKind = iota
	kindNumber
	kindBool
	kindAny
	kindOpenObject
	kindNull
	kindLiteral
	kindList
	kindTuple
	kindMap
	kindRef
	kindUnion
	kindNullable
)

type typeRef struct {
	kind     typeKind
	literals []any
	item     *typeRef
	size     int
	name     string
	options  []*typeRef
	hint     string
}

type fieldDef struct {
	jsonName    string
	typ         *typeRef
	required    bool
	description string
}

type definition struct {
	name        string
	description string
	isModel     bool
	fields      []fieldDef
	alias       *typeRef
}

type param struct {
	jsonName    string
	location    string
	required    bool
	typ         *typeRef
	description string
}

type body struct {
	kind        string
	contentType string
	typ         *typeRef
	required    bool
	description string
}

type answer struct {
	status      int
	kind        string
	contentType string
	typ         *typeRef
	description string
}

type operation struct {
	operationID  string
	method       string
	path         string
	tag          string
	summary      string
	params       []param
	body         *body
	answers      []answer
	sqlStatement bool
}

func (op operation) successes() []answer {
	var found []answer
	for _, a := range op.answers {
		if a.status >= 200 && a.status < 300 {
			found = append(found, a)
		}
	}
	return found
}

type api struct {
	definitions map[string]*definition
	order       []string
	operations  []operation
}

type specError struct{ message string }

func (e specError) Error() string { return e.message }

func failf(format string, values ...any) {
	panic(specError{fmt.Sprintf(format, values...)})
}

func catchSpecError(err *error) {
	if recovered := recover(); recovered != nil {
		failure, isSpec := recovered.(specError)
		if !isSpec {
			panic(recovered)
		}
		*err = failure
	}
}

func describe(schema *jsonObject) string {
	return strings.Join(strings.Fields(schema.text("description")), " ")
}

type builder struct {
	schemas   *jsonObject
	responses *jsonObject
	api       *api
}

func (b *builder) define(def *definition) {
	if _, taken := b.api.definitions[def.name]; taken {
		failf("Two schemas would both become the type %s", def.name)
	}
	b.api.definitions[def.name] = def
	b.api.order = append(b.api.order, def.name)
}

func (b *builder) refName(ref string) string {
	name, found := strings.CutPrefix(ref, schemaPrefix)
	if !found {
		failf("The generator reads only component schema references, and found %s", ref)
	}
	if !b.schemas.has(name) {
		failf("The files hold no schema for the reference %s", ref)
	}
	return name
}

func isModel(schema *jsonObject) bool {
	if !schema.has("properties") {
		return false
	}
	switch extra := schema.get("additionalProperties").(type) {
	case nil:
		return true
	case bool:
		return !extra
	case *jsonObject:
		return extra.size() != 0
	default:
		return true
	}
}

func (b *builder) topLevel(name string, schema *jsonObject) {
	if isModel(schema) {
		b.model(name, schema, "")
		return
	}
	b.define(&definition{name: name, description: describe(schema), alias: b.convert(schema, name, "")})
}

func locationOf(where, hint string) string {
	if where != "" {
		return where
	}
	return "`" + hint + "`"
}

func (b *builder) model(name string, schema *jsonObject, where string) *typeRef {
	required := map[string]bool{}
	for _, entry := range schema.list("required") {
		if key, isString := entry.(string); isString {
			required[key] = true
		}
	}
	properties := schema.object("properties")
	description := describe(schema)
	if description == "" && where != "" {
		description = "The shape of " + where + "."
	}
	def := &definition{name: name, description: description, isModel: true}
	b.define(def)
	for _, key := range properties.orderedKeys() {
		value := properties.get(key)
		valueSchema, _ := value.(*jsonObject)
		def.fields = append(def.fields, fieldDef{
			jsonName:    key,
			typ:         b.convert(value, name+pascalWords(key), "the `"+key+"` field of `"+name+"`"),
			required:    required[key],
			description: describe(valueSchema),
		})
	}
	return &typeRef{kind: kindRef, name: name}
}

func discriminatorOf(option *jsonObject) string {
	properties := option.object("properties")
	for _, key := range properties.orderedKeys() {
		if value, isString := properties.object(key).get("const").(string); isString && value != "" {
			return value
		}
	}
	return ""
}

func (b *builder) union(options []any, hint, where string) *typeRef {
	nullable := false
	var rest []*jsonObject
	for _, raw := range options {
		option, ok := raw.(*jsonObject)
		if !ok {
			failf("The union %s holds an option that is not an object", hint)
		}
		if option.text("type") == "null" {
			nullable = true
			continue
		}
		rest = append(rest, option)
	}
	models := 0
	for _, option := range rest {
		if isModel(option) {
			models++
		}
	}
	var converted []*typeRef
	for position, option := range rest {
		if isModel(option) {
			suffix := "Object"
			if models > 1 {
				suffix = fmt.Sprintf("Variant%d", position+1)
				if discriminator := discriminatorOf(option); discriminator != "" {
					suffix = pascalWords(discriminator)
				}
			}
			converted = append(converted, b.convert(option, hint+suffix, "one form of "+locationOf(where, hint)))
			continue
		}
		converted = append(converted, b.convert(option, hint, where))
	}
	var merged *typeRef
	switch len(converted) {
	case 0:
		return &typeRef{kind: kindNull}
	case 1:
		merged = converted[0]
	default:
		merged = &typeRef{kind: kindUnion, options: converted, hint: hint}
	}
	if nullable {
		return &typeRef{kind: kindNullable, item: merged}
	}
	return merged
}

func literalOf(value any, hint string) any {
	switch typed := value.(type) {
	case string, bool:
		return typed
	case json.Number:
		number, err := strconv.ParseFloat(typed.String(), 64)
		if err != nil {
			failf("The schema for %s holds the number %s, which does not fit a float64", hint, typed)
		}
		return number
	default:
		failf("The schema for %s holds a literal of a kind that the generator does not read", hint)
	}
	return nil
}

func (b *builder) convert(raw any, hint, where string) *typeRef {
	if flag, isBool := raw.(bool); isBool && flag {
		return &typeRef{kind: kindAny}
	}
	schema, ok := raw.(*jsonObject)
	if !ok {
		failf("The schema for %s is not an object", hint)
	}
	if schema.size() == 0 || (schema.size() == 1 && schema.has("description")) {
		return &typeRef{kind: kindAny}
	}
	if ref := schema.text("$ref"); ref != "" {
		return &typeRef{kind: kindRef, name: b.refName(ref)}
	}
	for _, keyword := range unionKeywords {
		if schema.has(keyword) {
			return b.union(schema.list(keyword), hint, where)
		}
	}
	if schema.has("allOf") {
		failf("The schema for %s uses allOf, which this generator does not read", hint)
	}
	if schema.has("const") {
		return &typeRef{kind: kindLiteral, literals: []any{literalOf(schema.get("const"), hint)}}
	}
	if schema.has("enum") {
		var literals []any
		for _, value := range schema.list("enum") {
			literals = append(literals, literalOf(value, hint))
		}
		return &typeRef{kind: kindLiteral, literals: literals}
	}
	kind := schema.text("type")
	switch kind {
	case "string":
		return &typeRef{kind: kindString}
	case "number", "integer":
		return &typeRef{kind: kindNumber}
	case "boolean":
		return &typeRef{kind: kindBool}
	case "null":
		return &typeRef{kind: kindNull}
	case "array":
		var items any = true
		if schema.has("items") {
			items = schema.get("items")
		}
		item := b.convert(items, hint+"Item", "one item of "+locationOf(where, hint))
		low, lowOK := schema.get("minItems").(json.Number)
		high, highOK := schema.get("maxItems").(json.Number)
		if lowOK && highOK && low == high {
			size, err := strconv.Atoi(low.String())
			if err == nil && size > 0 {
				return &typeRef{kind: kindTuple, item: item, size: size}
			}
		}
		return &typeRef{kind: kindList, item: item}
	case "object", "":
		if kind == "" && !schema.has("properties") && !schema.has("additionalProperties") {
			return &typeRef{kind: kindAny}
		}
		if isModel(schema) {
			return b.model(hint, schema, where)
		}
		if extra, isObject := schema.get("additionalProperties").(*jsonObject); isObject && extra.size() > 0 {
			return &typeRef{kind: kindMap, item: b.convert(extra, hint+"Value", "each value of "+locationOf(where, hint))}
		}
		return &typeRef{kind: kindOpenObject}
	}
	failf("The schema for %s has type %q, which this generator does not read", hint, kind)
	return nil
}

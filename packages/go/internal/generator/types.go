package main

import (
	"fmt"
	"strings"
)

type valueClass int

const (
	classScalar valueClass = iota
	classStruct
	classReference
)

type variant struct {
	field    string
	goType   string
	jsonKind string
	pointer  bool
	required []string
	known    []string
	consts   []string
	open     bool
}

type unionPlan struct {
	name        string
	description string
	variants    []variant
}

type stringPlan struct {
	name        string
	description string
	values      []string
}

type planner struct {
	api       *api
	unions    map[string]*unionPlan
	unionList []string
	taken     map[string]string
}

func newPlanner(spec *api) *planner {
	p := &planner{api: spec, unions: map[string]*unionPlan{}, taken: map[string]string{}}
	for _, name := range spec.order {
		goName := exportedName(name)
		if previous, clash := p.taken[goName]; clash {
			failf("The schemas %s and %s would both become the Go type %s", previous, name, goName)
		}
		p.taken[goName] = name
	}
	return p
}

func (p *planner) definition(name string) *definition {
	def, found := p.api.definitions[name]
	if !found {
		failf("The generator found no schema called %s", name)
	}
	return def
}

func literalKind(t *typeRef) string {
	kind := ""
	for _, value := range t.literals {
		var current string
		switch value.(type) {
		case string:
			current = "string"
		case float64:
			current = "number"
		case bool:
			current = "bool"
		}
		if kind != "" && kind != current {
			failf("An enum mixes %s and %s values, which one Go type cannot hold", kind, current)
		}
		kind = current
	}
	return kind
}

func stringLiterals(t *typeRef) []string {
	var values []string
	for _, value := range t.literals {
		if text, isString := value.(string); isString {
			values = append(values, text)
		}
	}
	return values
}

func (p *planner) jsonKind(t *typeRef) string {
	switch t.kind {
	case kindString:
		return "string"
	case kindNumber:
		return "number"
	case kindBool:
		return "bool"
	case kindLiteral:
		return literalKind(t)
	case kindList, kindTuple:
		return "array"
	case kindMap, kindOpenObject:
		return "object"
	case kindRef:
		def := p.definition(t.name)
		if def.isModel {
			return "object"
		}
		return p.jsonKind(def.alias)
	case kindUnion:
		kinds := map[string]bool{}
		for _, option := range t.options {
			kinds[p.jsonKind(option)] = true
		}
		if len(kinds) == 1 {
			for kind := range kinds {
				return kind
			}
		}
		return "mixed"
	}
	return "any"
}

func modelVariant(def *definition, goType string) variant {
	v := variant{field: goType, goType: goType, jsonKind: "object", pointer: true}
	for _, field := range def.fields {
		v.known = append(v.known, field.jsonName)
		if field.required {
			v.required = append(v.required, field.jsonName)
		}
		if field.typ.kind == kindLiteral && len(field.typ.literals) == 1 {
			if text, isString := field.typ.literals[0].(string); isString {
				v.consts = append(v.consts, field.jsonName, text)
			}
		}
	}
	return v
}

func (p *planner) objectVariants(members []*typeRef, owner string) []variant {
	var variants []variant
	mapLike := 0
	for _, member := range members {
		if member.kind == kindRef && p.definition(member.name).isModel {
			variants = append(variants, modelVariant(p.definition(member.name), exportedName(member.name)))
			continue
		}
		mapLike++
		if mapLike > 1 {
			failf("The union %s holds two map options, which a Go union cannot tell apart", owner)
		}
		goType := p.goType(member, owner+"Map")
		field := "Map"
		if member.kind == kindRef {
			field = goType
		}
		variants = append(variants, variant{field: field, goType: goType, jsonKind: "object", open: true})
	}
	return variants
}

func (p *planner) planVariants(t *typeRef, owner string) []variant {
	groups := map[string][]*typeRef{}
	var order []string
	for _, option := range t.options {
		kind := p.jsonKind(option)
		if kind == "any" || kind == "mixed" {
			failf("The union %s holds an option that no Go union field can hold", owner)
		}
		if _, seen := groups[kind]; !seen {
			order = append(order, kind)
		}
		groups[kind] = append(groups[kind], option)
	}
	var variants []variant
	for _, kind := range order {
		members := groups[kind]
		switch kind {
		case "string", "number", "bool":
			goType := map[string]string{"string": "string", "number": "float64", "bool": "bool"}[kind]
			field := map[string]string{"string": "String", "number": "Number", "bool": "Bool"}[kind]
			if len(members) == 1 && members[0].kind == kindRef {
				goType = exportedName(members[0].name)
				field = goType
			}
			variants = append(variants, variant{field: field, goType: goType, jsonKind: kind, pointer: true})
		case "array":
			if len(members) > 1 {
				failf("The union %s holds two list options, which a Go union cannot tell apart", owner)
			}
			goType := p.goType(members[0], owner+"List")
			field := "List"
			if members[0].kind == kindTuple {
				field = "Tuple"
			}
			variants = append(variants, variant{
				field: field, goType: goType, jsonKind: kind, pointer: members[0].kind == kindTuple,
			})
		case "object":
			variants = append(variants, p.objectVariants(members, owner)...)
		}
	}
	seen := map[string]bool{}
	for _, v := range variants {
		if seen[v.field] {
			failf("The union %s would hold two fields called %s", owner, v.field)
		}
		seen[v.field] = true
	}
	return variants
}

func (p *planner) unionType(t *typeRef, name, description string) string {
	variants := p.planVariants(t, name)
	if len(variants) == 1 {
		return variants[0].goType
	}
	if existing, found := p.unions[name]; found {
		return existing.name
	}
	if owner, clash := p.taken[name]; clash {
		failf("The union %s would take the Go name of the schema %s", name, owner)
	}
	p.unions[name] = &unionPlan{name: name, description: description, variants: variants}
	p.unionList = append(p.unionList, name)
	return name
}

func (p *planner) goType(t *typeRef, hint string) string {
	switch t.kind {
	case kindString:
		return "string"
	case kindNumber:
		return "float64"
	case kindBool:
		return "bool"
	case kindAny, kindNull:
		return "any"
	case kindOpenObject:
		return "map[string]any"
	case kindLiteral:
		return map[string]string{"string": "string", "number": "float64", "bool": "bool"}[literalKind(t)]
	case kindList:
		return "[]" + p.elementType(t.item, hint+"Item")
	case kindTuple:
		return fmt.Sprintf("[%d]%s", t.size, p.elementType(t.item, hint+"Item"))
	case kindMap:
		return "map[string]" + p.elementType(t.item, hint+"Value")
	case kindRef:
		return exportedName(t.name)
	case kindUnion:
		name := exportedName(t.hint)
		if name == "" {
			name = exportedName(hint)
		}
		return p.unionType(t, name, "")
	case kindNullable:
		return p.elementType(t, hint)
	}
	failf("The generator cannot give %s a Go type", hint)
	return ""
}

func (p *planner) elementType(t *typeRef, hint string) string {
	if t.kind != kindNullable {
		return p.goType(t, hint)
	}
	inner := p.goType(t.item, hint)
	if p.classOf(t.item) == classReference {
		return inner
	}
	return "*" + inner
}

func (p *planner) classOf(t *typeRef) valueClass {
	switch t.kind {
	case kindString, kindNumber, kindBool, kindLiteral:
		return classScalar
	case kindTuple:
		return classStruct
	case kindUnion:
		variants := p.planVariants(t, t.hint)
		if len(variants) == 1 {
			return variantClass(variants[0])
		}
		return classStruct
	case kindRef:
		def := p.definition(t.name)
		if def.isModel {
			return classStruct
		}
		return p.classOf(def.alias)
	}
	return classReference
}

func variantClass(v variant) valueClass {
	if v.open || strings.HasPrefix(v.goType, "[]") {
		return classReference
	}
	if v.jsonKind == "object" || strings.HasPrefix(v.goType, "[") {
		return classStruct
	}
	return classScalar
}

type fieldShape struct {
	goType string
	tag    string
}

func (p *planner) fieldShape(t *typeRef, required bool, hint string) fieldShape {
	nullable := t.kind == kindNullable
	inner := t
	if nullable {
		inner = t.item
	}
	base := p.goType(inner, hint)
	class := p.classOf(inner)
	switch {
	case nullable && !required:
		return fieldShape{goType: "Nullable[" + base + "]", tag: ",omitzero"}
	case nullable:
		if class == classReference {
			return fieldShape{goType: base}
		}
		return fieldShape{goType: "*" + base}
	case required:
		return fieldShape{goType: base}
	case class == classReference && strings.HasPrefix(base, "any"):
		return fieldShape{goType: base, tag: ",omitempty"}
	case class == classReference:
		return fieldShape{goType: base, tag: ",omitzero"}
	default:
		return fieldShape{goType: "*" + base, tag: ",omitempty"}
	}
}

func (p *planner) aliasPlan(def *definition) (string, *stringPlan) {
	alias := def.alias
	name := exportedName(def.name)
	switch alias.kind {
	case kindLiteral:
		if literalKind(alias) == "string" {
			return "string", &stringPlan{name: name, description: def.description, values: stringLiterals(alias)}
		}
	case kindUnion:
		variants := p.planVariants(alias, name)
		if len(variants) == 1 && variants[0].jsonKind == "string" {
			var values []string
			for _, option := range alias.options {
				if option.kind == kindLiteral {
					values = append(values, stringLiterals(option)...)
				}
			}
			return "string", &stringPlan{name: name, description: def.description, values: values}
		}
		if len(variants) > 1 {
			p.unions[name] = &unionPlan{name: name, description: def.description, variants: variants}
			p.unionList = append(p.unionList, name)
			return "union", nil
		}
		return p.goType(alias, name), nil
	case kindNullable:
		failf("The schema %s is only a nullable value, which needs no Go type of its own", def.name)
	}
	return p.goType(alias, name), nil
}

package main

import (
	"fmt"
	"strings"
)

type chunk struct {
	text string
}

func typeLead(name, description, fallback string) string {
	if description == "" {
		return docComment("", name+" "+fallback, "")
	}
	return docComment("", name+":", description)
}

func (p *planner) renderModel(def *definition) chunk {
	name := exportedName(def.name)
	var out strings.Builder
	out.WriteString(typeLead(name, def.description, "is a JSON object that the API sends or takes."))
	fmt.Fprintf(&out, "type %s struct {\n", name)
	seen := map[string]string{}
	for _, field := range def.fields {
		fieldName := exportedName(field.jsonName)
		if previous, clash := seen[fieldName]; clash {
			failf("The fields %s and %s of %s would both become the Go field %s", previous, field.jsonName, def.name, fieldName)
		}
		seen[fieldName] = field.jsonName
		shape := p.fieldShape(field.typ, field.required, def.name+pascalWords(field.jsonName))
		description := field.description
		if literal := stringChoices(field.typ); literal != nil {
			description = strings.TrimSpace(description + " " + oneOfText(literal.literals))
		}
		if description != "" {
			out.WriteString(docComment("\t", "", description))
		}
		fmt.Fprintf(&out, "\t%s %s `json:%s`\n", fieldName, shape.goType, quoteGo(field.jsonName+shape.tag))
	}
	out.WriteString("}\n")
	return chunk{text: out.String()}
}

func stringChoices(t *typeRef) *typeRef {
	if t.kind == kindNullable {
		t = t.item
	}
	if t.kind == kindLiteral && literalKind(t) == "string" && len(t.literals) > 1 {
		return t
	}
	return nil
}

func renderStringType(plan *stringPlan) chunk {
	var out strings.Builder
	out.WriteString(typeLead(plan.name, plan.description, "is a string that the API reads."))
	fmt.Fprintf(&out, "type %s string\n", plan.name)
	if len(plan.values) == 0 {
		return chunk{text: out.String()}
	}
	out.WriteString("\nconst (\n")
	seen := map[string]string{}
	for _, value := range plan.values {
		constant := constantName(plan.name, value)
		if previous, clash := seen[constant]; clash {
			failf("The values %q and %q of %s would both become the Go constant %s", previous, value, plan.name, constant)
		}
		seen[constant] = value
		fmt.Fprintf(&out, "\t%s %s = %s\n", constant, plan.name, quoteGo(value))
	}
	out.WriteString(")\n")
	return chunk{text: out.String()}
}

func (p *planner) renderAlias(def *definition) chunk {
	name := exportedName(def.name)
	kind, plan := p.aliasPlan(def)
	switch kind {
	case "string":
		return renderStringType(plan)
	case "union":
		return chunk{}
	}
	var out strings.Builder
	out.WriteString(typeLead(name, def.description, "is a value that the API sends or takes."))
	if def.alias.kind == kindRef || def.alias.kind == kindAny {
		fmt.Fprintf(&out, "type %s = %s\n", name, kind)
	} else {
		fmt.Fprintf(&out, "type %s %s\n", name, kind)
	}
	return chunk{text: out.String()}
}

func shapesName(name string) string {
	words := splitWords(name)
	words[0] = strings.ToLower(words[0])
	return strings.Join(words, "") + "Shapes"
}

func quotedList(values []string) string {
	var quoted []string
	for _, value := range values {
		quoted = append(quoted, quoteGo(value))
	}
	return "[]string{" + strings.Join(quoted, ", ") + "}"
}

func renderUnionDecoder(out *strings.Builder, plan *unionPlan) []variant {
	var objects []variant
	for _, kind := range []string{"string", "number", "bool", "array", "object"} {
		var matching []variant
		for _, v := range plan.variants {
			if v.jsonKind == kind {
				matching = append(matching, v)
			}
		}
		if len(matching) == 0 {
			continue
		}
		fmt.Fprintf(out, "\tcase %s:\n", quoteGo(kind))
		if kind != "object" || len(matching) == 1 {
			fmt.Fprintf(out, "\t\treturn decodeWire(data, &u.%s)\n", matching[0].field)
			continue
		}
		objects = matching
		fmt.Fprintf(out, "\t\tswitch chooseVariant(data, %s) {\n", shapesName(plan.name))
		for position, v := range matching {
			fmt.Fprintf(out, "\t\tcase %d:\n\t\t\treturn decodeWire(data, &u.%s)\n", position, v.field)
		}
		out.WriteString("\t\t}\n")
	}
	return objects
}

func renderUnion(plan *unionPlan) chunk {
	var out strings.Builder
	out.WriteString(typeLead(plan.name, plan.description, "holds one of several forms."))
	if plan.description != "" {
		out.WriteString("//\n")
	}
	out.WriteString("// Set exactly one field. A value with no field set, or with two, fails to encode.\n")
	fmt.Fprintf(&out, "type %s struct {\n", plan.name)
	var fields []string
	for _, v := range plan.variants {
		goType := v.goType
		if v.pointer {
			goType = "*" + goType
		}
		fmt.Fprintf(&out, "\t%s %s\n", v.field, goType)
		fields = append(fields, "u."+v.field)
	}
	out.WriteString("}\n\n")
	out.WriteString("// MarshalJSON writes the one field that is set in the tagged value encoding, and fails when none or\n")
	out.WriteString("// several are set.\n")
	fmt.Fprintf(&out, "func (u %s) MarshalJSON() ([]byte, error) {\n", plan.name)
	fmt.Fprintf(&out, "\treturn marshalUnion(%s, %s)\n}\n\n", quoteGo(plan.name), strings.Join(fields, ", "))
	out.WriteString("// UnmarshalJSON sets the field whose form matches the JSON, decoding the tagged value encoding, and\n")
	out.WriteString("// fails when no form matches.\n")
	fmt.Fprintf(&out, "func (u *%s) UnmarshalJSON(data []byte) error {\n", plan.name)
	fmt.Fprintf(&out, "\t*u = %s{}\n\tswitch jsonKindOf(data) {\n", plan.name)
	objects := renderUnionDecoder(&out, plan)
	out.WriteString("\tcase \"null\":\n\t\treturn nil\n\t}\n")
	fmt.Fprintf(&out, "\treturn unionMismatch(%s, data)\n}\n", quoteGo(plan.name))
	if len(objects) > 1 {
		fmt.Fprintf(&out, "\nvar %s = []variantShape{\n", shapesName(plan.name))
		for _, v := range objects {
			if v.open {
				out.WriteString("\t{open: true},\n")
				continue
			}
			fmt.Fprintf(&out, "\t{required: %s, known: %s, consts: %s},\n", quotedList(v.required), quotedList(v.known), quotedList(v.consts))
		}
		out.WriteString("}\n")
	}
	return chunk{text: out.String()}
}

func (p *planner) modelChunks() []chunk {
	var chunks []chunk
	for _, name := range p.api.order {
		def := p.definition(name)
		var rendered chunk
		if def.isModel {
			rendered = p.renderModel(def)
		} else {
			rendered = p.renderAlias(def)
		}
		if rendered.text != "" {
			chunks = append(chunks, rendered)
		}
	}
	return chunks
}

func (p *planner) unionChunks(start int) ([]chunk, int) {
	var chunks []chunk
	for position := start; position < len(p.unionList); position++ {
		chunks = append(chunks, renderUnion(p.unions[p.unionList[position]]))
	}
	return chunks, len(p.unionList)
}

package main

import (
	"fmt"
	"strings"
)

type operationPlan struct {
	op         operation
	name       string
	pathArgs   []param
	queryArgs  []param
	resultType string
	resultKind string
	answerType string
}

func (p *planner) planOperation(op operation) *operationPlan {
	plan := &operationPlan{op: op, name: exportedName(op.operationID)}
	for _, prm := range op.params {
		if prm.location == "path" {
			plan.pathArgs = append(plan.pathArgs, prm)
		} else {
			plan.queryArgs = append(plan.queryArgs, prm)
		}
	}
	successes := op.successes()
	kinds := map[string]bool{}
	types := map[string]bool{}
	for _, a := range successes {
		kinds[a.kind] = true
		if a.kind == "json" {
			types[p.goType(a.typ, plan.name+"Answer")] = true
		}
	}
	switch {
	case len(kinds) == 1 && kinds["empty"]:
		plan.resultKind = "empty"
	case len(kinds) == 1 && kinds["text"]:
		plan.resultKind = "text"
		plan.resultType = "string"
	case len(kinds) == 1 && kinds["json"] && len(types) == 1:
		plan.resultKind = "json"
		first := successes[0]
		plan.resultType = p.goType(first.typ, plan.name+"Answer")
		if p.classOf(first.typ) == classStruct {
			plan.resultKind = "jsonPointer"
			plan.resultType = "*" + plan.resultType
		}
	case len(kinds) == 1 && kinds["json"]:
		plan.resultKind = "choice"
		plan.answerType = plan.name + "Answer"
		if owner, clash := p.taken[plan.answerType]; clash {
			failf("The answer type of %s would take the Go name of the schema %s", op.operationID, owner)
		}
		p.taken[plan.answerType] = plan.answerType
		plan.resultType = "*" + plan.answerType
	default:
		failf("Operation %s answers success in forms that one Go method cannot return", op.operationID)
	}
	return plan
}

func zeroResult(plan *operationPlan) string {
	switch plan.resultKind {
	case "text":
		return `""`
	case "json":
		if strings.HasPrefix(plan.resultType, "map[") || strings.HasPrefix(plan.resultType, "[]") {
			return "nil"
		}
		return "zeroOf[" + plan.resultType + "]()"
	}
	return "nil"
}

func (p *planner) bodyArgument(plan *operationPlan) string {
	b := plan.op.body
	if b == nil {
		return ""
	}
	if b.kind == "text" {
		return "body string"
	}
	goType := p.goType(b.typ, plan.name+"Body")
	if !b.required && p.classOf(b.typ) != classReference {
		goType = "*" + goType
	}
	return "body " + goType
}

func (p *planner) renderParams(plan *operationPlan) string {
	if len(plan.queryArgs) == 0 {
		return ""
	}
	name := plan.name + "Params"
	if owner, clash := p.taken[name]; clash {
		failf("The query type of %s would take the Go name of the schema %s", plan.op.operationID, owner)
	}
	p.taken[name] = name
	var out strings.Builder
	out.WriteString(docComment("", name, "holds the query settings of "+plan.name+"."))
	fmt.Fprintf(&out, "type %s struct {\n", name)
	for _, prm := range plan.queryArgs {
		goType := p.goType(prm.typ, plan.name+pascalWords(prm.jsonName))
		if !prm.required && p.classOf(prm.typ) != classReference {
			goType = "*" + goType
		}
		if prm.description != "" {
			out.WriteString(docComment("\t", "", prm.description))
		}
		fmt.Fprintf(&out, "\t%s %s\n", exportedName(prm.jsonName), goType)
	}
	out.WriteString("}\n\n")
	return out.String()
}

func (p *planner) queryLine(prm param) string {
	field := "params." + exportedName(prm.jsonName)
	pointer := !prm.required && p.classOf(prm.typ) != classReference
	value := field
	if pointer {
		value = "*" + field
	}
	var formatted string
	switch p.jsonKind(prm.typ) {
	case "string":
		formatted = "string(" + value + ")"
	case "number":
		formatted = "formatNumber(float64(" + value + "))"
	case "bool":
		formatted = "strconv.FormatBool(bool(" + value + "))"
	default:
		failf("The query setting %s has a type that the generator cannot write into a URL", prm.jsonName)
	}
	set := fmt.Sprintf("query.Set(%s, %s)", quoteGo(prm.jsonName), formatted)
	if pointer {
		return fmt.Sprintf("\tif %s != nil {\n\t\t%s\n\t}\n", field, set)
	}
	return "\t" + set + "\n"
}

func pathExpression(op operation, args map[string]string) string {
	var parts []string
	rest := op.path
	for rest != "" {
		start := strings.Index(rest, "{")
		if start < 0 {
			parts = append(parts, quoteGo(rest))
			break
		}
		end := strings.Index(rest[start:], "}")
		if end < 0 {
			failf("The path %s opens a parameter that it never closes", op.path)
		}
		if start > 0 {
			parts = append(parts, quoteGo(rest[:start]))
		}
		name := rest[start+1 : start+end]
		argument, known := args[name]
		if !known {
			failf("The path %s holds the parameter %s, which the operation does not declare", op.path, name)
		}
		parts = append(parts, "escapePathSegment("+argument+")")
		rest = rest[start+end+1:]
	}
	return strings.Join(parts, " + ")
}

func (p *planner) operationComment(plan *operationPlan) string {
	op := plan.op
	block := &commentBlock{}
	summary := strings.TrimSuffix(op.summary, ".")
	block.paragraph(fmt.Sprintf("%s sends %s %s: %s.", plan.name, op.method, op.path, summary))
	if op.sqlStatement {
		block.paragraph("The route carries a SQL statement, so the client reads GET /capabilities once per server " +
			"and fails with SQL_NOT_ACCEPTED, sending nothing, when the server leaves out query.sql.")
	}
	var params []string
	for _, prm := range plan.pathArgs {
		params = append(params, argumentName(prm.jsonName)+": "+prm.description)
	}
	if op.body != nil {
		description := op.body.description
		if description == "" && op.body.typ != nil && op.body.typ.kind == kindRef {
			description = p.definition(op.body.typ.name).description
		}
		if description != "" {
			params = append(params, "body: "+description)
		}
	}
	if len(plan.queryArgs) > 0 {
		params = append(params, "params: The query settings, each left out of the URL while it stays nil.")
	}
	block.list("Parameters:", params)
	var answers []string
	for _, a := range op.successes() {
		answers = append(answers, fmt.Sprintf("HTTP %d: %s", a.status, a.description))
	}
	block.list("Answers:", answers)
	return block.render("")
}

func (p *planner) renderRequest(out *strings.Builder, plan *operationPlan, args map[string]string, failure string) {
	op := plan.op
	if op.sqlStatement {
		fmt.Fprintf(out, "\tif err := o.requireSQL(ctx); err != nil {\n\t\t%s\n\t}\n", failure)
	}
	fmt.Fprintf(out, "\trequest := exchange{\n\t\tmethod: %s,\n\t\tpath: %s,\n\t}\n", quoteGo(op.method), pathExpression(op, args))
	if len(plan.queryArgs) > 0 {
		out.WriteString("\tquery := url.Values{}\n")
		for _, prm := range plan.queryArgs {
			out.WriteString(p.queryLine(prm))
		}
		out.WriteString("\trequest.query = query\n")
	}
	if op.body == nil {
		return
	}
	fmt.Fprintf(out, "\trequest.contentType = %s\n", quoteGo(op.body.contentType))
	if op.body.kind == "text" {
		out.WriteString("\trequest.payload = []byte(body)\n")
		return
	}
	optional := !op.body.required
	if optional {
		out.WriteString("\tif body != nil {\n")
	}
	fmt.Fprintf(out, "\t\tpayload, err := encodeWire(body)\n\t\tif err != nil {\n\t\t\t%s\n\t\t}\n\t\trequest.payload = payload\n", failure)
	if optional {
		out.WriteString("\t} else {\n\t\trequest.contentType = \"\"\n\t}\n")
	}
}

func (p *planner) renderOperation(op operation) string {
	plan := p.planOperation(op)
	var out strings.Builder
	out.WriteString(p.renderParams(plan))
	if plan.resultKind == "choice" {
		out.WriteString(docComment("", plan.answerType, "holds the answer of "+plan.name+", in the field for the status that the server sent."))
		fmt.Fprintf(&out, "type %s struct {\n", plan.answerType)
		for _, a := range op.successes() {
			goType := p.goType(a.typ, plan.name+"Answer")
			fmt.Fprintf(&out, "\t%s *%s\n", goType, goType)
		}
		out.WriteString("}\n\n")
	}
	out.WriteString(p.operationComment(plan))
	args := map[string]string{}
	signature := []string{"ctx context.Context"}
	for _, prm := range plan.pathArgs {
		args[prm.jsonName] = argumentName(prm.jsonName)
		signature = append(signature, argumentName(prm.jsonName)+" string")
	}
	if bodyArg := p.bodyArgument(plan); bodyArg != "" {
		signature = append(signature, bodyArg)
	}
	if len(plan.queryArgs) > 0 {
		signature = append(signature, "params "+plan.name+"Params")
	}
	signature = append(signature, "options ...CallOption")
	results := "error"
	failure := "return err"
	if plan.resultType != "" {
		results = "(" + plan.resultType + ", error)"
		failure = "return " + zeroResult(plan) + ", err"
	}
	fmt.Fprintf(&out, "func (o *Operations) %s(%s) %s {\n", plan.name, strings.Join(signature, ", "), results)
	p.renderRequest(&out, plan, args, failure)
	if plan.resultKind == "empty" {
		out.WriteString("\tif _, err := o.transport.exchange(ctx, request, options); err != nil {\n\t\treturn err\n\t}\n\treturn nil\n}\n")
		return out.String()
	}
	fmt.Fprintf(&out, "\tanswer, err := o.transport.exchange(ctx, request, options)\n\tif err != nil {\n\t\t%s\n\t}\n", failure)
	switch plan.resultKind {
	case "text":
		out.WriteString("\treturn answer.text(), nil\n")
	case "json":
		fmt.Fprintf(&out, "\treturn decodeAnswer[%s](answer)\n", plan.resultType)
	case "jsonPointer":
		fmt.Fprintf(&out, "\treturn decodePointer[%s](answer)\n", strings.TrimPrefix(plan.resultType, "*"))
	case "choice":
		out.WriteString("\tswitch answer.status {\n")
		for _, a := range op.successes() {
			goType := p.goType(a.typ, plan.name+"Answer")
			fmt.Fprintf(&out, "\tcase %d:\n\t\tvalue, err := decodePointer[%s](answer)\n\t\tif err != nil {\n\t\t\treturn nil, err\n\t\t}\n\t\treturn &%s{%s: value}, nil\n", a.status, goType, plan.answerType, goType)
		}
		out.WriteString("\t}\n\treturn nil, undocumentedStatus(answer)\n")
	}
	out.WriteString("}\n")
	return out.String()
}

func (p *planner) operationChunks() map[string][]chunk {
	byTag := map[string][]chunk{}
	for _, op := range p.api.operations {
		byTag[op.tag] = append(byTag[op.tag], chunk{text: p.renderOperation(op)})
	}
	return byTag
}

package main

import (
	"strconv"
	"strings"
)

func (b *builder) content(content *jsonObject, hint string) (string, string, *typeRef) {
	if content.size() != 1 {
		failf("%s offers %d content types, and the generator reads exactly one", hint, content.size())
	}
	contentType := content.keys[0]
	schema := content.object(contentType).object("schema")
	if strings.TrimSpace(strings.Split(contentType, ";")[0]) == jsonContent {
		return "json", contentType, b.convert(schema, hint, "")
	}
	if schema.text("type") == "string" && schema.text("format") == "" {
		return "text", contentType, nil
	}
	failf("%s sends %s, which the generator cannot map to a type", hint, contentType)
	return "", "", nil
}

func (b *builder) answers(operationID string, responses *jsonObject) []answer {
	var found []answer
	for _, status := range responses.orderedKeys() {
		if status == "default" {
			continue
		}
		response := responses.object(status)
		if ref := response.text("$ref"); ref != "" {
			name, _ := strings.CutPrefix(ref, responsePrefix)
			response = b.responses.object(name)
			if response == nil {
				failf("Operation %s refers to the missing response %s", operationID, ref)
			}
		}
		code, err := strconv.Atoi(status)
		if err != nil {
			failf("Operation %s documents the status %q, which is not a number", operationID, status)
		}
		content := response.object("content")
		if content.size() == 0 {
			found = append(found, answer{status: code, kind: "empty", description: describe(response)})
			continue
		}
		kind, contentType, typ := b.content(content, pascalWords(operationID)+"Response"+status)
		found = append(found, answer{
			status: code, kind: kind, contentType: contentType, typ: typ, description: describe(response),
		})
	}
	for _, a := range found {
		if a.status >= 200 && a.status < 300 {
			return found
		}
	}
	failf("Operation %s documents no successful response", operationID)
	return nil
}

func (b *builder) operation(path, method string, raw *jsonObject, shared []any) operation {
	operationID := raw.text("operationId")
	if operationID == "" {
		failf("%s %s has no operationId, so it would have no method", strings.ToUpper(method), path)
	}
	var params []param
	for _, entry := range append(append([]any{}, shared...), raw.list("parameters")...) {
		spec, _ := entry.(*jsonObject)
		location := spec.text("in")
		if location != "path" && location != "query" {
			failf("Operation %s takes a %s parameter, which the generator does not read", operationID, location)
		}
		var schema any = &jsonObject{values: map[string]any{}}
		if spec.has("schema") {
			schema = spec.get("schema")
		}
		name := spec.text("name")
		params = append(params, param{
			jsonName:    name,
			location:    location,
			required:    spec.flag("required") || location == "path",
			typ:         b.convert(schema, pascalWords(operationID)+pascalWords(name), ""),
			description: describe(spec),
		})
	}
	op := operation{
		operationID:  operationID,
		method:       strings.ToUpper(method),
		path:         path,
		tag:          "Default",
		summary:      strings.Join(strings.Fields(raw.text("summary")), " "),
		params:       params,
		sqlStatement: raw.flag(sqlStatementExtension),
	}
	if raw.has(sqlStatementExtension) {
		if _, isBool := raw.get(sqlStatementExtension).(bool); !isBool {
			failf("Operation %s gives %s a value that is not a boolean", operationID, sqlStatementExtension)
		}
	}
	if tags := raw.list("tags"); len(tags) > 0 {
		if tag, isString := tags[0].(string); isString && tag != "" {
			op.tag = tag
		}
	}
	if request := raw.object("requestBody"); request != nil {
		kind, contentType, typ := b.content(request.object("content"), pascalWords(operationID)+"Body")
		op.body = &body{
			kind: kind, contentType: contentType, typ: typ, required: request.flag("required"),
			description: describe(request),
		}
	}
	op.answers = b.answers(operationID, raw.object("responses"))
	return op
}

func buildAPI(openAPI, asyncAPI *jsonObject) (result *api, err error) {
	defer catchSpecError(&err)
	schemas := mergeSchemas(openAPI.object("components").object("schemas"), asyncAPI.object("components").object("schemas"))
	checkMessages(asyncAPI, schemas)
	b := &builder{
		schemas:   schemas,
		responses: openAPI.object("components").object("responses"),
		api:       &api{definitions: map[string]*definition{}},
	}
	for _, name := range b.schemas.orderedKeys() {
		schema := b.schemas.object(name)
		if schema == nil {
			failf("The schema %s is not an object", name)
		}
		b.topLevel(name, schema)
	}
	paths := openAPI.object("paths")
	seen := map[string]bool{}
	for _, path := range paths.orderedKeys() {
		item := paths.object(path)
		for _, method := range httpMethods {
			raw := item.object(method)
			if raw == nil {
				continue
			}
			op := b.operation(path, method, raw, item.list("parameters"))
			if seen[op.operationID] {
				failf("Two operations share the operationId %s", op.operationID)
			}
			seen[op.operationID] = true
			b.api.operations = append(b.api.operations, op)
		}
	}
	if len(b.api.operations) == 0 {
		failf("The OpenAPI document lists no operation")
	}
	return b.api, nil
}

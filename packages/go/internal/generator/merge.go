package main

import (
	"reflect"
	"sort"
	"strings"
)

var literalKeywords = map[string]bool{
	"const": true, "enum": true, "required": true, "default": true, "examples": true, "type": true,
	"format": true, "$ref": true, "minItems": true, "maxItems": true,
}

var annotationKeywords = map[string]bool{"description": true, "title": true, "examples": true}

func mergeSchemas(openAPI, asyncAPI *jsonObject) *jsonObject {
	merged := &jsonObject{values: map[string]any{}}
	for _, name := range openAPI.orderedKeys() {
		merged.keys = append(merged.keys, name)
		merged.values[name] = openAPI.get(name)
	}
	for _, name := range asyncAPI.orderedKeys() {
		incoming := asyncAPI.get(name)
		if existing, shared := merged.values[name]; shared {
			if !sameShape(existing, incoming) {
				failf("The schema %s has one shape in openapi.json and another in asyncapi.json, so one Go type cannot hold both", name)
			}
			continue
		}
		merged.keys = append(merged.keys, name)
		merged.values[name] = incoming
	}
	return merged
}

func sameShape(left, right any) bool {
	leftObject, leftIsObject := left.(*jsonObject)
	rightObject, rightIsObject := right.(*jsonObject)
	if leftIsObject != rightIsObject {
		return false
	}
	if !leftIsObject {
		return sameList(left, right)
	}
	leftKeys := shapeKeys(leftObject)
	rightKeys := shapeKeys(rightObject)
	if !reflect.DeepEqual(leftKeys, rightKeys) {
		return false
	}
	for _, key := range leftKeys {
		leftValue, rightValue := leftObject.get(key), rightObject.get(key)
		switch {
		case literalKeywords[key]:
			if !sameLiteral(leftValue, rightValue) {
				return false
			}
		case key == "properties":
			if !sameProperties(leftValue, rightValue) {
				return false
			}
		default:
			if !sameShape(leftValue, rightValue) {
				return false
			}
		}
	}
	return true
}

func sameList(left, right any) bool {
	leftList, leftIsList := left.([]any)
	rightList, rightIsList := right.([]any)
	if !leftIsList || !rightIsList {
		return reflect.DeepEqual(left, right)
	}
	if len(leftList) != len(rightList) {
		return false
	}
	for position := range leftList {
		if !sameShape(leftList[position], rightList[position]) {
			return false
		}
	}
	return true
}

func sameProperties(left, right any) bool {
	leftObject, leftIsObject := left.(*jsonObject)
	rightObject, rightIsObject := right.(*jsonObject)
	if !leftIsObject || !rightIsObject {
		return false
	}
	if !reflect.DeepEqual(sortedKeys(leftObject), sortedKeys(rightObject)) {
		return false
	}
	for _, key := range leftObject.orderedKeys() {
		if !sameShape(leftObject.get(key), rightObject.get(key)) {
			return false
		}
	}
	return true
}

func sameLiteral(left, right any) bool {
	return reflect.DeepEqual(plain(left), plain(right))
}

func plain(value any) any {
	switch typed := value.(type) {
	case *jsonObject:
		out := map[string]any{}
		for _, key := range typed.orderedKeys() {
			out[key] = plain(typed.get(key))
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for position, item := range typed {
			out[position] = plain(item)
		}
		return out
	}
	return value
}

func shapeKeys(object *jsonObject) []string {
	var keys []string
	for _, key := range sortedKeys(object) {
		if !annotationKeywords[key] {
			keys = append(keys, key)
		}
	}
	return keys
}

func sortedKeys(object *jsonObject) []string {
	keys := append([]string{}, object.orderedKeys()...)
	sort.Strings(keys)
	return keys
}

func checkMessages(asyncAPI *jsonObject, schemas *jsonObject) {
	messages := asyncAPI.object("components").object("messages")
	if messages.size() == 0 {
		failf("The AsyncAPI document lists no message")
	}
	for _, name := range messages.orderedKeys() {
		ref := messages.object(name).object("payload").text("$ref")
		schema, found := strings.CutPrefix(ref, schemaPrefix)
		if !found || !schemas.has(schema) {
			failf("The WebSocket message %s carries a payload that names no schema of the two files", name)
		}
	}
}

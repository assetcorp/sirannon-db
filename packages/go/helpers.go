package sirannon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

const unreservedInComponent = "-_.!~*'()"

func escapePathSegment(value string) string {
	if value == "." || value == ".." {
		return strings.Repeat("%2E", len(value))
	}
	var out strings.Builder
	for _, b := range []byte(value) {
		if ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') || ('0' <= b && b <= '9') ||
			strings.IndexByte(unreservedInComponent, b) >= 0 {
			out.WriteByte(b)
			continue
		}
		fmt.Fprintf(&out, "%%%02X", b)
	}
	return out.String()
}

func formatNumber(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "NaN"
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func decodeAnswer[T any](answer *answered) (T, error) {
	var value T
	if reflect.TypeFor[T]().Kind() == reflect.Struct && jsonKindOf(answer.body) != "object" {
		return value, invalidResponse("The answer from %s is not a JSON object (HTTP %d)", answer.url, answer.status)
	}
	if err := decodeWire(answer.body, &value); err != nil {
		failure := invalidResponse("The answer from %s does not hold the shape that the route documents: %v", answer.url, err)
		failure.Status = answer.status
		return value, failure
	}
	return value, nil
}

func decodePointer[T any](answer *answered) (*T, error) {
	value, err := decodeAnswer[T](answer)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func zeroOf[T any]() T {
	var zero T
	return zero
}

func undocumentedStatus(answer *answered) *Error {
	failure := invalidResponse("%s answered HTTP %d, which the route does not document", answer.url, answer.status)
	failure.Status = answer.status
	return failure
}

type variantShape struct {
	required []string
	known    []string
	consts   []string
	open     bool
}

func jsonKindOf(data []byte) string {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 {
		return ""
	}
	switch trimmed[0] {
	case '"':
		return "string"
	case '{':
		return "object"
	case '[':
		return "array"
	case 't', 'f':
		return "bool"
	case 'n':
		return "null"
	}
	return "number"
}

func matchesConsts(fields map[string]json.RawMessage, consts []string) bool {
	for position := 0; position+1 < len(consts); position += 2 {
		var value string
		raw, present := fields[consts[position]]
		if !present || json.Unmarshal(raw, &value) != nil || value != consts[position+1] {
			return false
		}
	}
	return true
}

func chooseVariant(data []byte, shapes []variantShape) int {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return -1
	}
	holdsRequired := func(shape variantShape) bool {
		for _, key := range shape.required {
			if _, present := fields[key]; !present {
				return false
			}
		}
		return matchesConsts(fields, shape.consts)
	}
	for position, shape := range shapes {
		if !holdsRequired(shape) {
			continue
		}
		if shape.open {
			return position
		}
		allKnown := true
		for key := range fields {
			if !slices.Contains(shape.known, key) {
				allKnown = false
				break
			}
		}
		if allKnown {
			return position
		}
	}
	for position, shape := range shapes {
		if holdsRequired(shape) {
			return position
		}
	}
	return -1
}

func isSet(choice any) bool {
	value := reflect.ValueOf(choice)
	switch value.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
		return !value.IsNil()
	case reflect.Invalid:
		return false
	}
	return true
}

func marshalUnion(name string, choices ...any) ([]byte, error) {
	var chosen any
	set := 0
	for _, choice := range choices {
		if isSet(choice) {
			set++
			chosen = choice
		}
	}
	if set != 1 {
		return nil, invalidArgument("A %s must have exactly one field set, and this one has %d", name, set)
	}
	return encodeWire(chosen)
}

func unionMismatch(name string, data []byte) error {
	preview := data
	if len(preview) > 80 {
		preview = preview[:80]
	}
	return fmt.Errorf("the JSON %s fits none of the forms of %s", preview, name)
}

type Nullable[T any] struct {
	value T
	state nullableState
}

type nullableState uint8

const (
	nullableAbsent nullableState = iota
	nullableNull
	nullablePresent
)

func Null[T any]() Nullable[T] {
	return Nullable[T]{state: nullableNull}
}

func Value[T any](value T) Nullable[T] {
	return Nullable[T]{value: value, state: nullablePresent}
}

func (n Nullable[T]) Get() (T, bool) {
	return n.value, n.state == nullablePresent
}

func (n Nullable[T]) IsNull() bool {
	return n.state == nullableNull
}

func (n Nullable[T]) IsZero() bool {
	return n.state == nullableAbsent
}

func (n Nullable[T]) MarshalJSON() ([]byte, error) {
	if n.state != nullablePresent {
		return []byte("null"), nil
	}
	return encodeWire(n.value)
}

func (n *Nullable[T]) UnmarshalJSON(data []byte) error {
	if jsonKindOf(data) == "null" {
		*n = Null[T]()
		return nil
	}
	var value T
	if err := decodeWire(data, &value); err != nil {
		return err
	}
	*n = Value(value)
	return nil
}

func Ptr[T any](value T) *T {
	return &value
}

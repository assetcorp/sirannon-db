package sirannon

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
)

const (
	integerTag         = "__sirannon_int"
	blobTag            = "__sirannon_blob"
	maxIntegerDigits   = 19
	maxSafeInteger     = 1<<53 - 1
	maxWireNestingDeep = 256
)

var errNestedTooDeep = errors.New("the value nests deeper than the client reads")

func decodeNumbers(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("the JSON holds more than one value")
	}
	return nil
}

func decodeWire(data []byte, target any) error {
	if err := decodeNumbers(data, target); err != nil {
		return err
	}
	return normaliseValues(reflect.ValueOf(target), 0)
}

func numberValue(number json.Number) (any, error) {
	text := number.String()
	if !strings.ContainsAny(text, ".eE") {
		if integer, err := strconv.ParseInt(text, 10, 64); err == nil {
			return integer, nil
		}
	}
	parsed, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil, fmt.Errorf("the number %s does not fit a float64", text)
	}
	return parsed, nil
}

func integerEnvelope(payload string) (int64, error) {
	digits := strings.TrimPrefix(payload, "-")
	if digits == "" || len(digits) > maxIntegerDigits || strings.Trim(digits, "0123456789") != "" {
		return 0, fmt.Errorf("the integer envelope %q holds no decimal integer of 1 to 19 digits", payload)
	}
	value, err := strconv.ParseInt(payload, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("the integer envelope %q does not fit a 64-bit integer", payload)
	}
	return value, nil
}

func blobEnvelope(payload string) ([]byte, error) {
	if len(payload)%2 != 0 {
		return nil, fmt.Errorf("the BLOB envelope holds %d hexadecimal digits, which is not two per byte", len(payload))
	}
	value, err := hex.DecodeString(payload)
	if err != nil {
		return nil, errors.New("the BLOB envelope holds a character that is not hexadecimal")
	}
	return value, nil
}

func decodeValue(value any, depth int) (any, error) {
	if depth > maxWireNestingDeep {
		return nil, errNestedTooDeep
	}
	switch typed := value.(type) {
	case json.Number:
		return numberValue(typed)
	case map[string]any:
		if len(typed) == 1 {
			if payload, isString := typed[integerTag].(string); isString {
				return integerEnvelope(payload)
			}
			if payload, isString := typed[blobTag].(string); isString {
				return blobEnvelope(payload)
			}
		}
		for key, item := range typed {
			decoded, err := decodeValue(item, depth+1)
			if err != nil {
				return nil, err
			}
			typed[key] = decoded
		}
		return typed, nil
	case []any:
		for position, item := range typed {
			decoded, err := decodeValue(item, depth+1)
			if err != nil {
				return nil, err
			}
			typed[position] = decoded
		}
		return typed, nil
	}
	return value, nil
}

func setDecoded(target reflect.Value, depth int) error {
	if target.IsNil() {
		return nil
	}
	decoded, err := decodeValue(target.Elem().Interface(), depth+1)
	if err != nil {
		return err
	}
	if decoded == nil {
		target.Set(reflect.Zero(target.Type()))
		return nil
	}
	target.Set(reflect.ValueOf(decoded))
	return nil
}

func holdsValues(kind reflect.Kind) bool {
	switch kind {
	case reflect.Interface, reflect.Pointer, reflect.Struct, reflect.Slice, reflect.Array, reflect.Map:
		return true
	}
	return false
}

func normaliseValues(value reflect.Value, depth int) error {
	if depth > maxWireNestingDeep {
		return errNestedTooDeep
	}
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		return normaliseValues(value.Elem(), depth+1)
	case reflect.Interface:
		if !value.CanSet() {
			return nil
		}
		return setDecoded(value, depth)
	case reflect.Struct:
		for position := range value.NumField() {
			field := value.Field(position)
			if field.CanSet() && holdsValues(field.Kind()) {
				if err := normaliseValues(field, depth+1); err != nil {
					return err
				}
			}
		}
	case reflect.Slice, reflect.Array:
		if !holdsValues(value.Type().Elem().Kind()) {
			return nil
		}
		for position := range value.Len() {
			if err := normaliseValues(value.Index(position), depth+1); err != nil {
				return err
			}
		}
	case reflect.Map:
		return normaliseMap(value, depth)
	}
	return nil
}

func normaliseMap(value reflect.Value, depth int) error {
	elementType := value.Type().Elem()
	if value.IsNil() || !holdsValues(elementType.Kind()) {
		return nil
	}
	iterator := value.MapRange()
	type replacement struct{ key, value reflect.Value }
	var replacements []replacement
	for iterator.Next() {
		copied := reflect.New(elementType).Elem()
		copied.Set(iterator.Value())
		if err := normaliseValues(copied, depth+1); err != nil {
			return err
		}
		replacements = append(replacements, replacement{iterator.Key(), copied})
	}
	for _, entry := range replacements {
		value.SetMapIndex(entry.key, entry.value)
	}
	return nil
}

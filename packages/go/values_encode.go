package sirannon

import (
	"bytes"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

var (
	marshalerType     = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
	numberType        = reflect.TypeFor[json.Number]()
	zeroCheckerType   = reflect.TypeFor[interface{ IsZero() bool }]()
)

func encodeWire(value any) ([]byte, error) {
	tree, err := wireTree(value)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(tree); err != nil {
		return nil, invalidArgument("The client cannot encode the request as JSON: %v", err)
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

func wireTree(value any) (any, error) {
	tree, err := treeOf(reflect.ValueOf(value), 0)
	if err != nil {
		var failure *Error
		if errors.As(err, &failure) {
			return nil, failure
		}
		return nil, invalidArgument("The client cannot encode the request: %v", err)
	}
	return tree, nil
}

func integerTree(value int64) any {
	if value >= -maxSafeInteger && value <= maxSafeInteger {
		return value
	}
	return map[string]any{integerTag: strconv.FormatInt(value, 10)}
}

func blobTree(value []byte) any {
	return map[string]any{blobTag: strings.ToUpper(hex.EncodeToString(value))}
}

func numberTree(number json.Number) (any, error) {
	text := number.String()
	if !strings.ContainsAny(text, ".eE") {
		if integer, err := strconv.ParseInt(text, 10, 64); err == nil {
			return integerTree(integer), nil
		}
	}
	parsed, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsInf(parsed, 0) {
		return nil, fmt.Errorf("the number %q is neither a 64-bit integer nor a finite float", text)
	}
	return parsed, nil
}

func marshalled(value reflect.Value) (any, bool, error) {
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return nil, true, nil
	}
	if value.Type().Implements(marshalerType) {
		raw, err := value.Interface().(json.Marshaler).MarshalJSON()
		if err != nil {
			return nil, true, err
		}
		return json.RawMessage(raw), true, nil
	}
	if value.Type().Implements(textMarshalerType) {
		text, err := value.Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return nil, true, err
		}
		return string(text), true, nil
	}
	return nil, false, nil
}

func treeOf(value reflect.Value, depth int) (any, error) {
	if depth > maxWireNestingDeep {
		return nil, errNestedTooDeep
	}
	if !value.IsValid() {
		return nil, nil
	}
	if value.Type() == numberType {
		return numberTree(json.Number(value.String()))
	}
	if tree, handled, err := marshalled(value); handled {
		return tree, err
	}
	if value.Kind() != reflect.Pointer && value.CanAddr() {
		if tree, handled, err := marshalled(value.Addr()); handled {
			return tree, err
		}
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			return nil, nil
		}
		return treeOf(value.Elem(), depth+1)
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return integerTree(value.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if value.Uint() > math.MaxInt64 {
			return nil, invalidArgument("The integer %d does not fit the signed 64-bit integer that SQLite stores", value.Uint())
		}
		return integerTree(int64(value.Uint())), nil
	case reflect.Float32, reflect.Float64:
		number := value.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, invalidArgument("The value %v is not a finite number, which JSON cannot carry", number)
		}
		return number, nil
	case reflect.String:
		return value.String(), nil
	case reflect.Slice:
		if value.IsNil() {
			return nil, nil
		}
		return listTree(value, depth)
	case reflect.Array:
		return listTree(value, depth)
	case reflect.Map:
		return mapTree(value, depth)
	case reflect.Struct:
		return structTree(value, depth)
	}
	return nil, invalidArgument("The client cannot send a value of type %s", value.Type())
}

func listTree(value reflect.Value, depth int) (any, error) {
	if value.Type().Elem().Kind() == reflect.Uint8 {
		raw := make([]byte, value.Len())
		reflect.Copy(reflect.ValueOf(raw), value)
		return blobTree(raw), nil
	}
	out := make([]any, value.Len())
	for position := range value.Len() {
		item, err := treeOf(value.Index(position), depth+1)
		if err != nil {
			return nil, err
		}
		out[position] = item
	}
	return out, nil
}

func mapKey(key reflect.Value) (string, error) {
	switch key.Kind() {
	case reflect.String:
		return key.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(key.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(key.Uint(), 10), nil
	}
	return "", invalidArgument("The client cannot send a map keyed by %s", key.Type())
}

func mapTree(value reflect.Value, depth int) (any, error) {
	if value.IsNil() {
		return nil, nil
	}
	out := make(map[string]any, value.Len())
	iterator := value.MapRange()
	for iterator.Next() {
		key, err := mapKey(iterator.Key())
		if err != nil {
			return nil, err
		}
		item, err := treeOf(iterator.Value(), depth+1)
		if err != nil {
			return nil, err
		}
		out[key] = item
	}
	return out, nil
}

type fieldTag struct {
	name      string
	skip      bool
	omitEmpty bool
	omitZero  bool
}

func readFieldTag(field reflect.StructField) fieldTag {
	tag, tagged := field.Tag.Lookup("json")
	if tag == "-" {
		return fieldTag{skip: true}
	}
	name, rest, _ := strings.Cut(tag, ",")
	if !tagged || name == "" {
		name = field.Name
	}
	parsed := fieldTag{name: name}
	for option := range strings.SplitSeq(rest, ",") {
		switch option {
		case "omitempty":
			parsed.omitEmpty = true
		case "omitzero":
			parsed.omitZero = true
		}
	}
	return parsed
}

func isEmptyValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return value.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return value.IsZero()
	}
	return false
}

func isZeroValue(value reflect.Value) bool {
	if value.Type().Implements(zeroCheckerType) {
		if value.Kind() == reflect.Pointer && value.IsNil() {
			return true
		}
		return value.Interface().(interface{ IsZero() bool }).IsZero()
	}
	return value.IsZero()
}

func structTree(value reflect.Value, depth int) (any, error) {
	out := map[string]any{}
	if err := addStructFields(out, value, depth); err != nil {
		return nil, err
	}
	return out, nil
}

func addStructFields(out map[string]any, value reflect.Value, depth int) error {
	valueType := value.Type()
	for position := range valueType.NumField() {
		field := valueType.Field(position)
		tag := readFieldTag(field)
		fieldValue := value.Field(position)
		_, tagged := field.Tag.Lookup("json")
		if field.Anonymous && !tagged && fieldValue.Kind() == reflect.Struct {
			if err := addStructFields(out, fieldValue, depth+1); err != nil {
				return err
			}
			continue
		}
		if tag.skip || !field.IsExported() {
			continue
		}
		if (tag.omitEmpty && isEmptyValue(fieldValue)) || (tag.omitZero && isZeroValue(fieldValue)) {
			continue
		}
		item, err := treeOf(fieldValue, depth+1)
		if err != nil {
			return err
		}
		out[tag.name] = item
	}
	return nil
}

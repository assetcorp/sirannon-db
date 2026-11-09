package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

type jsonObject struct {
	keys   []string
	values map[string]any
}

func (o *jsonObject) get(key string) any {
	if o == nil {
		return nil
	}
	return o.values[key]
}

func (o *jsonObject) has(key string) bool {
	if o == nil {
		return false
	}
	_, found := o.values[key]
	return found
}

func (o *jsonObject) object(key string) *jsonObject {
	value, _ := o.get(key).(*jsonObject)
	return value
}

func (o *jsonObject) text(key string) string {
	value, _ := o.get(key).(string)
	return value
}

func (o *jsonObject) list(key string) []any {
	value, _ := o.get(key).([]any)
	return value
}

func (o *jsonObject) flag(key string) bool {
	value, _ := o.get(key).(bool)
	return value
}

func (o *jsonObject) size() int {
	if o == nil {
		return 0
	}
	return len(o.keys)
}

func (o *jsonObject) orderedKeys() []string {
	if o == nil {
		return nil
	}
	return o.keys
}

func decodeDocument(data []byte, label string) (*jsonObject, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := readValue(decoder, label)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("the %s document holds more than one JSON value", label)
	}
	root, ok := value.(*jsonObject)
	if !ok {
		return nil, fmt.Errorf("the %s document is not a JSON object", label)
	}
	return root, nil
}

func readValue(decoder *json.Decoder, label string) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch delimiter := token.(type) {
	case json.Delim:
		switch delimiter {
		case '{':
			return readObject(decoder, label)
		case '[':
			return readArray(decoder, label)
		default:
			return nil, fmt.Errorf("the %s document holds an unexpected %q", label, delimiter)
		}
	default:
		return token, nil
	}
}

func readObject(decoder *json.Decoder, label string) (*jsonObject, error) {
	result := &jsonObject{values: map[string]any{}}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("the %s document holds an object key that is not a string", label)
		}
		value, err := readValue(decoder, label)
		if err != nil {
			return nil, err
		}
		if _, duplicate := result.values[key]; duplicate {
			return nil, fmt.Errorf("the %s document repeats the key %q in one object", label, key)
		}
		result.keys = append(result.keys, key)
		result.values[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	return result, nil
}

func readArray(decoder *json.Decoder, label string) ([]any, error) {
	result := []any{}
	for decoder.More() {
		value, err := readValue(decoder, label)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	return result, nil
}

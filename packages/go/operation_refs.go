package sirannon

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sync"
)

type NoArgs struct{}

type ReadRef[Args any, R any] struct {
	Name string
}

type WriteRef[Args any] struct {
	Name string
}

func Query[Args any, R any](ctx context.Context, db *RemoteDatabase, ref ReadRef[Args, R], args Args, options ...QueryOption) ([]R, error) {
	if db == nil {
		return nil, invalidArgument("Query needs a database")
	}
	convert, err := rowConverter[R]()
	if err != nil {
		return nil, err
	}
	encoded, err := operationArgs(ref.Name, args)
	if err != nil {
		return nil, err
	}
	rows, err := db.queryOperation(ctx, ref.Name, encoded, options)
	if err != nil {
		return nil, err
	}
	converted := make([]R, 0, len(rows))
	for _, row := range rows {
		value, err := convert(row)
		if err != nil {
			return nil, err
		}
		converted = append(converted, value)
	}
	return converted, nil
}

func Execute[Args any](ctx context.Context, db *RemoteDatabase, ref WriteRef[Args], args Args, options ...ExecuteOption) ([]ExecuteResult, error) {
	if db == nil {
		return nil, invalidArgument("Execute needs a database")
	}
	encoded, err := operationArgs(ref.Name, args)
	if err != nil {
		return nil, err
	}
	return db.executeOperation(ctx, ref.Name, encoded, options)
}

func Live[Args any, R any](ctx context.Context, db *RemoteDatabase, ref ReadRef[Args, R], args Args, options ...SubscribeOption) (*LiveQuery[R], error) {
	if db == nil {
		return nil, invalidArgument("Live needs a database")
	}
	convert, err := rowConverter[R]()
	if err != nil {
		return nil, err
	}
	encoded, err := operationArgs(ref.Name, args)
	if err != nil {
		return nil, err
	}
	return openLiveQuery(ctx, db, ref.Name, encoded, convert, resolveSubscribeOptions(options).onError)
}

func operationArgs(name string, args any) (map[string]any, error) {
	if name == "" {
		return nil, invalidArgument("A registered operation needs a name")
	}
	tree, err := wireTree(args)
	if err != nil {
		return nil, err
	}
	switch value := tree.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		if len(value) == 0 {
			return nil, nil
		}
		return value, nil
	}
	return nil, invalidArgument("The arguments of %s must be a struct or a map, and %T is neither", name, args)
}

type rowField struct {
	index []int
	name  string
}

var rowFieldPlans sync.Map

func rowConverter[R any]() (func(Row) (R, error), error) {
	target := reflect.TypeFor[R]()
	if target == reflect.TypeFor[Row]() {
		return func(row Row) (R, error) {
			return any(row).(R), nil
		}, nil
	}
	if target.Kind() != reflect.Struct {
		return nil, invalidArgument("A registered read returns Row or a struct, and %s is neither", target)
	}
	fields := rowFieldsOf(target)
	return func(row Row) (R, error) {
		var value R
		destination := reflect.ValueOf(&value).Elem()
		for _, field := range fields {
			column, found := row[field.name]
			if !found || column == nil {
				continue
			}
			if err := assignColumn(destination.FieldByIndex(field.index), column); err != nil {
				return value, invalidResponse("The column %s cannot fill the field of %s: %v", field.name, target, err)
			}
		}
		return value, nil
	}, nil
}

func rowFieldsOf(target reflect.Type) []rowField {
	if cached, found := rowFieldPlans.Load(target); found {
		return cached.([]rowField)
	}
	var fields []rowField
	collectRowFields(target, nil, &fields)
	rowFieldPlans.Store(target, fields)
	return fields
}

func collectRowFields(target reflect.Type, prefix []int, fields *[]rowField) {
	for position := range target.NumField() {
		field := target.Field(position)
		index := append(append([]int{}, prefix...), position)
		_, tagged := field.Tag.Lookup("json")
		if field.Anonymous && !tagged && field.Type.Kind() == reflect.Struct {
			collectRowFields(field.Type, index, fields)
			continue
		}
		tag := readFieldTag(field)
		if tag.skip || !field.IsExported() {
			continue
		}
		*fields = append(*fields, rowField{index: index, name: tag.name})
	}
}

func numericKind(kind reflect.Kind) bool {
	return (kind >= reflect.Int && kind <= reflect.Uint64) || kind == reflect.Float32 || kind == reflect.Float64
}

func assignColumn(field reflect.Value, column any) error {
	source := reflect.ValueOf(column)
	if source.Type().AssignableTo(field.Type()) {
		field.Set(source)
		return nil
	}
	if numericKind(source.Kind()) && numericKind(field.Kind()) {
		if source.CanInt() && source.Int() < 0 && field.CanUint() {
			return fmt.Errorf("the negative value %v does not fit %s", column, field.Type())
		}
		if source.CanUint() && source.Uint() > math.MaxInt64 && field.CanInt() {
			return fmt.Errorf("the value %v does not fit %s", column, field.Type())
		}
		converted := source.Convert(field.Type())
		if !converted.Convert(source.Type()).Equal(source) {
			return fmt.Errorf("the value %v does not fit %s", column, field.Type())
		}
		field.Set(converted)
		return nil
	}
	return fmt.Errorf("a %T does not fit %s", column, field.Type())
}

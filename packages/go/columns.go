package sirannon

import (
	"context"
	"encoding/json"
)

const columnsShape = "columns"

const malformedColumnsAnswer = "The server answered a columns read with a body that holds no valid columns, rows, and truncated"

// ColumnLimits caps the answer of one [RemoteDatabase.QueryWithColumns]. A nil field sets no limit,
// and each limit you set must be a whole number from 0 to 2^53-1.
type ColumnLimits struct {
	// MaxRows is the most rows that the answer holds. The server reads one more row to learn whether
	// more exist, and it sets Truncated to "rows" when they do.
	MaxRows *int64
	// MaxBytes is the most bytes of values that the answer holds, where every value counts 8 bytes and
	// a TEXT or BLOB value also counts its length in bytes. The server sets Truncated to "bytes" when the
	// next row would pass it.
	MaxBytes *int64
}

type columnsCall struct {
	sql         string
	params      any
	readConcern *ReadConcern
	maxRows     *float64
	maxBytes    *float64
}

func columnLimit(value *int64, field string) (*float64, error) {
	if value == nil {
		return nil, nil
	}
	if *value < 0 || *value > maxSafeInteger {
		return nil, invalidArgument("ColumnLimits.%s must be a whole number from 0 to %d, and it was %d", field, int64(maxSafeInteger), *value)
	}
	return Ptr(float64(*value)), nil
}

// QueryWithColumns sends one read statement and returns each result column's details with the rows
// as arrays of values in column order, so two columns that share a name keep separate places. It
// takes params in the same forms as [RemoteDatabase.Query] and accepts [WithReadConcern]. Truncated
// in the answer is nil when the answer holds every row, and "rows" or "bytes" when that limit in
// limits stopped the read. It fails with INVALID_ARGUMENT for a limit outside 0 to 2^53-1 before it
// sends anything, and with SQL_NOT_ACCEPTED when the server refuses SQL statements.
func (d *RemoteDatabase) QueryWithColumns(ctx context.Context, sql string, params any, limits ColumnLimits, options ...QueryOption) (QueryColumnsResponse, error) {
	maxRows, err := columnLimit(limits.MaxRows, "MaxRows")
	if err != nil {
		return QueryColumnsResponse{}, err
	}
	maxBytes, err := columnLimit(limits.MaxBytes, "MaxBytes")
	if err != nil {
		return QueryColumnsResponse{}, err
	}
	call := columnsCall{sql: sql, params: params, readConcern: readQueryOptions(options).readConcern, maxRows: maxRows, maxBytes: maxBytes}
	var answer QueryColumnsResponse
	err = d.run(ctx, func(ctx context.Context) error {
		if err := d.requireSQL(ctx); err != nil {
			return err
		}
		var err error
		answer, err = d.transport.queryColumns(ctx, call)
		return err
	})
	return answer, err
}

func columnsRequest(call columnsCall) (QueryRequest, error) {
	named, positional, err := paramsTree(call.params)
	if err != nil {
		return QueryRequest{}, err
	}
	body := QueryRequest{SQL: call.sql, ReadConcern: call.readConcern, Shape: Ptr(columnsShape), MaxRows: call.maxRows, MaxBytes: call.maxBytes}
	if named != nil || positional != nil {
		body.Params = &QueryRequestParams{Map: named, List: positional}
	}
	return body, nil
}

func columnsAnswerFrom(data json.RawMessage) (QueryColumnsResponse, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil || fields["truncated"] == nil {
		return QueryColumnsResponse{}, invalidResponse(malformedColumnsAnswer)
	}
	var columns []map[string]json.RawMessage
	if err := json.Unmarshal(fields["columns"], &columns); err != nil || columns == nil {
		return QueryColumnsResponse{}, invalidResponse(malformedColumnsAnswer)
	}
	for _, column := range columns {
		if jsonKindOf(column["name"]) != "string" {
			return QueryColumnsResponse{}, invalidResponse(malformedColumnsAnswer)
		}
		for _, field := range []string{"declaredType", "table", "column", "database"} {
			kind := jsonKindOf(column[field])
			if kind != "string" && kind != "null" {
				return QueryColumnsResponse{}, invalidResponse(malformedColumnsAnswer)
			}
		}
	}
	var answer QueryColumnsResponse
	if err := decodeWire(data, &answer); err != nil {
		return QueryColumnsResponse{}, invalidResponse(malformedColumnsAnswer)
	}
	if answer.Columns == nil || answer.Rows == nil {
		return QueryColumnsResponse{}, invalidResponse(malformedColumnsAnswer)
	}
	if answer.Truncated != nil && *answer.Truncated != "rows" && *answer.Truncated != "bytes" {
		return QueryColumnsResponse{}, invalidResponse(malformedColumnsAnswer)
	}
	for _, row := range answer.Rows {
		if row == nil || len(row) != len(answer.Columns) {
			return QueryColumnsResponse{}, invalidResponse(malformedColumnsAnswer)
		}
	}
	return answer, nil
}

package sirannon

import (
	"context"
	"math"
	"strconv"
)

// Row holds one result row under its column names. The client stores an integer as an int64,
// including one beyond 2^53, and a BLOB as a []byte.
type Row = map[string]any

// Statement is one statement of a [RemoteDatabase.Transaction].
type Statement struct {
	// SQL is the text of the statement.
	SQL string
	// Params holds the values that the server binds to the statement: a []any of positional values, a
	// map[string]any of named values, or nil when the statement takes none.
	Params any
}

// ExecuteResult holds what one write statement changed.
type ExecuteResult struct {
	// Changes is the number of rows that the statement inserted, updated, or deleted.
	Changes int64
	// LastInsertRowID is the rowid that SQLite reports for the most recent insert once the statement
	// completes.
	LastInsertRowID int64
}

// LoadResult holds the totals of a bulk load.
type LoadResult struct {
	// RowsLoaded is the number of parameter sets that the load applied.
	RowsLoaded int64
	// Changes is the number of rows that the load inserted, updated, or deleted.
	Changes int64
}

// ChangeEvent holds one change to a row, as the client passes it to a subscription callback.
type ChangeEvent struct {
	// Type is insert, update, or delete.
	Type string
	// Table is the table of the changed row.
	Table string
	// Row holds the row's values after the change, or an empty Row when the server sends none.
	Row Row
	// OldRow holds the row's values before an update or a delete, and nil for an insert.
	OldRow Row
	// Seq is the position of this change in the database's change log, which the client uses to
	// resume a subscription after the last change that it delivered.
	Seq int64
	// Timestamp is the number of milliseconds since the Unix epoch at the moment when the database
	// recorded the change.
	Timestamp float64
	// HLC is the hybrid logical clock stamp that the writing node gave this change, or "" when the
	// change has no stamp.
	HLC string
	// Origin is the identifier of the node or device that wrote the change, or "" when the server
	// sends none.
	Origin string
	// RowID is the primary key of the changed row, encoded as a string.
	RowID string
	// TxID is the identifier of the transaction that made the change, or "" when the change has no
	// stamp.
	TxID string
	// TxEnd is true on the last change of a transaction that passes the subscription's filter, so
	// that you can apply each transaction as a whole.
	TxEnd bool
}

type statementCall struct {
	sql          string
	params       any
	name         string
	args         map[string]any
	readConcern  *ReadConcern
	writeConcern *WriteConcern
}

type liveSpec struct {
	name     string
	args     map[string]any
	handlers liveHandlers
	digest   func(ctx context.Context, refresh bool) (string, error)
}

type subscriptionSpec struct {
	table    string
	filter   map[string]any
	callback func(ChangeEvent)
	onError  func(error)
	onReset  func()
}

type closer interface {
	unsubscribe(ctx context.Context) error
}

type databaseTransport interface {
	query(ctx context.Context, call statementCall) ([]Row, error)
	execute(ctx context.Context, call statementCall) ([]ExecuteResult, error)
	transaction(ctx context.Context, statements []Statement, concern *WriteConcern) ([]ExecuteResult, error)
	batch(ctx context.Context, sql string, paramsBatch []any, concern *WriteConcern) ([]ExecuteResult, error)
	load(ctx context.Context, sql string, paramsBatch []any, options LoadOptions) (LoadResult, error)
	subscribe(ctx context.Context, spec subscriptionSpec) (closer, error)
	live(ctx context.Context, spec liveSpec) (closer, error)
	ack(ctx context.Context, deviceID string, seq int64) (*AckResponse, error)
	close()
}

func wholeNumber(value float64, field string) (int64, error) {
	if value != math.Trunc(value) || math.Abs(value) > maxSafeInteger {
		return 0, invalidResponse("The server sent %s as %v, which is not a whole number", field, value)
	}
	return int64(value), nil
}

func executeResultOf(response ExecuteResponse) (ExecuteResult, error) {
	changes, err := wholeNumber(response.Changes, "changes")
	if err != nil {
		return ExecuteResult{}, err
	}
	result := ExecuteResult{Changes: changes}
	switch {
	case response.LastInsertRowID.Number != nil:
		result.LastInsertRowID, err = wholeNumber(*response.LastInsertRowID.Number, "lastInsertRowId")
	case response.LastInsertRowID.String != nil:
		result.LastInsertRowID, err = strconv.ParseInt(*response.LastInsertRowID.String, 10, 64)
		if err != nil {
			err = invalidResponse("The server sent lastInsertRowId as %q, which is not a 64-bit integer", *response.LastInsertRowID.String)
		}
	default:
		err = invalidResponse("The server sent an execute result with no lastInsertRowId")
	}
	return result, err
}

func executeResultsOf(responses []ExecuteResponse) ([]ExecuteResult, error) {
	results := make([]ExecuteResult, 0, len(responses))
	for _, response := range responses {
		result, err := executeResultOf(response)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func loadResultOf(rowsLoaded, changes float64) (LoadResult, error) {
	loaded, err := wholeNumber(rowsLoaded, "rowsLoaded")
	if err != nil {
		return LoadResult{}, err
	}
	changed, err := wholeNumber(changes, "changes")
	if err != nil {
		return LoadResult{}, err
	}
	return LoadResult{RowsLoaded: loaded, Changes: changed}, nil
}

func rowsOf(raw []map[string]any) []Row {
	rows := make([]Row, len(raw))
	for position, row := range raw {
		if row == nil {
			row = Row{}
		}
		rows[position] = row
	}
	return rows
}

func paramsTree(params any) (map[string]any, []any, error) {
	if params == nil {
		return nil, nil, nil
	}
	tree, err := wireTree(params)
	if err != nil {
		return nil, nil, err
	}
	switch typed := tree.(type) {
	case nil:
		return nil, nil, nil
	case map[string]any:
		return typed, nil, nil
	case []any:
		return nil, typed, nil
	}
	return nil, nil, invalidArgument("Statement parameters must be a list of positional values or a map of named values")
}

func batchTrees(paramsBatch []any) ([]any, error) {
	if len(paramsBatch) == 0 {
		return nil, invalidArgument("A batch or a load needs at least one parameter set")
	}
	out := make([]any, len(paramsBatch))
	for position, params := range paramsBatch {
		named, positional, err := paramsTree(params)
		if err != nil {
			return nil, err
		}
		switch {
		case named != nil:
			out[position] = named
		case positional != nil:
			out[position] = positional
		default:
			out[position] = []any{}
		}
	}
	return out, nil
}

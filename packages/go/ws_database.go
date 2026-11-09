package sirannon

import (
	"context"
	"encoding/json"
)

func decodeData(data json.RawMessage, target any) error {
	if jsonKindOf(data) != "object" {
		return invalidResponse("The server answered with a result whose data is not a JSON object")
	}
	if err := decodeWire(data, target); err != nil {
		return invalidResponse("The server answered with a result the client cannot decode: %v", err)
	}
	return nil
}

func (t *wsTransport) query(ctx context.Context, call statementCall) ([]Row, error) {
	id := t.nextID()
	frame := WSQueryMessage{Type: "query", ID: id, ReadConcern: call.readConcern}
	if call.name != "" {
		frame.Name, frame.Args = Ptr(call.name), call.args
	} else {
		named, positional, err := paramsTree(call.params)
		if err != nil {
			return nil, err
		}
		frame.SQL = Ptr(call.sql)
		if named != nil || positional != nil {
			frame.Params = &WSQueryMessageParams{Map: named, List: positional}
		}
	}
	data, err := t.request(ctx, id, frame, true)
	if err != nil {
		return nil, err
	}
	var answer QueryResponse
	if err := decodeData(data, &answer); err != nil {
		return nil, err
	}
	return rowsOf(answer.Rows), nil
}

func (t *wsTransport) execute(ctx context.Context, call statementCall) ([]ExecuteResult, error) {
	id := t.nextID()
	frame := WSExecuteMessage{Type: "execute", ID: id, WriteConcern: call.writeConcern}
	if call.name != "" {
		frame.Name, frame.Args = Ptr(call.name), call.args
		data, err := t.request(ctx, id, frame, true)
		if err != nil {
			return nil, err
		}
		var answer TransactionResponse
		if err := decodeData(data, &answer); err != nil {
			return nil, err
		}
		return executeResultsOf(answer.Results)
	}
	named, positional, err := paramsTree(call.params)
	if err != nil {
		return nil, err
	}
	frame.SQL = Ptr(call.sql)
	if named != nil || positional != nil {
		frame.Params = &WSExecuteMessageParams{Map: named, List: positional}
	}
	data, err := t.request(ctx, id, frame, true)
	if err != nil {
		return nil, err
	}
	var answer ExecuteResponse
	if err := decodeData(data, &answer); err != nil {
		return nil, err
	}
	result, err := executeResultOf(answer)
	if err != nil {
		return nil, err
	}
	return []ExecuteResult{result}, nil
}

func (t *wsTransport) transaction(ctx context.Context, statements []Statement, concern *WriteConcern) ([]ExecuteResult, error) {
	id := t.nextID()
	frame := WSTransactionMessage{Type: "transaction", ID: id, WriteConcern: concern, Statements: []TransactionStatement{}}
	for _, statement := range statements {
		entry, err := transactionStatement(statement)
		if err != nil {
			return nil, err
		}
		frame.Statements = append(frame.Statements, entry)
	}
	data, err := t.request(ctx, id, frame, true)
	if err != nil {
		return nil, err
	}
	var answer TransactionResponse
	if err := decodeData(data, &answer); err != nil {
		return nil, err
	}
	return executeResultsOf(answer.Results)
}

func (t *wsTransport) batch(ctx context.Context, sql string, paramsBatch []any, concern *WriteConcern) ([]ExecuteResult, error) {
	trees, err := batchTrees(paramsBatch)
	if err != nil {
		return nil, err
	}
	id := t.nextID()
	frame := map[string]any{"type": "batch", "id": id, "sql": sql, "paramsBatch": trees}
	if concern != nil {
		frame["writeConcern"] = concern
	}
	data, err := t.request(ctx, id, frame, true)
	if err != nil {
		return nil, err
	}
	var answer BatchResponse
	if err := decodeData(data, &answer); err != nil {
		return nil, err
	}
	return executeResultsOf(answer.Results)
}

func (t *wsTransport) load(ctx context.Context, sql string, paramsBatch []any, options LoadOptions) (LoadResult, error) {
	trees, err := batchTrees(paramsBatch)
	if err != nil {
		return LoadResult{}, err
	}
	id := t.nextID()
	frame := map[string]any{"type": "load", "id": id, "sql": sql, "paramsBatch": trees}
	if options.Durability != "" {
		frame["durability"] = options.Durability
	}
	if options.Checkpoint != nil {
		frame["checkpoint"] = *options.Checkpoint
	}
	data, err := t.request(ctx, id, frame, true)
	if err != nil {
		return LoadResult{}, err
	}
	var answer BulkLoadResult
	if err := decodeData(data, &answer); err != nil {
		return LoadResult{}, err
	}
	return loadResultOf(answer.RowsLoaded, answer.Changes)
}

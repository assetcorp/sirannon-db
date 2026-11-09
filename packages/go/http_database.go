package sirannon

import (
	"context"
)

type httpDatabase struct {
	transport *httpTransport
	prefix    string
}

func newHTTPDatabase(transport *httpTransport, databaseID string) *httpDatabase {
	return &httpDatabase{transport: transport, prefix: "/db/" + escapePathSegment(databaseID)}
}

func httpOnlyWebSocket(feature string) *Error {
	return newError(CodeTransportError, feature+" need the WebSocket transport. Create the client with Options.Transport set to TransportWebSocket to use them.")
}

func (d *httpDatabase) query(ctx context.Context, call statementCall) ([]Row, error) {
	var answer QueryResponse
	if call.name != "" {
		body := OperationQueryRequest{Args: call.args, ReadConcern: call.readConcern}
		if err := d.transport.postJSON(ctx, d.prefix+"/query/"+escapePathSegment(call.name), body, &answer); err != nil {
			return nil, err
		}
		return rowsOf(answer.Rows), nil
	}
	named, positional, err := paramsTree(call.params)
	if err != nil {
		return nil, err
	}
	body := QueryRequest{SQL: call.sql, ReadConcern: call.readConcern}
	if named != nil || positional != nil {
		body.Params = &QueryRequestParams{Map: named, List: positional}
	}
	if err := d.transport.postJSON(ctx, d.prefix+"/query", body, &answer); err != nil {
		return nil, err
	}
	return rowsOf(answer.Rows), nil
}

func (d *httpDatabase) execute(ctx context.Context, call statementCall) ([]ExecuteResult, error) {
	if call.name != "" {
		var answer OperationExecuteResponse
		body := OperationExecuteRequest{Args: call.args, WriteConcern: call.writeConcern}
		if err := d.transport.postJSON(ctx, d.prefix+"/execute/"+escapePathSegment(call.name), body, &answer); err != nil {
			return nil, err
		}
		return executeResultsOf(answer.Results)
	}
	named, positional, err := paramsTree(call.params)
	if err != nil {
		return nil, err
	}
	body := ExecuteRequest{SQL: call.sql, WriteConcern: call.writeConcern}
	if named != nil || positional != nil {
		body.Params = &ExecuteRequestParams{Map: named, List: positional}
	}
	var answer ExecuteResponse
	if err := d.transport.postJSON(ctx, d.prefix+"/execute", body, &answer); err != nil {
		return nil, err
	}
	result, err := executeResultOf(answer)
	if err != nil {
		return nil, err
	}
	return []ExecuteResult{result}, nil
}

func (d *httpDatabase) transaction(ctx context.Context, statements []Statement, concern *WriteConcern) ([]ExecuteResult, error) {
	body := TransactionRequest{WriteConcern: concern}
	for _, statement := range statements {
		entry, err := transactionStatement(statement)
		if err != nil {
			return nil, err
		}
		body.Statements = append(body.Statements, entry)
	}
	var answer TransactionResponse
	if err := d.transport.postJSON(ctx, d.prefix+"/transaction", body, &answer); err != nil {
		return nil, err
	}
	return executeResultsOf(answer.Results)
}

func transactionStatement(statement Statement) (TransactionStatement, error) {
	named, positional, err := paramsTree(statement.Params)
	if err != nil {
		return TransactionStatement{}, err
	}
	entry := TransactionStatement{SQL: statement.SQL}
	if named != nil || positional != nil {
		entry.Params = &TransactionStatementParams{Map: named, List: positional}
	}
	return entry, nil
}

func (d *httpDatabase) batch(ctx context.Context, sql string, paramsBatch []any, concern *WriteConcern) ([]ExecuteResult, error) {
	trees, err := batchTrees(paramsBatch)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"sql": sql, "paramsBatch": trees}
	if concern != nil {
		body["writeConcern"] = concern
	}
	var answer BatchResponse
	if err := d.transport.postJSON(ctx, d.prefix+"/batch", body, &answer); err != nil {
		return nil, err
	}
	return executeResultsOf(answer.Results)
}

func (d *httpDatabase) load(ctx context.Context, sql string, paramsBatch []any, options LoadOptions) (LoadResult, error) {
	trees, err := batchTrees(paramsBatch)
	if err != nil {
		return LoadResult{}, err
	}
	body := map[string]any{"sql": sql, "paramsBatch": trees}
	if options.Durability != "" {
		body["durability"] = options.Durability
	}
	if options.Checkpoint != nil {
		body["checkpoint"] = *options.Checkpoint
	}
	var answer LoadResponse
	if err := d.transport.postJSON(ctx, d.prefix+"/load", body, &answer); err != nil {
		return LoadResult{}, err
	}
	return loadResultOf(answer.RowsLoaded, answer.Changes)
}

func (d *httpDatabase) subscribe(context.Context, subscriptionSpec) (closer, error) {
	return nil, httpOnlyWebSocket("Subscriptions")
}

func (d *httpDatabase) live(context.Context, liveSpec) (closer, error) {
	return nil, httpOnlyWebSocket("Live queries")
}

func (d *httpDatabase) ack(context.Context, string, int64) (*AckResponse, error) {
	return nil, httpOnlyWebSocket("Acknowledgements")
}

func (d *httpDatabase) close() {}

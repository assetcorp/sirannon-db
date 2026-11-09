package sirannon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// The client returns these codes when it fails before the server answers, while it passes every code
// that the server sends through unchanged.
const (
	// CodeConnectionError means that the client could not reach the server, that the connection broke
	// before the answer came back, or that the caller cancelled the context of the call.
	CodeConnectionError = "CONNECTION_ERROR"
	// CodeTimeout means that the deadline of the context or of [Options.RequestTimeout] passed before
	// the server answered.
	CodeTimeout = "TIMEOUT"
	// CodeTransportError means that the client is closed, or that the call needs the WebSocket
	// transport while the client uses HTTP.
	CodeTransportError = "TRANSPORT_ERROR"
	// CodeInvalidResponse means that the client could not decode the server's answer, or that the
	// answer passed [Options.MaxResponseBytes].
	CodeInvalidResponse = "INVALID_RESPONSE"
	// CodeRoutingError means that the topology client found no node to send the call to.
	CodeRoutingError = "ROUTING_ERROR"
	// CodeNoSafePrimary means that the replication group has no current primary for a write or for a
	// linearizable read.
	CodeNoSafePrimary = "NO_SAFE_PRIMARY"
	// CodeInvalidArgument means that the client refused an argument before it sent the call.
	CodeInvalidArgument = "INVALID_ARGUMENT"
	// CodeSQLNotAccepted means that the server refuses SQL statements, so the client refused the
	// statement before it sent the call. That server still accepts registered operations.
	CodeSQLNotAccepted = "SQL_NOT_ACCEPTED"
	// CodeUnauthorized means that the server refused the WebSocket connection with close code 4401 or
	// with a refusal code from 4000 to 4099. [Error.Details] holds that close code under closeCode.
	CodeUnauthorized = "UNAUTHORIZED"
	// CodeForbidden means that the server refused the WebSocket connection with close code 4403.
	// [Error.Details] holds that close code under closeCode.
	CodeForbidden = "FORBIDDEN"
)

const (
	codeNotFound              = "NOT_FOUND"
	codeRegistryMismatch      = "REGISTRY_MISMATCH"
	codeDuplicateSubscription = "DUPLICATE_SUBSCRIPTION"
	codeUnknownError          = "UNKNOWN_ERROR"
)

const sqlRefusedMessage = "This server does not accept SQL over the network. Call a registered operation by name, or start the server with acceptSql: true."

// Error is the type of every failure that the client returns. Read it with errors.As, or read its
// code alone with [ErrorCode].
type Error struct {
	// Code says what failed, such as SQL_NOT_ACCEPTED. It holds the code that the server sent, or one
	// of the Code constants when the client failed before the server answered.
	Code string
	// Message is a description of the failure for a log or for a person.
	Message string
	// Details holds the values behind the failure as the server sent them, or an empty map when the
	// server sends none.
	Details map[string]any
	// Status is the HTTP status of the answer that held the error, or 0 when no HTTP answer held it,
	// as on the WebSocket transport.
	Status int
	cause  error
}

// Error returns the code and the message, such as "TRANSPORT_ERROR: The client is closed".
func (e *Error) Error() string {
	return e.Code + ": " + e.Message
}

// Unwrap returns the failure underneath, such as context.DeadlineExceeded, so that errors.Is can find
// it. It returns nil when the client recorded no underlying failure.
func (e *Error) Unwrap() error {
	return e.cause
}

// ErrorCode returns the Code of the first [*Error] in the chain of err, or "" when the chain holds
// none.
func ErrorCode(err error) string {
	var failure *Error
	if errors.As(err, &failure) {
		return failure.Code
	}
	return ""
}

func newError(code, message string) *Error {
	return &Error{Code: code, Message: message, Details: map[string]any{}}
}

func wrapError(code, message string, cause error) *Error {
	failure := newError(code, message)
	failure.cause = cause
	return failure
}

func invalidArgument(format string, values ...any) *Error {
	return newError(CodeInvalidArgument, fmt.Sprintf(format, values...))
}

func invalidResponse(format string, values ...any) *Error {
	return newError(CodeInvalidResponse, fmt.Sprintf(format, values...))
}

func transportClosed() *Error {
	return newError(CodeTransportError, "The client is closed")
}

func contextFailure(err error, target string) *Error {
	if errors.Is(err, context.DeadlineExceeded) {
		return wrapError(CodeTimeout, fmt.Sprintf("The deadline passed before %s answered", target), err)
	}
	return wrapError(CodeConnectionError, fmt.Sprintf("The caller cancelled the request to %s", target), err)
}

type errorBody struct {
	Code    *string        `json:"code"`
	Message *string        `json:"message"`
	Details map[string]any `json:"details"`
}

type errorEnvelope struct {
	Error *errorBody `json:"error"`
}

func errorFromBody(status int, body []byte, target string) *Error {
	var envelope errorEnvelope
	if decodeWire(body, &envelope) != nil || envelope.Error == nil || envelope.Error.Code == nil {
		failure := invalidResponse("%s answered HTTP %d without a Sirannon error envelope", target, status)
		failure.Status = status
		return failure
	}
	message := fmt.Sprintf("HTTP %d", status)
	if envelope.Error.Message != nil {
		message = *envelope.Error.Message
	}
	failure := newError(*envelope.Error.Code, message)
	if envelope.Error.Details != nil {
		failure.Details = envelope.Error.Details
	}
	failure.Status = status
	return failure
}

func remoteFrameError(raw json.RawMessage) *Error {
	var body errorBody
	if len(raw) == 0 || decodeWire(raw, &body) != nil {
		return newError(codeUnknownError, "The server sent an error frame that carried no error")
	}
	code := codeUnknownError
	if body.Code != nil {
		code = *body.Code
	}
	message := "The server sent an error frame that carried no message"
	if body.Message != nil {
		message = *body.Message
	}
	failure := newError(code, message)
	if body.Details != nil {
		failure.Details = body.Details
	}
	return failure
}

package sirannon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	CodeConnectionError = "CONNECTION_ERROR"
	CodeTimeout         = "TIMEOUT"
	CodeTransportError  = "TRANSPORT_ERROR"
	CodeInvalidResponse = "INVALID_RESPONSE"
	CodeRoutingError    = "ROUTING_ERROR"
	CodeNoSafePrimary   = "NO_SAFE_PRIMARY"
	CodeInvalidArgument = "INVALID_ARGUMENT"
	CodeSQLNotAccepted  = "SQL_NOT_ACCEPTED"
	CodeUnauthorized    = "UNAUTHORIZED"
	CodeForbidden       = "FORBIDDEN"
)

const (
	codeNotFound              = "NOT_FOUND"
	codeRegistryMismatch      = "REGISTRY_MISMATCH"
	codeDuplicateSubscription = "DUPLICATE_SUBSCRIPTION"
	codeUnknownError          = "UNKNOWN_ERROR"
)

const sqlRefusedMessage = "This server does not accept SQL over the network. Call a registered operation by name, or start the server with acceptSql: true."

type Error struct {
	Code    string
	Message string
	Details map[string]any
	Status  int
	cause   error
}

func (e *Error) Error() string {
	return e.Code + ": " + e.Message
}

func (e *Error) Unwrap() error {
	return e.cause
}

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

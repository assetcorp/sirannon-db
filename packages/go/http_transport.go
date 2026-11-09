package sirannon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type exchange struct {
	method      string
	path        string
	query       url.Values
	contentType string
	payload     []byte
}

type answered struct {
	status int
	body   []byte
	url    string
}

func (a *answered) text() string {
	return strings.ToValidUTF8(string(a.body), "�")
}

type httpTransport struct {
	base     string
	settings *settings
	lifetime context.Context
}

func newHTTPTransport(base string, resolved *settings, lifetime context.Context) *httpTransport {
	return &httpTransport{base: base, settings: resolved, lifetime: lifetime}
}

func readBounded(body io.Reader, ceiling int64) ([]byte, bool, error) {
	if ceiling == 0 {
		data, err := io.ReadAll(body)
		return data, false, err
	}
	data, err := io.ReadAll(io.LimitReader(body, ceiling+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(data)) > ceiling {
		return nil, true, nil
	}
	return data, false, nil
}

func (t *httpTransport) callTimeout(call callSettings) time.Duration {
	if call.timeout != nil {
		return max(*call.timeout, 0)
	}
	return t.settings.requestTimeout
}

func (t *httpTransport) failure(ctx, tryCtx context.Context, err error, target string, timeout time.Duration) *Error {
	if t.lifetime.Err() != nil {
		return wrapError(CodeTransportError, "The client closed before "+target+" answered", err)
	}
	if ctx.Err() != nil {
		return contextFailure(ctx.Err(), target)
	}
	if errors.Is(tryCtx.Err(), context.DeadlineExceeded) {
		return wrapError(CodeTimeout, fmt.Sprintf("%s did not answer within %s", target, timeout), err)
	}
	return wrapError(CodeConnectionError, fmt.Sprintf("The client could not reach %s: %v", target, redactedCause(err)), err)
}

func redactedCause(err error) string {
	var urlFailure *url.Error
	if errors.As(err, &urlFailure) {
		return urlFailure.Err.Error()
	}
	return err.Error()
}

func (t *httpTransport) send(ctx context.Context, request exchange, options []CallOption) (*answered, error) {
	call := callSettings{}
	for _, option := range options {
		if option != nil {
			option(&call)
		}
	}
	target := t.base + request.path
	if len(request.query) > 0 {
		target += "?" + request.query.Encode()
	}
	label := request.method + " " + t.base + request.path
	if t.lifetime.Err() != nil {
		return nil, transportClosed()
	}
	if ctx.Err() != nil {
		return nil, contextFailure(ctx.Err(), label)
	}
	timeout := t.callTimeout(call)
	tryCtx, cancel := boundedContext(ctx, timeout)
	defer cancel()
	stop := context.AfterFunc(t.lifetime, cancel)
	defer stop()
	var body io.Reader
	if request.payload != nil {
		body = bytes.NewReader(request.payload)
	}
	httpRequest, err := http.NewRequestWithContext(tryCtx, request.method, target, body)
	if err != nil {
		return nil, invalidArgument("The client cannot build a request to %s: %v", label, redactedCause(err))
	}
	httpRequest.Header = t.settings.headers.Clone()
	for name, values := range call.headers {
		httpRequest.Header[name] = values
	}
	if request.contentType != "" {
		httpRequest.Header.Set("Content-Type", request.contentType)
	}
	httpRequest.Header.Set("Accept", jsonContentType)
	response, err := t.settings.httpClient.Do(httpRequest)
	if err != nil {
		return nil, t.failure(ctx, tryCtx, err, label, timeout)
	}
	defer response.Body.Close()
	data, tooLarge, err := readBounded(response.Body, t.settings.maxResponseBytes)
	if err != nil {
		return nil, t.failure(ctx, tryCtx, err, label, timeout)
	}
	if tooLarge {
		failure := invalidResponse("%s answered more than the %d bytes that this client reads", label, t.settings.maxResponseBytes)
		failure.Status = response.StatusCode
		return nil, failure
	}
	if t.lifetime.Err() != nil {
		return nil, transportClosed()
	}
	return &answered{status: response.StatusCode, body: data, url: label}, nil
}

func (t *httpTransport) exchange(ctx context.Context, request exchange, options []CallOption) (*answered, error) {
	answer, err := t.send(ctx, request, options)
	if err != nil {
		return nil, err
	}
	if answer.status < 200 || answer.status >= 300 {
		return nil, errorFromBody(answer.status, answer.body, answer.url)
	}
	return answer, nil
}

func (t *httpTransport) postJSON(ctx context.Context, path string, payload any, result any) error {
	encoded, err := encodeWire(payload)
	if err != nil {
		return err
	}
	answer, err := t.exchange(ctx, exchange{method: http.MethodPost, path: path, contentType: jsonContentType, payload: encoded}, nil)
	if err != nil {
		return err
	}
	return decodeInto(answer, result)
}

func (t *httpTransport) getJSON(ctx context.Context, path string, result any, options ...CallOption) error {
	answer, err := t.exchange(ctx, exchange{method: http.MethodGet, path: path}, options)
	if err != nil {
		return err
	}
	return decodeInto(answer, result)
}

func decodeInto(answer *answered, result any) error {
	if jsonKindOf(answer.body) != "object" {
		failure := invalidResponse("The answer from %s is not a JSON object (HTTP %d)", answer.url, answer.status)
		failure.Status = answer.status
		return failure
	}
	if err := decodeWire(answer.body, result); err != nil {
		failure := invalidResponse("The answer from %s does not hold the shape that the route documents: %v", answer.url, err)
		failure.Status = answer.status
		return failure
	}
	return nil
}

const jsonContentType = "application/json"

func boundedContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return context.WithCancel(ctx)
}

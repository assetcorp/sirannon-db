package sirannon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const (
	closeUnauthenticated = 4401
	closeForbidden       = 4403
	refusalCloseCodeLow  = 4000
	refusalCloseCodeHigh = 4099
	clientCloseReason    = "Client closed"
)

type frameResult struct {
	data json.RawMessage
	err  error
}

type pendingRequest struct {
	result chan frameResult
}

type dialAttempt struct {
	done chan struct{}
	err  error
}

type wsTransport struct {
	url      string
	settings *settings
	lifetime context.Context
	cancel   context.CancelFunc
	tasks    *taskGroup
	events   *dispatcher
	counter  atomic.Uint64

	mutex          sync.Mutex
	conn           *websocket.Conn
	dialing        *dialAttempt
	refusal        *Error
	closed         bool
	pending        map[string]*pendingRequest
	subscriptions  map[string]*activeSubscription
	lives          map[string]*activeLive
	reconnectTimer *time.Timer
	needsRestore   bool
	restoring      *restoreAttempt
}

func newWSTransport(url string, resolved *settings, parent context.Context) *wsTransport {
	lifetime, cancel := context.WithCancel(parent)
	return &wsTransport{
		url:           url,
		settings:      resolved,
		lifetime:      lifetime,
		cancel:        cancel,
		tasks:         newTaskGroup(),
		events:        startDispatcher(lifetime),
		pending:       map[string]*pendingRequest{},
		subscriptions: map[string]*activeSubscription{},
		lives:         map[string]*activeLive{},
	}
}

func (t *wsTransport) nextID() string {
	return "c_" + strconv.FormatUint(t.counter.Add(1), 10) + "_" + strconv.FormatInt(time.Now().UnixMilli(), 10)
}

func isRefusalCode(code int) bool {
	return code == closeUnauthenticated || code == closeForbidden || (code >= refusalCloseCodeLow && code <= refusalCloseCodeHigh)
}

func refusalError(code int, reason string) *Error {
	message := reason
	if message == "" {
		message = fmt.Sprintf("The server refused the WebSocket connection with close code %d", code)
	}
	errorCode := CodeUnauthorized
	if code == closeForbidden {
		errorCode = CodeForbidden
	}
	failure := newError(errorCode, message)
	failure.Details["closeCode"] = code
	return failure
}

func (t *wsTransport) connection(ctx context.Context, dial bool) (*websocket.Conn, error) {
	for {
		t.mutex.Lock()
		switch {
		case t.closed:
			t.mutex.Unlock()
			return nil, transportClosed()
		case t.refusal != nil:
			refusal := t.refusal
			t.mutex.Unlock()
			return nil, refusal
		case t.conn != nil:
			conn := t.conn
			t.mutex.Unlock()
			return conn, nil
		case !dial:
			t.mutex.Unlock()
			return nil, nil
		}
		attempt := t.dialing
		if attempt == nil {
			attempt = &dialAttempt{done: make(chan struct{})}
			if !t.tasks.start(func() { t.dial(attempt) }) {
				t.mutex.Unlock()
				return nil, transportClosed()
			}
			t.dialing = attempt
		}
		t.mutex.Unlock()
		select {
		case <-attempt.done:
			if attempt.err != nil {
				return nil, attempt.err
			}
		case <-ctx.Done():
			return nil, contextFailure(ctx.Err(), t.url)
		}
	}
}

func (t *wsTransport) dial(attempt *dialAttempt) {
	ctx, cancel := boundedContext(t.lifetime, t.settings.requestTimeout)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, t.url, &websocket.DialOptions{
		HTTPClient:   t.settings.httpClient,
		HTTPHeader:   t.settings.headers.Clone(),
		Subprotocols: t.settings.protocols,
	})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	t.mutex.Lock()
	defer close(attempt.done)
	defer t.mutex.Unlock()
	t.dialing = nil
	switch {
	case err != nil && t.lifetime.Err() != nil:
		attempt.err = transportClosed()
	case err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded):
		attempt.err = wrapError(CodeTimeout, fmt.Sprintf("The WebSocket handshake with %s took longer than %s", t.url, t.settings.requestTimeout), err)
	case err != nil:
		attempt.err = wrapError(CodeConnectionError, fmt.Sprintf("Failed to connect to %s: %v", t.url, err), err)
	case t.closed:
		_ = conn.CloseNow()
		attempt.err = transportClosed()
	default:
		conn.SetReadLimit(webSocketReadLimit(t.settings.maxResponseBytes))
		if !t.tasks.start(func() { t.readLoop(conn) }) {
			_ = conn.CloseNow()
			attempt.err = transportClosed()
			return
		}
		t.conn = conn
	}
}

func (t *wsTransport) readLoop(conn *websocket.Conn) {
	for {
		t.paceCallbacks()
		_, data, err := conn.Read(t.lifetime)
		if err != nil {
			t.connectionLost(conn, err)
			return
		}
		t.route(data)
	}
}

func (t *wsTransport) connectionLost(conn *websocket.Conn, cause error) {
	code := int(websocket.CloseStatus(cause))
	reason := ""
	var closeFailure websocket.CloseError
	if errors.As(cause, &closeFailure) {
		reason = closeFailure.Reason
	}
	t.mutex.Lock()
	if t.conn == conn {
		t.conn = nil
	}
	refusedNow := false
	if isRefusalCode(code) && t.refusal == nil && !t.closed {
		t.refusal = refusalError(code, reason)
		refusedNow = true
	}
	var failure *Error
	switch {
	case t.closed:
		failure = transportClosed()
	case t.refusal != nil:
		failure = t.refusal
	default:
		failure = wrapError(CodeConnectionError, fmt.Sprintf("The WebSocket connection to %s closed: %v", t.url, cause), cause)
	}
	pending := t.pending
	t.pending = map[string]*pendingRequest{}
	var subscriptions []*activeSubscription
	for _, subscription := range t.subscriptions {
		subscription.established = false
		subscriptions = append(subscriptions, subscription)
	}
	var lives []*activeLive
	for _, live := range t.lives {
		live.established = false
		lives = append(lives, live)
	}
	restorable := len(subscriptions)+len(lives) > 0
	if restorable && !t.closed {
		t.needsRestore = true
		if t.settings.autoReconnect && t.refusal == nil {
			t.scheduleReconnectLocked()
		}
	}
	t.mutex.Unlock()
	for _, waiting := range pending {
		waiting.result <- frameResult{err: failure}
	}
	for _, live := range lives {
		if refusedNow {
			live.spec.handlers.fail(failure)
			continue
		}
		live.spec.handlers.revalidating()
	}
	if refusedNow {
		for _, subscription := range subscriptions {
			onError := subscription.spec.onError
			t.events.post(func() { reportFailure(onError, failure) })
		}
	}
}

func (t *wsTransport) scheduleReconnectLocked() {
	if t.reconnectTimer != nil || t.closed {
		return
	}
	t.reconnectTimer = time.AfterFunc(t.settings.reconnectInterval, func() {
		t.tasks.start(t.reconnect)
	})
}

func (t *wsTransport) holdsRestorableLocked() bool {
	return len(t.subscriptions)+len(t.lives) > 0
}

func (t *wsTransport) reconnect() {
	t.mutex.Lock()
	t.reconnectTimer = nil
	closed := t.closed
	t.mutex.Unlock()
	if closed {
		return
	}
	_, err := t.connection(t.lifetime, true)
	if err == nil {
		err = t.restore(t.lifetime)
	}
	if err == nil {
		return
	}
	t.mutex.Lock()
	if !t.closed && t.refusal == nil && t.holdsRestorableLocked() {
		t.scheduleReconnectLocked()
	}
	t.mutex.Unlock()
}

func (t *wsTransport) request(ctx context.Context, id string, frame any, dial bool) (json.RawMessage, error) {
	payload, err := encodeWire(frame)
	if err != nil {
		return nil, err
	}
	conn, err := t.connection(ctx, dial)
	if err != nil || conn == nil {
		return nil, err
	}
	waiting := &pendingRequest{result: make(chan frameResult, 1)}
	t.mutex.Lock()
	switch {
	case t.closed:
		t.mutex.Unlock()
		return nil, transportClosed()
	case t.refusal != nil:
		refusal := t.refusal
		t.mutex.Unlock()
		return nil, refusal
	case t.conn != conn:
		t.mutex.Unlock()
		return nil, newError(CodeConnectionError, "The WebSocket connection to "+t.url+" closed before the request left")
	case t.pending[id] != nil:
		t.mutex.Unlock()
		return nil, invalidArgument("A request with the id %s is still waiting for its answer", id)
	}
	t.pending[id] = waiting
	t.mutex.Unlock()
	timeout := t.settings.requestTimeout
	writeCtx, cancelWrite := boundedContext(ctx, timeout)
	writeErr := conn.Write(writeCtx, websocket.MessageText, payload)
	cancelWrite()
	if writeErr != nil {
		if ctx.Err() != nil {
			t.forget(id, waiting)
			return nil, contextFailure(ctx.Err(), t.url)
		}
		_ = conn.CloseNow()
	}
	var expiry <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		expiry = timer.C
	}
	select {
	case outcome := <-waiting.result:
		return outcome.data, outcome.err
	case <-ctx.Done():
		t.forget(id, waiting)
		return nil, contextFailure(ctx.Err(), t.url)
	case <-expiry:
		t.forget(id, waiting)
		return nil, newError(CodeTimeout, fmt.Sprintf("The request to %s timed out after %s", t.url, timeout))
	}
}

func (t *wsTransport) forget(id string, waiting *pendingRequest) {
	t.mutex.Lock()
	if t.pending[id] == waiting {
		delete(t.pending, id)
	}
	t.mutex.Unlock()
}

func (t *wsTransport) settle(id string, data json.RawMessage, failure error) bool {
	t.mutex.Lock()
	waiting := t.pending[id]
	delete(t.pending, id)
	t.mutex.Unlock()
	if waiting == nil {
		return false
	}
	waiting.result <- frameResult{data: data, err: failure}
	return true
}

func (t *wsTransport) close() {
	t.mutex.Lock()
	if t.closed {
		t.mutex.Unlock()
		return
	}
	t.closed = true
	if t.reconnectTimer != nil {
		t.reconnectTimer.Stop()
		t.reconnectTimer = nil
	}
	conn := t.conn
	t.conn = nil
	pending := t.pending
	t.pending = map[string]*pendingRequest{}
	t.subscriptions = map[string]*activeSubscription{}
	t.lives = map[string]*activeLive{}
	t.mutex.Unlock()
	failure := transportClosed()
	for _, waiting := range pending {
		waiting.result <- frameResult{err: failure}
	}
	if conn != nil {
		_ = conn.Close(websocket.StatusNormalClosure, clientCloseReason)
	}
	t.cancel()
	t.tasks.closeAndWait()
}

package sirannon

import (
	"net/http"
	"slices"
	"strings"
	"time"
)

// TransportKind is the protocol that the client uses to reach the server for each database.
type TransportKind string

const (
	// TransportWebSocket makes the client open one WebSocket connection for each database.
	// Subscriptions, live queries, and acknowledgements work only over this transport. The client uses
	// it unless you set another.
	TransportWebSocket TransportKind = "websocket"
	// TransportHTTP makes the client send each call as a separate HTTP request.
	TransportHTTP TransportKind = "http"
)

const (
	// NoTimeout, set as [Options.RequestTimeout], makes the client wait for as long as the server
	// takes. The context of each call still bounds the wait.
	NoTimeout = time.Duration(-1)
	// NoResponseLimit, set as [Options.MaxResponseBytes], makes the client read an answer of any size.
	NoResponseLimit          = int64(-1)
	defaultRequestTimeout    = 30 * time.Second
	defaultReconnectInterval = time.Second
	defaultMaxResponseBytes  = 64 << 20
	sirannonSubprotocol      = "sirannon.v1"
	headerTokenSymbols       = "!#$%&'*+-.^_`|~"
)

// Options holds the settings of a [Client]. The client applies a default to every field that you
// leave unset.
type Options struct {
	// Transport is the protocol that the client uses for each database. The client uses
	// [TransportWebSocket] unless you set another.
	Transport TransportKind
	// Headers holds the headers that the client sends with every HTTP request and every WebSocket
	// handshake, such as Authorization. New fails with INVALID_ARGUMENT for a header whose name is
	// not a header token or whose value holds a line break or a NUL byte.
	Headers map[string]string
	// WebSocketProtocols lists further subprotocols that the client offers in the WebSocket handshake
	// after sirannon.v1. New fails with INVALID_ARGUMENT unless each entry is a distinct header token.
	WebSocketProtocols []string
	// DisableAutoReconnect stops the client from reconnecting a WebSocket connection that drops while
	// its database has open subscriptions or live queries.
	DisableAutoReconnect bool
	// ReconnectInterval is how long the client waits before it reconnects a dropped WebSocket
	// connection, which is one second unless you set it.
	ReconnectInterval time.Duration
	// RequestTimeout is the longest that the client waits for each call and each WebSocket
	// handshake, which is 30 seconds unless you set it. [NoTimeout] makes the client wait for as long
	// as the server takes, bounded only by the context of the call.
	RequestTimeout time.Duration
	// MaxResponseBytes is the size of the largest answer that the client reads, which is 64 MiB unless
	// you set it. Over HTTP, the client stops reading an answer past this many bytes and fails with
	// INVALID_RESPONSE. Over WebSocket, the client closes a connection whose message passes the limit.
	// [NoResponseLimit] makes the client read whatever the server sends.
	MaxResponseBytes int64
	// HTTPClient sends every HTTP request and every WebSocket handshake. When you leave it unset, New
	// creates an http.Client that follows no redirects. Close then releases the idle connections of
	// that http.Client.
	HTTPClient *http.Client
}

type settings struct {
	transport         TransportKind
	headers           http.Header
	protocols         []string
	autoReconnect     bool
	reconnectInterval time.Duration
	requestTimeout    time.Duration
	maxResponseBytes  int64
	httpClient        *http.Client
	ownsHTTPClient    bool
}

func isHeaderToken(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r > 0x7e || r <= 0x20 {
			return false
		}
		if ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('0' <= r && r <= '9') || strings.ContainsRune(headerTokenSymbols, r) {
			continue
		}
		return false
	}
	return true
}

func resolveProtocols(configured []string) ([]string, error) {
	if len(configured) == 0 {
		return nil, nil
	}
	seen := map[string]bool{}
	for _, protocol := range configured {
		if !isHeaderToken(protocol) || seen[protocol] {
			return nil, invalidArgument("Options.WebSocketProtocols must hold distinct header tokens, and one entry is empty, repeated, or holds a character a header token forbids")
		}
		seen[protocol] = true
	}
	resolved := []string{sirannonSubprotocol}
	for _, protocol := range configured {
		if protocol != sirannonSubprotocol {
			resolved = append(resolved, protocol)
		}
	}
	return resolved, nil
}

func resolveSettings(options Options) (*settings, error) {
	resolved := &settings{
		transport:         options.Transport,
		headers:           http.Header{},
		autoReconnect:     !options.DisableAutoReconnect,
		reconnectInterval: options.ReconnectInterval,
		requestTimeout:    options.RequestTimeout,
		maxResponseBytes:  options.MaxResponseBytes,
		httpClient:        options.HTTPClient,
	}
	switch resolved.transport {
	case "":
		resolved.transport = TransportWebSocket
	case TransportWebSocket, TransportHTTP:
	default:
		return nil, invalidArgument("Options.Transport takes %q or %q", TransportWebSocket, TransportHTTP)
	}
	for name, value := range options.Headers {
		if !isHeaderToken(name) || strings.ContainsAny(value, "\r\n\x00") {
			return nil, invalidArgument("Options.Headers holds a header whose name or value an HTTP request cannot carry")
		}
		resolved.headers.Set(name, value)
	}
	protocols, err := resolveProtocols(slices.Clone(options.WebSocketProtocols))
	if err != nil {
		return nil, err
	}
	resolved.protocols = protocols
	if resolved.reconnectInterval < 0 {
		return nil, invalidArgument("Options.ReconnectInterval takes 0 or more")
	}
	if resolved.reconnectInterval == 0 {
		resolved.reconnectInterval = defaultReconnectInterval
	}
	switch {
	case resolved.requestTimeout == 0:
		resolved.requestTimeout = defaultRequestTimeout
	case resolved.requestTimeout < 0:
		resolved.requestTimeout = 0
	}
	switch {
	case resolved.maxResponseBytes == 0:
		resolved.maxResponseBytes = defaultMaxResponseBytes
	case resolved.maxResponseBytes < 0:
		resolved.maxResponseBytes = 0
	}
	if resolved.httpClient == nil {
		resolved.ownsHTTPClient = true
		resolved.httpClient = &http.Client{
			Transport:     http.DefaultTransport.(*http.Transport).Clone(),
			CheckRedirect: refuseRedirect,
		}
	}
	return resolved, nil
}

func refuseRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// CallOption changes one call of an [Operations] method. Pass any number of them as the last
// arguments.
type CallOption func(*callSettings)

type callSettings struct {
	timeout *time.Duration
	headers http.Header
}

// WithTimeout makes the client wait this long for the answer to this call, in place of
// [Options.RequestTimeout]. A timeout of zero or less makes the client wait for as long as the
// server takes, bounded only by the context.
func WithTimeout(timeout time.Duration) CallOption {
	return func(call *callSettings) {
		call.timeout = &timeout
	}
}

// WithHeader adds a header to this call, replacing a header of the same name from [Options.Headers].
func WithHeader(name, value string) CallOption {
	return func(call *callSettings) {
		if call.headers == nil {
			call.headers = http.Header{}
		}
		call.headers.Set(name, value)
	}
}

// QueryOption changes one read that [RemoteDatabase.Query] or [Query] sends.
type QueryOption func(*queryOptions)

type queryOptions struct {
	readConcern *ReadConcern
}

// WithReadConcern asks the server to confirm level, such as [ReadConcernLevelMajority], before it
// answers this read. A read through a [TopologyClient] fails with INVALID_ARGUMENT when you pass this
// option, because [TopologyOptions.ReadConcern] applies to every read there.
func WithReadConcern(level ReadConcernLevel) QueryOption {
	return func(options *queryOptions) {
		options.readConcern = &ReadConcern{Level: level}
	}
}

// ExecuteOption changes one write that [RemoteDatabase.Transaction], [RemoteDatabase.Batch], or
// [Execute] sends.
type ExecuteOption func(*executeOptions)

type executeOptions struct {
	writeConcern *WriteConcern
}

// WithWriteConcern makes the server confirm this write only once the nodes that level requires have
// acknowledged it. When the timeout is above zero, the server fails the call once that much time
// passes without those acknowledgements.
func WithWriteConcern(level WriteConcernLevel, timeout time.Duration) ExecuteOption {
	return func(options *executeOptions) {
		concern := &WriteConcern{Level: level}
		if timeout > 0 {
			concern.TimeoutMs = Ptr(float64(timeout.Milliseconds()))
		}
		options.writeConcern = concern
	}
}

func readQueryOptions(options []QueryOption) queryOptions {
	var resolved queryOptions
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	return resolved
}

func readExecuteOptions(options []ExecuteOption) executeOptions {
	var resolved executeOptions
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	return resolved
}

// LoadOptions holds the settings of one [RemoteDatabase.Load].
type LoadOptions struct {
	// Durability is the writer's durability level during the load, which the server sets to
	// [BulkLoadDurabilityOff] unless you set another.
	Durability BulkLoadDurability
	// Checkpoint sets whether the server checkpoints the WAL after this load, which it does unless you
	// set Checkpoint to false. Set it to false on every batch except the last of a load that you split
	// into batches, as [RemoteDatabase.LoadAll] does.
	Checkpoint *bool
}

// LoadAllOptions holds the settings of [RemoteDatabase.LoadAll].
type LoadAllOptions struct {
	// BatchSize is the number of rows that each request holds, which is 1000 unless you set it.
	// LoadAll fails with INVALID_ARGUMENT for a negative BatchSize.
	BatchSize int
	// Durability is the writer's durability level during the load, which the server sets to
	// [BulkLoadDurabilityOff] unless you set another.
	Durability BulkLoadDurability
}

package sirannon

import (
	"net/http"
	"slices"
	"strings"
	"time"
)

type TransportKind string

const (
	TransportWebSocket TransportKind = "websocket"
	TransportHTTP      TransportKind = "http"
)

const (
	NoTimeout                = time.Duration(-1)
	NoResponseLimit          = int64(-1)
	defaultRequestTimeout    = 30 * time.Second
	defaultReconnectInterval = time.Second
	defaultMaxResponseBytes  = 64 << 20
	sirannonSubprotocol      = "sirannon.v1"
	headerTokenSymbols       = "!#$%&'*+-.^_`|~"
)

type Options struct {
	Transport            TransportKind
	Headers              map[string]string
	WebSocketProtocols   []string
	DisableAutoReconnect bool
	ReconnectInterval    time.Duration
	RequestTimeout       time.Duration
	MaxResponseBytes     int64
	HTTPClient           *http.Client
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

type CallOption func(*callSettings)

type callSettings struct {
	timeout *time.Duration
	headers http.Header
}

func WithTimeout(timeout time.Duration) CallOption {
	return func(call *callSettings) {
		call.timeout = &timeout
	}
}

func WithHeader(name, value string) CallOption {
	return func(call *callSettings) {
		if call.headers == nil {
			call.headers = http.Header{}
		}
		call.headers.Set(name, value)
	}
}

type QueryOption func(*queryOptions)

type queryOptions struct {
	readConcern *ReadConcern
}

func WithReadConcern(level ReadConcernLevel) QueryOption {
	return func(options *queryOptions) {
		options.readConcern = &ReadConcern{Level: level}
	}
}

type ExecuteOption func(*executeOptions)

type executeOptions struct {
	writeConcern *WriteConcern
}

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

type LoadOptions struct {
	Durability BulkLoadDurability
	Checkpoint *bool
}

type LoadAllOptions struct {
	BatchSize  int
	Durability BulkLoadDurability
}

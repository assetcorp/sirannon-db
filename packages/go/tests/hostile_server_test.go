package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/coder/websocket"
)

type fakeFrameHandler func(ctx context.Context, conn *websocket.Conn, frame map[string]any)

type fakeServer struct {
	server   *httptest.Server
	mutex    sync.Mutex
	uris     []string
	frames   []map[string]any
	release  chan struct{}
	onFrame  fakeFrameHandler
	onHTTP   http.HandlerFunc
	received chan struct{}
}

func startFakeServer(t *testing.T, onFrame fakeFrameHandler, onHTTP http.HandlerFunc) *fakeServer {
	t.Helper()
	fake := &fakeServer{onFrame: onFrame, onHTTP: onHTTP, release: make(chan struct{}), received: make(chan struct{}, 64)}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(func() {
		close(fake.release)
		fake.server.CloseClientConnections()
		fake.server.Close()
	})
	return fake
}

func (f *fakeServer) seenURIs() []string {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return append([]string{}, f.uris...)
}

func (f *fakeServer) noteReceived() {
	select {
	case f.received <- struct{}{}:
	default:
	}
}

func (f *fakeServer) serve(writer http.ResponseWriter, request *http.Request) {
	f.mutex.Lock()
	f.uris = append(f.uris, request.Method+" "+request.RequestURI)
	f.mutex.Unlock()
	if request.URL.Path == "/capabilities" {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"capabilities":["query.sql","query.named"]}`))
		return
	}
	if strings.EqualFold(request.Header.Get("Upgrade"), "websocket") {
		f.serveSocket(writer, request)
		return
	}
	f.noteReceived()
	if f.onHTTP == nil {
		select {
		case <-request.Context().Done():
		case <-f.release:
		}
		return
	}
	f.onHTTP(writer, request)
}

func (f *fakeServer) serveSocket(writer http.ResponseWriter, request *http.Request) {
	conn, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(64 << 20)
	defer conn.CloseNow()
	ctx := request.Context()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var frame map[string]any
		if json.Unmarshal(data, &frame) != nil {
			continue
		}
		f.mutex.Lock()
		f.frames = append(f.frames, frame)
		f.mutex.Unlock()
		f.noteReceived()
		if f.onFrame != nil {
			f.onFrame(ctx, conn, frame)
		}
	}
}

func sendFrame(ctx context.Context, conn *websocket.Conn, frame map[string]any) {
	encoded, _ := json.Marshal(frame)
	_ = conn.Write(ctx, websocket.MessageText, encoded)
}

func sendRaw(ctx context.Context, conn *websocket.Conn, raw string) {
	_ = conn.Write(ctx, websocket.MessageText, []byte(raw))
}

func writeJSON(writer http.ResponseWriter, raw string) {
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(raw))
}

package tests

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
)

type tcpRelay struct {
	listener net.Listener
	target   string
	accepted atomic.Int64
	mutex    sync.Mutex
	open     map[net.Conn]struct{}
	refusing bool
	group    sync.WaitGroup
}

func startRelay(t *testing.T, target string) *tcpRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("open the relay: %v", err)
	}
	relay := &tcpRelay{listener: listener, target: target, open: map[net.Conn]struct{}{}}
	relay.group.Go(relay.serve)
	t.Cleanup(relay.close)
	return relay
}

func (r *tcpRelay) url() string {
	return "http://" + r.listener.Addr().String()
}

func (r *tcpRelay) serve() {
	for {
		inbound, err := r.listener.Accept()
		if err != nil {
			return
		}
		r.accepted.Add(1)
		r.mutex.Lock()
		refusing, target := r.refusing, r.target
		r.mutex.Unlock()
		if refusing {
			_ = inbound.Close()
			continue
		}
		outbound, err := net.Dial("tcp", target)
		if err != nil {
			_ = inbound.Close()
			continue
		}
		r.mutex.Lock()
		r.open[inbound], r.open[outbound] = struct{}{}, struct{}{}
		r.mutex.Unlock()
		r.group.Go(func() { r.pipe(inbound, outbound) })
		r.group.Go(func() { r.pipe(outbound, inbound) })
	}
}

func (r *tcpRelay) pipe(from, to net.Conn) {
	_, _ = io.Copy(to, from)
	_ = from.Close()
	_ = to.Close()
	r.mutex.Lock()
	delete(r.open, from)
	delete(r.open, to)
	r.mutex.Unlock()
}

func (r *tcpRelay) cut() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for conn := range r.open {
		_ = conn.Close()
	}
}

func (r *tcpRelay) refuse(refusing bool) {
	r.mutex.Lock()
	r.refusing = refusing
	r.mutex.Unlock()
}

func (r *tcpRelay) retarget(target string) {
	r.mutex.Lock()
	r.target = target
	r.mutex.Unlock()
}

func (r *tcpRelay) close() {
	_ = r.listener.Close()
	r.cut()
	r.group.Wait()
}

type countingProxy struct {
	server *httptest.Server
	mutex  sync.Mutex
	paths  []string
}

func startCountingProxy(t *testing.T, target string) *countingProxy {
	t.Helper()
	parsed, err := url.Parse(target)
	if err != nil {
		t.Fatalf("parse the proxy target: %v", err)
	}
	proxy := &countingProxy{}
	forward := httputil.NewSingleHostReverseProxy(parsed)
	proxy.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		proxy.mutex.Lock()
		proxy.paths = append(proxy.paths, request.Method+" "+request.URL.Path)
		proxy.mutex.Unlock()
		forward.ServeHTTP(writer, request)
	}))
	t.Cleanup(proxy.server.Close)
	return proxy
}

func (p *countingProxy) seen() []string {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return append([]string{}, p.paths...)
}

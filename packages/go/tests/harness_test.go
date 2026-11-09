package tests

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

const (
	serverStartDeadline = 90 * time.Second
	serverStopDeadline  = 15 * time.Second
	distRetryPause      = 2 * time.Second
)

type serverConfig struct {
	Mode            string `json:"mode,omitempty"`
	Directory       string `json:"directory"`
	HTTPPort        int    `json:"httpPort,omitempty"`
	Token           string `json:"token,omitempty"`
	ForbiddenToken  string `json:"forbiddenToken,omitempty"`
	AcceptSQL       *bool  `json:"acceptSql,omitempty"`
	CDCRetentionMs  int    `json:"cdcRetentionMs,omitempty"`
	Certificates    string `json:"certificates,omitempty"`
	GRPCPort        int    `json:"grpcPort,omitempty"`
	PrimaryGRPCPort int    `json:"primaryGrpcPort,omitempty"`
}

type readyLine struct {
	Port     int `json:"port"`
	GRPCPort int `json:"grpcPort"`
}

type lockedBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

type testServer struct {
	config   serverConfig
	url      string
	port     int
	grpcPort int
	command  *exec.Cmd
	exited   chan struct{}
	output   *lockedBuffer
	stopOnce sync.Once
}

func serverScript(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "ts", "scripts", "client-test-server.mjs"))
	if err != nil {
		t.Fatalf("resolve the test server script: %v", err)
	}
	return path
}

func distMissing(output string) bool {
	return strings.Contains(output, "ERR_MODULE_NOT_FOUND") || strings.Contains(output, "Cannot find module") || strings.Contains(output, "SyntaxError")
}

func startServer(t *testing.T, config serverConfig) *testServer {
	t.Helper()
	if config.Directory == "" {
		config.Directory = t.TempDir()
	}
	deadline := time.Now().Add(serverStartDeadline)
	for {
		server, output, err := launchServer(t, config)
		if err == nil {
			t.Cleanup(func() {
				server.stop()
				if t.Failed() {
					t.Logf("test server output:\n%s", server.output.String())
				}
			})
			return server
		}
		if !distMissing(output) || time.Now().After(deadline) {
			t.Fatalf("start the test server: %v\n%s", err, output)
		}
		time.Sleep(distRetryPause)
	}
}

func launchServer(t *testing.T, config serverConfig) (*testServer, string, error) {
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, "", err
	}
	script := serverScript(t)
	command := exec.Command("node", script)
	command.Dir = filepath.Dir(script)
	command.Env = append(os.Environ(), "SIRANNON_CLIENT_TEST_SERVER="+string(encoded))
	output := &lockedBuffer{}
	command.Stderr = output
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, "", err
	}
	if err := command.Start(); err != nil {
		return nil, "", err
	}
	server := &testServer{config: config, command: command, exited: make(chan struct{}), output: output}
	ready := make(chan readyLine, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = output.Write([]byte(line + "\n"))
			if payload, found := strings.CutPrefix(line, "READY "); found {
				var parsed readyLine
				if json.Unmarshal([]byte(payload), &parsed) == nil {
					ready <- parsed
				}
			}
		}
		_, _ = io.Copy(io.Discard, stdout)
	}()
	go func() {
		_ = command.Wait()
		close(server.exited)
	}()
	select {
	case parsed := <-ready:
		server.port, server.grpcPort = parsed.Port, parsed.GRPCPort
		server.url = fmt.Sprintf("http://127.0.0.1:%d", parsed.Port)
		return server, output.String(), nil
	case <-server.exited:
		return nil, output.String(), fmt.Errorf("the server exited before it printed READY")
	case <-time.After(serverStartDeadline):
		_ = command.Process.Kill()
		<-server.exited
		return nil, output.String(), fmt.Errorf("the server printed no READY line within %s", serverStartDeadline)
	}
}

func (s *testServer) stop() {
	s.stopOnce.Do(func() {
		_ = s.command.Process.Signal(syscall.SIGTERM)
		select {
		case <-s.exited:
		case <-time.After(serverStopDeadline):
			_ = s.command.Process.Kill()
			<-s.exited
		}
	})
}

func (s *testServer) databaseFile(id string) string {
	return filepath.Join(s.config.Directory, id+".db")
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type oracleItem struct {
	id          int64
	label       sql.NullString
	amount      sql.NullInt64
	amountType  string
	payload     []byte
	payloadType string
}

func oracleItems(t *testing.T, path string) []oracleItem {
	t.Helper()
	handle, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open the oracle on %s: %v", path, err)
	}
	defer handle.Close()
	rows, err := handle.Query("SELECT id, label, amount, typeof(amount), payload, typeof(payload) FROM items ORDER BY id")
	if err != nil {
		t.Fatalf("read items through the oracle: %v", err)
	}
	defer rows.Close()
	var items []oracleItem
	for rows.Next() {
		var item oracleItem
		if err := rows.Scan(&item.id, &item.label, &item.amount, &item.amountType, &item.payload, &item.payloadType); err != nil {
			t.Fatalf("scan an oracle row: %v", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate oracle rows: %v", err)
	}
	return items
}

func oracleCount(t *testing.T, path string, query string, args ...any) int64 {
	t.Helper()
	handle, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open the oracle on %s: %v", path, err)
	}
	defer handle.Close()
	var count int64
	if err := handle.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count through the oracle: %v", err)
	}
	return count
}

func waitFor(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for %s", timeout, description)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

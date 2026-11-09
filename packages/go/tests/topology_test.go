package tests

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	sirannon "github.com/assetcorp/sirannon-db/packages/go"
)

const replicatedDatabase = "items"

func TestReadsFallBackToThePrimaryWhenTheReplicaStopsMidBurst(t *testing.T) {
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		t.Run(string(transport), func(t *testing.T) {
			readsSurviveReplicaStop(t, transport)
		})
	}
}

func readsSurviveReplicaStop(t *testing.T, transport sirannon.TransportKind) {
	certificates := t.TempDir()
	primary := startServer(t, serverConfig{Mode: "primary", Certificates: certificates})
	replica := startServer(t, serverConfig{Mode: "replica", Certificates: certificates, PrimaryGRPCPort: primary.grpcPort})
	replicaRelay := startRelay(t, fmt.Sprintf("127.0.0.1:%d", replica.port))
	{
		ctx := testContext(t)
		client, err := sirannon.NewTopology(sirannon.TopologyOptions{
			Options:        sirannon.Options{Transport: transport, ReconnectInterval: 50 * time.Millisecond},
			Primary:        primary.url,
			Replicas:       []string{replicaRelay.url()},
			ReadPreference: sirannon.ReadFromReplica,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		db, err := client.Database(replicatedDatabase)
		if err != nil {
			t.Fatal(err)
		}
		const total = 400
		const stopAt = 150
		stopReplica := make(chan struct{})
		replicaStopped := make(chan struct{})
		var readsBeforeStop int64
		go func() {
			<-stopReplica
			readsBeforeStop = replicaRelay.accepted.Load()
			replica.stop()
			close(replicaStopped)
		}()
		var acknowledged []int64
		var writeErrors []error
		writesDone := make(chan struct{})
		go func() {
			defer close(writesDone)
			for id := int64(1); id <= total; id++ {
				_, err := db.Execute(ctx, insertItemSQL, []any{id, fmt.Sprintf("burst-%d", id), id << 40, []byte{byte(id), byte(id >> 8)}})
				if err != nil {
					writeErrors = append(writeErrors, fmt.Errorf("item %d: %w", id, err))
					continue
				}
				acknowledged = append(acknowledged, id)
				if id == stopAt {
					close(stopReplica)
				}
			}
		}()
		var readMutex sync.Mutex
		var reads int
		var readErrors []error
		readsDone := make(chan struct{})
		go func() {
			defer close(readsDone)
			for {
				select {
				case <-writesDone:
					return
				default:
				}
				var err error
				if reads%2 == 0 {
					_, err = db.Query(ctx, "SELECT COUNT(*) AS n FROM items", nil)
				} else {
					_, err = sirannon.Query(ctx, db, itemByID, itemArgs{ID: int64(1)})
				}
				readMutex.Lock()
				reads++
				if err != nil {
					readErrors = append(readErrors, err)
				}
				readMutex.Unlock()
			}
		}()
		<-writesDone
		<-readsDone
		<-replicaStopped
		if len(writeErrors) > 0 {
			t.Fatalf("%d writes failed while the replica stopped, first: %v", len(writeErrors), writeErrors[0])
		}
		if readsBeforeStop == 0 {
			t.Fatalf("no read reached the replica before it stopped")
		}
		readMutex.Lock()
		if len(readErrors) > 0 {
			t.Fatalf("%d of %d reads failed while the replica stopped, first: %v", len(readErrors), reads, readErrors[0])
		}
		readMutex.Unlock()
		rows, err := db.Query(ctx, "SELECT COUNT(*) AS n FROM items", nil)
		if err != nil || len(rows) != 1 || rows[0]["n"] != int64(total) {
			t.Fatalf("a read after the replica stopped gave %v, %v, want the primary's %d rows", rows, err, total)
		}
		path := primary.databaseFile(replicatedDatabase)
		if duplicates := oracleCount(t, path, "SELECT COUNT(*) FROM (SELECT id FROM items GROUP BY id HAVING COUNT(*) > 1)"); duplicates != 0 {
			t.Fatalf("the primary holds %d ids more than once", duplicates)
		}
		if count := oracleCount(t, path, "SELECT COUNT(*) FROM items"); count != int64(len(acknowledged)) {
			t.Fatalf("the primary holds %d rows for %d acknowledged writes", count, len(acknowledged))
		}
		for _, id := range acknowledged {
			if oracleCount(t, path, "SELECT COUNT(*) FROM items WHERE id = ? AND amount = ?", id, id<<40) != 1 {
				t.Fatalf("the primary lacks acknowledged item %d", id)
			}
		}
	}
}

func TestCoordinatorDiscoveryFailsWithRoutingErrorWhenNoNodeServesRouting(t *testing.T) {
	server := startServer(t, serverConfig{})
	ctx := testContext(t)
	client, err := sirannon.NewTopology(sirannon.TopologyOptions{Endpoints: []string{server.url}, Discovery: sirannon.DiscoveryCoordinator})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	db, _ := client.Database("coordinated")
	if _, err := db.Query(ctx, "SELECT 1", nil); sirannon.ErrorCode(err) != sirannon.CodeRoutingError {
		t.Fatalf("a read with no routing failed with %v, want ROUTING_ERROR", err)
	}
	if _, err := db.Execute(ctx, insertItemSQL, []any{1, "x", 1, nil}); sirannon.ErrorCode(err) != sirannon.CodeRoutingError {
		t.Fatalf("a write with no routing failed with %v, want ROUTING_ERROR", err)
	}
	if count := oracleCount(t, server.databaseFile("coordinated"), "SELECT COUNT(*) FROM sqlite_master"); count != 0 {
		t.Fatalf("a write with no routing created %d schema objects", count)
	}
}

func TestTopologyOptionsRejectUnusableSettings(t *testing.T) {
	cases := []sirannon.TopologyOptions{
		{},
		{Primary: "ftp://127.0.0.1:1"},
		{Primary: "http://user:secret@127.0.0.1:1"},
		{Primary: "http://127.0.0.1:1", ReadPreference: "fastest"},
		{Primary: "http://127.0.0.1:1", Discovery: "gossip"},
		{Primary: "http://127.0.0.1:1", ReadConcern: "eventual"},
		{Primary: "http://127.0.0.1:1", Options: sirannon.Options{WebSocketProtocols: []string{"bad token"}}},
	}
	for _, options := range cases {
		_, err := sirannon.NewTopology(options)
		if sirannon.ErrorCode(err) != sirannon.CodeInvalidArgument {
			t.Fatalf("options %+v gave %v, want INVALID_ARGUMENT", options, err)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Fatalf("the error %q carries the URL password", err)
		}
	}
}

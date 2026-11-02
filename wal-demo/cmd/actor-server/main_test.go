package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestListenerRequiresTokenOutsideLoopback(t *testing.T) {
	for _, tc := range []struct {
		addr, token string
		ok          bool
	}{
		{"127.0.0.1:8081", "", true}, {"[::1]:8081", "", true},
		{"0.0.0.0:8081", "", false}, {":8081", "", false},
		{"localhost:8081", "", false}, {"0.0.0.0:8081", "0123456789abcdef", true},
		{"127.0.0.1:8081", "short", false},
	} {
		if err := validateListener(tc.addr, tc.token); (err == nil) != tc.ok {
			t.Errorf("addr=%s ok=%v err=%v", tc.addr, tc.ok, err)
		}
	}
}

func TestInvalidConfigurationRefused(t *testing.T) {
	for _, name := range []string{"ACTOR_SHARDS", "ACTOR_QUEUE", "ACTOR_BATCH", "ACTOR_MAX_PLAYERS", "ACTOR_MAX_RECEIPTS", "ACTOR_BATCH_WINDOW", "ACTOR_FLUSH_INTERVAL", "ACTOR_MAX_UNCHECKPOINTED", "ACTOR_WAL_SEGMENT_BYTES"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "invalid")
			if _, err := configuration(); err == nil {
				t.Fatalf("accepted invalid %s", name)
			}
		})
	}
}

// The child executes the real server, not a fake in-memory storage adapter.
func TestActorProcessHelper(t *testing.T) {
	if os.Getenv("ACTOR_PROCESS_TEST") != "1" {
		t.Skip("subprocess helper")
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
}

func TestAcknowledgedAssetsSurviveSIGKILL(t *testing.T) {
	for _, reclaimed := range []bool{false, true} {
		t.Run(fmt.Sprintf("reclaimed=%v", reclaimed), func(t *testing.T) { testCrashState(t, reclaimed) })
	}
}

func testCrashState(t *testing.T, reclaimed bool) {
	uri := os.Getenv("MONGO_TEST_URI")
	if uri == "" {
		t.Skip("set MONGO_TEST_URI for subprocess crash test")
	}
	db := fmt.Sprintf("actor_process_test_%d", time.Now().UnixNano())
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := client.Database(db).Drop(ctx); err != nil {
			t.Error(err)
		}
		_ = client.Disconnect(ctx)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	start := func() *exec.Cmd {
		t.Helper()
		cmd := exec.Command(executable, "-test.run=^TestActorProcessHelper$")
		// Override inherited config, including any interactive server's token.
		values := map[string]string{"ACTOR_PROCESS_TEST": "1", "HTTP_ADDR": addr, "APP_API_TOKEN": "", "MONGO_URI": uri, "MONGO_DATABASE": db, "WAL_DIRECTORY": dir, "ACTOR_SHARDS": "2", "ACTOR_FLUSH_INTERVAL": "1h", "ACTOR_BATCH_WINDOW": "1ms", "ACTOR_MAX_UNCHECKPOINTED": "1000", "ACTOR_MAX_PLAYERS": "100", "ACTOR_MAX_RECEIPTS": "100", "ACTOR_QUEUE": "64", "ACTOR_BATCH": "16", "ACTOR_WAL_SEGMENT_BYTES": "1024"}
		if reclaimed {
			values["ACTOR_FLUSH_INTERVAL"] = "10ms"
		}
		for _, entry := range os.Environ() {
			name, _, _ := strings.Cut(entry, "=")
			if _, replaced := values[name]; !replaced {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		for name, value := range values {
			cmd.Env = append(cmd.Env, name+"="+value)
		}
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		ready := false
		for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
			c := &http.Client{Timeout: 200 * time.Millisecond}
			r, err := c.Get("http://" + addr + "/healthz")
			if err == nil {
				_ = r.Body.Close()
				ready = r.StatusCode == 200
			}
			if ready {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		if !ready {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("child server did not become ready")
		}
		return cmd
	}
	request := func(path string, body string, status int) map[string]any {
		t.Helper()
		method := "GET"
		if body != "" {
			method = "POST"
		}
		req, err := http.NewRequest(method, "http://"+addr+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		r, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		var result map[string]any
		if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if r.StatusCode != status {
			t.Fatalf("status=%d want=%d result=%v", r.StatusCode, status, result)
		}
		return result
	}
	child := start()
	alive := true
	defer func() {
		if alive {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	create := `{"player_id":"alice","request_id":"create"}`
	buy := `{"request_id":"buy","item":"sword"}`
	poor := `{"request_id":"poor","item":"sword"}`
	reward := `{"request_id":"reward","amount":50}`
	request("/players", create, 201)
	request("/players/alice/purchases", buy, 200)
	request("/players/alice/purchases", poor, 409)
	request("/players/alice/rewards", reward, 200)
	count, err := client.Database(db).Collection("actor_snapshots").CountDocuments(context.Background(), map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	expectedCoins := float64(70)
	if !reclaimed && count != 0 {
		t.Fatalf("expected WAL-only assets before kill: snapshots=%d", count)
	}
	if reclaimed {
		deadline := time.Now().Add(10 * time.Second)
		removed := false
		for time.Now().Before(deadline) {
			stats := request("/debug/stats", "", 200)
			for _, raw := range stats["shards"].([]any) {
				if raw.(map[string]any)["wal_reclaimed_segments"].(float64) > 0 {
					removed = true
				}
			}
			if removed {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !removed {
			t.Fatal("no actual segment deletion before kill")
		}
		request("/players/alice/rewards", `{"request_id":"tail","amount":7}`, 200)
		expectedCoins = 77
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	alive = false
	child = start()
	alive = true
	for _, tc := range []struct {
		path, body string
		status     int
	}{
		{"/players", create, 201}, {"/players/alice/purchases", buy, 200},
		{"/players/alice/purchases", poor, 409}, {"/players/alice/rewards", reward, 200},
	} {
		if r := request(tc.path, tc.body, tc.status); r["replayed"] != true {
			t.Fatalf("not replayed after SIGKILL: %v", r)
		}
	}
	p := request("/players/alice", "", 200)
	if p["coins"] != expectedCoins || p["inventory"].(map[string]any)["sword"] != float64(1) {
		t.Fatalf("assets changed across crash: %v", p)
	}
	if reclaimed {
		r := request("/players/alice/rewards", `{"request_id":"tail","amount":7}`, 200)
		if r["replayed"] != true {
			t.Fatal("tail duplicated after reclamation")
		}
	}
}

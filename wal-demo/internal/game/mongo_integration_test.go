package game

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/wal"
)

func integrationStore(t *testing.T) (*MongoStore, context.Context) {
	t.Helper()
	uri := os.Getenv("MONGO_TEST_URI")
	if uri == "" {
		t.Skip("set MONGO_TEST_URI to run real MongoDB integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	database := fmt.Sprintf("game_test_%d", time.Now().UnixNano())
	store, err := OpenMongo(ctx, uri, database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.client.Database(database).Drop(cleanup); err != nil {
			t.Errorf("drop test database: %v", err)
		}
		if err := store.Close(cleanup); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	return store, ctx
}

func TestMongoIdempotencyAndRejectedReceipts(t *testing.T) {
	store, ctx := integrationStore(t)
	apply := func(op Operation) Result {
		t.Helper()
		r, err := store.Apply(ctx, op)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	create := Operation{PlayerID: "alice", RequestID: "create-1", Kind: "create"}
	r := apply(create)
	if r.Status != 201 || r.Player.Coins != 100 {
		t.Fatalf("create: %+v", r)
	}
	r = apply(create)
	if !r.Replayed || r.Player.Coins != 100 {
		t.Fatalf("create replay: %+v", r)
	}
	if r = apply(Operation{PlayerID: "alice", RequestID: "create-1", Kind: "reward", Amount: 100}); r.Code != "idempotency_conflict" {
		t.Fatalf("conflict: %+v", r)
	}
	if r = apply(Operation{PlayerID: "alice", RequestID: "buy-1", Kind: "purchase", Item: "sword"}); r.Player.Coins != 20 || r.Player.Inventory["sword"] != 1 {
		t.Fatalf("purchase: %+v", r)
	}
	failed := Operation{PlayerID: "alice", RequestID: "buy-2", Kind: "purchase", Item: "sword"}
	if r = apply(failed); r.Code != "insufficient_coins" {
		t.Fatalf("expected rejection: %+v", r)
	}
	apply(Operation{PlayerID: "alice", RequestID: "reward-1", Kind: "reward", Amount: 100})
	r = apply(failed)
	if !r.Replayed || r.Code != "insufficient_coins" || r.Player.Coins != 20 {
		t.Fatalf("rejected receipt changed after reward: %+v", r)
	}
	p, err := store.Get(ctx, "alice")
	if err != nil || p.Coins != 120 || p.Inventory["sword"] != 1 {
		t.Fatalf("assets after replay: %+v err=%v", p, err)
	}
	if _, err := store.Get(ctx, "missing"); err != ErrNotFound {
		t.Fatalf("missing player: %v", err)
	}
}

func TestMongoConcurrentDuplicateRewardAppliesOnce(t *testing.T) {
	store, ctx := integrationStore(t)
	if _, err := store.Apply(ctx, Operation{PlayerID: "alice", RequestID: "create-1", Kind: "create"}); err != nil {
		t.Fatal(err)
	}
	op := Operation{PlayerID: "alice", RequestID: "same-reward", Kind: "reward", Amount: 17}
	const workers = 8
	results := make(chan Result, workers)
	errors := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r, err := store.Apply(ctx, op); results <- r; errors <- err }()
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	fresh := 0
	for r := range results {
		if !r.Replayed {
			fresh++
		}
		if r.Player.Coins != 117 {
			t.Fatalf("result: %+v", r)
		}
	}
	if fresh != 1 {
		t.Fatalf("fresh applications=%d; want 1", fresh)
	}
	p, err := store.Get(ctx, "alice")
	if err != nil || p.Coins != 117 {
		t.Fatalf("coins=%d err=%v", p.Coins, err)
	}
}

func TestMongoWALRecoveryBeforeAndAfterCommit(t *testing.T) {
	store, ctx := integrationStore(t)
	for _, precommitted := range []bool{false, true} {
		t.Run(fmt.Sprint("precommitted=", precommitted), func(t *testing.T) {
			id := fmt.Sprintf("alice-%v", precommitted)
			if _, err := store.Apply(ctx, Operation{PlayerID: id, RequestID: "create-1", Kind: "create"}); err != nil {
				t.Fatal(err)
			}
			op := Operation{PlayerID: id, RequestID: "reward-1", Kind: "reward", Amount: 23}
			path := filepath.Join(t.TempDir(), "assets.wal")
			log, err := wal.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(op)
			if _, err := log.Append(payload); err != nil {
				t.Fatal(err)
			}
			if precommitted {
				if _, err := store.Apply(ctx, op); err != nil {
					t.Fatal(err)
				}
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
			log, err = wal.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			engine, err := NewEngine(ctx, log, store)
			if err != nil {
				t.Fatal(err)
			}
			if len(log.Pending()) != 0 {
				t.Fatal("pending intent remains after recovery")
			}
			if err := engine.Healthy(ctx); err != nil {
				t.Fatal(err)
			}
			p, err := engine.Get(ctx, id)
			if err != nil || p.Coins != 123 {
				t.Fatalf("recovered balance=%d err=%v", p.Coins, err)
			}
			r, err := engine.Execute(ctx, op)
			if err != nil || !r.Replayed || r.Player.Coins != 123 {
				t.Fatalf("retry=%+v err=%v", r, err)
			}
		})
	}
}

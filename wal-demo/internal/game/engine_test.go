package game

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/wal"
)

// This store simulates a durable remote database, including a failed response
// after commit. Its state outlives the engine and local WAL handles in each test.
type recoveryStore struct {
	mu         sync.Mutex
	applied    map[string]Result
	coins      int64
	calls      int
	failBefore bool
	failAfter  bool
}

func (s *recoveryStore) Apply(_ context.Context, op Operation) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failBefore {
		return Result{}, errors.New("database unavailable")
	}
	if saved, ok := s.applied[op.RequestID]; ok {
		saved.Replayed = true
		return saved, nil
	}
	s.coins += op.Amount
	r := Result{Status: 200, Code: "ok", Player: &Player{ID: op.PlayerID, Coins: s.coins}}
	s.applied[op.RequestID] = r
	if s.failAfter {
		return Result{}, errors.New("connection lost after commit")
	}
	return r, nil
}

func (s *recoveryStore) Get(context.Context, string) (Player, error) {
	return Player{Coins: s.coins}, nil
}
func (s *recoveryStore) Ping(context.Context) error { return nil }

func TestEngineRecoversUncertainWriteWithoutDuplicatingAssets(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "before_database_commit"
		if committed {
			name = "after_database_commit"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "assets.wal")
			log, err := wal.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			store := &recoveryStore{applied: map[string]Result{}, failBefore: !committed, failAfter: committed}
			engine, err := NewEngine(ctx, log, store)
			if err != nil {
				t.Fatal(err)
			}
			op := Operation{PlayerID: "alice", RequestID: "reward-1", Kind: "reward", Amount: 7}
			if _, err := engine.Execute(ctx, op); err == nil {
				t.Fatal("expected uncertain write error")
			}
			if len(log.Pending()) != 1 {
				t.Fatalf("pending intents = %d; want 1", len(log.Pending()))
			}
			if err := engine.Healthy(ctx); err == nil {
				t.Fatal("faulted engine reports healthy")
			}
			if _, err := engine.Execute(ctx, Operation{PlayerID: "alice", RequestID: "reward-2", Kind: "reward", Amount: 20}); err == nil {
				t.Fatal("faulted engine accepted another write")
			}
			if store.calls != 1 {
				t.Fatalf("faulted engine contacted store again: %d calls", store.calls)
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}

			store.failBefore, store.failAfter = false, false
			log, err = wal.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			engine, err = NewEngine(ctx, log, store)
			if err != nil {
				t.Fatal(err)
			}
			if store.coins != 7 {
				t.Fatalf("recovered coins = %d; want 7", store.coins)
			}
			if len(log.Pending()) != 0 {
				t.Fatal("recovered intent still pending")
			}
			if err := engine.Healthy(ctx); err != nil {
				t.Fatal(err)
			}
			r, err := engine.Execute(ctx, op)
			if err != nil || !r.Replayed || store.coins != 7 {
				t.Fatalf("retry result=%+v coins=%d err=%v", r, store.coins, err)
			}
		})
	}
}

func TestEngineReplaysIntentWhoseDatabaseWriteAlreadyCompleted(t *testing.T) {
	ctx := context.Background()
	op := Operation{PlayerID: "alice", RequestID: "reward-1", Kind: "reward", Amount: 11}
	store := &recoveryStore{applied: map[string]Result{}}
	path := filepath.Join(t.TempDir(), "assets.wal")
	log, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(op)
	if _, err := log.Append(payload); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(ctx, op); err != nil {
		t.Fatal(err)
	}
	// Simulate a process exit before the WAL completion frame.
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	log, err = wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if _, err := NewEngine(ctx, log, store); err != nil {
		t.Fatal(err)
	}
	if store.coins != 11 || len(log.Pending()) != 0 {
		t.Fatalf("coins=%d pending=%d", store.coins, len(log.Pending()))
	}
}

func TestTransitionPreservesAssetsAndReceiptSnapshots(t *testing.T) {
	d := document{Player: Player{ID: "alice", Coins: 20, Inventory: map[string]int64{"potion": 1}}, Initialized: true}
	r := transition(&d, Operation{Kind: "purchase", Item: "sword"})
	if r.Status != 409 || r.Code != "insufficient_coins" || d.Coins != 20 || d.Inventory["sword"] != 0 {
		t.Fatalf("rejected purchase mutated assets: %+v, %+v", d, r)
	}
	transition(&d, Operation{Kind: "reward", Amount: 100})
	if r.Player.Coins != 20 || r.Player.Inventory["potion"] != 1 {
		t.Fatalf("saved snapshot changed: %+v", r.Player)
	}
	transition(&d, Operation{Kind: "purchase", Item: "potion"})
	if d.Coins != 90 || d.Inventory["potion"] != 2 {
		t.Fatalf("purchase result: %+v", d)
	}
	if r.Player.Inventory["potion"] != 1 {
		t.Fatal("receipt inventory aliases mutable state")
	}
	d.Coins = MaxCoins
	r = transition(&d, Operation{Kind: "reward", Amount: 1})
	if r.Code != "balance_limit" || d.Coins != MaxCoins {
		t.Fatalf("balance limit failed: %+v", d)
	}
}

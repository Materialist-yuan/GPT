package actor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/game"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// checkpointGate preserves real MongoDB reads/binding while injecting a failed
// checkpoint write. It never alters or stops the shared database service.
type checkpointGate struct {
	*MongoStore
	fail atomic.Bool
}

func (s *checkpointGate) Save(ctx context.Context, shard int, snapshots []Snapshot) error {
	if s.fail.Load() {
		return errors.New("injected checkpoint write failure")
	}
	return s.MongoStore.Save(ctx, shard, snapshots)
}

func gcTestOptions(dir string) Options {
	opts := DefaultOptions(dir)
	opts.Shards = 1
	opts.WALSegmentBytes = 2048
	opts.FlushInterval = 10 * time.Millisecond
	return opts
}

func waitForGCState(t *testing.T, ctx context.Context, engine *Engine, predicate func(ShardStats) bool) ShardStats {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		state := engine.Stats().Shards[0]
		if predicate(state) {
			return state
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for GC: %v; state=%+v", ctx.Err(), state)
		case <-deadline.C:
			t.Fatalf("GC state did not converge: %+v", state)
		case <-ticker.C:
		}
	}
}

func gcWriteRewards(t *testing.T, ctx context.Context, engine *Engine, count int) {
	t.Helper()
	if r, err := engine.Execute(ctx, createOp("alice")); err != nil || r.Status != 201 {
		t.Fatalf("create: %+v, %v", r, err)
	}
	for i := 0; i < count; i++ {
		r, err := engine.Execute(ctx, game.Operation{PlayerID: "alice", RequestID: fmt.Sprintf("reward-%d", i), Kind: "reward", Amount: 1})
		if err != nil || r.Status != 200 {
			t.Fatalf("reward %d: %+v, %v", i, r, err)
		}
	}
}

func TestMongoGCReclaimsSegmentsAndRestartsWithMonotonicSequence(t *testing.T) {
	store, ctx, _, _ := checkpointTestStore(t, 1)
	opts := gcTestOptions(t.TempDir())
	engine, err := New(ctx, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close(context.Background()) })
	gcWriteRewards(t, ctx, engine, 40)
	state := waitForGCState(t, ctx, engine, func(s ShardStats) bool { return s.WALReclaimedSegments > 0 && s.Pending == 0 })
	if state.WALReclaimedBytes == 0 {
		t.Fatalf("segment count changed without reclaiming bytes: %+v", state)
	}
	loaded, err := store.Load(ctx, 0)
	if err != nil || len(loaded) != 1 || loaded[0].Player.Coins != 140 {
		t.Fatalf("saved checkpoint: %+v, %v", loaded, err)
	}
	previousSeq, previousVersion := loaded[0].Seq, loaded[0].Player.Version
	if err := engine.Close(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err = New(ctx, store, opts)
	if err != nil {
		t.Fatalf("restart after reclamation: %v", err)
	}
	// The original receipt must survive reclamation, including its old snapshot.
	r, err := engine.Execute(ctx, game.Operation{PlayerID: "alice", RequestID: "reward-0", Kind: "reward", Amount: 1})
	if err != nil || !r.Replayed || r.Status != 200 || r.Player.Coins != 101 {
		t.Fatalf("receipt after reclamation: %+v, %v", r, err)
	}
	r, err = engine.Execute(ctx, game.Operation{PlayerID: "alice", RequestID: "after-restart", Kind: "reward", Amount: 7})
	if err != nil || r.Player.Coins != 147 || r.Player.Version != previousVersion+1 {
		t.Fatalf("new write after reclamation: %+v, %v", r, err)
	}
	waitForGCState(t, ctx, engine, func(s ShardStats) bool { return s.Pending == 0 })
	loaded, err = store.Load(ctx, 0)
	if err != nil || len(loaded) != 1 || loaded[0].Seq <= previousSeq || loaded[0].Player.Coins != 147 {
		t.Fatalf("sequence reset or assets changed: previous=%d current=%+v err=%v", previousSeq, loaded, err)
	}
}

func TestMongoFailedCheckpointDoesNotReclaimPendingSegments(t *testing.T) {
	store, ctx, _, _ := checkpointTestStore(t, 1)
	gate := &checkpointGate{MongoStore: store}
	gate.fail.Store(true)
	opts := gcTestOptions(t.TempDir())
	engine, err := New(ctx, gate, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		gate.fail.Store(false)
		_ = engine.Close(context.Background())
	})
	gcWriteRewards(t, ctx, engine, 40)
	state := waitForGCState(t, ctx, engine, func(s ShardStats) bool { return s.CheckpointErrors > 0 })
	if state.Pending != 41 || state.WALReclaimedSegments != 0 || state.WALReclaimedBytes != 0 {
		t.Fatalf("failed checkpoint reclaimed accepted events: %+v", state)
	}
	loaded, err := store.Load(ctx, 0)
	if err != nil || len(loaded) != 0 {
		t.Fatalf("injected write failure unexpectedly saved snapshots: %+v %v", loaded, err)
	}
	gate.fail.Store(false)
	state = waitForGCState(t, ctx, engine, func(s ShardStats) bool { return s.Pending == 0 && s.WALReclaimedSegments > 0 })
	if state.Fault != "" {
		t.Fatalf("checkpoint recovery faulted: %+v", state)
	}
	player, err := engine.Get(ctx, "alice")
	if err != nil || player.Coins != 140 {
		t.Fatalf("checkpoint retry changed assets: %+v %v", player, err)
	}
}

func TestMongoMissingSnapshotAfterGCRefusesRestart(t *testing.T) {
	store, ctx, _, _ := checkpointTestStore(t, 1)
	opts := gcTestOptions(t.TempDir())
	engine, err := New(ctx, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close(context.Background()) })
	gcWriteRewards(t, ctx, engine, 40)
	waitForGCState(t, ctx, engine, func(s ShardStats) bool { return s.Pending == 0 && s.WALReclaimedSegments > 0 })
	if err := engine.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Deliberately damage only this test database. Metadata remains, so the
	// retired per-player watermark must detect the missing checkpoint itself.
	deleted, err := store.snapshots.DeleteOne(ctx, bson.M{"_id": "alice"})
	if err != nil || deleted.DeletedCount != 1 {
		t.Fatalf("delete test snapshot: %+v, %v", deleted, err)
	}
	if restarted, err := New(ctx, store, opts); err == nil {
		_ = restarted.Close(ctx)
		t.Fatal("missing MongoDB snapshot was accepted after its WAL segments were reclaimed")
	}
}

func TestMongoStaleBackupAfterGCRefusesRestart(t *testing.T) {
	store, ctx, _, _ := checkpointTestStore(t, 1)
	opts := gcTestOptions(t.TempDir())
	engine, err := New(ctx, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close(context.Background()) })
	gcWriteRewards(t, ctx, engine, 40)
	waitForGCState(t, ctx, engine, func(s ShardStats) bool { return s.Pending == 0 && s.WALReclaimedSegments > 0 })
	if err := engine.Close(ctx); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx, 0)
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load original checkpoint: %+v, %v", loaded, err)
	}
	// Restore a structurally valid older checkpoint directly, as an out-of-band
	// database backup restore would do. Normal Save refuses this stale update.
	old := loaded[0]
	old.Player = game.Player{ID: "alice", Coins: 100, Version: 1, Inventory: map[string]int64{}}
	old.Seq = 1
	key := hash([]byte("create"))
	old.Receipts = map[string]Receipt{key: old.Receipts[key]}
	if _, err := store.snapshots.ReplaceOne(ctx, bson.M{"_id": "alice"}, checkpointDocument{
		ID: "alice", Shard: 0, Schema: snapshotSchema, Snapshot: old,
	}); err != nil {
		t.Fatal(err)
	}
	if restarted, err := New(ctx, store, opts); err == nil {
		_ = restarted.Close(ctx)
		t.Fatal("stale backup accepted after newer acknowledged WAL history was reclaimed")
	}
}

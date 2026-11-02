package actor

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/game"
)

func checkpointTestStore(t *testing.T, shards int) (*MongoStore, context.Context, string, string) {
	t.Helper()
	uri := os.Getenv("MONGO_TEST_URI")
	if uri == "" {
		t.Skip("set MONGO_TEST_URI for real MongoDB checkpoint tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	database := fmt.Sprintf("actor_test_%d", time.Now().UnixNano())
	store, err := OpenMongo(ctx, uri, database, shards)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.client.Database(database).Drop(cleanup); err != nil {
			t.Errorf("drop checkpoint database: %v", err)
		}
		if err := store.Close(cleanup); err != nil {
			t.Errorf("disconnect checkpoint client: %v", err)
		}
	})
	return store, ctx, uri, database
}

func checkpoint(id string, seq uint64, coins int64) Snapshot {
	p := game.Player{ID: id, Coins: coins, Inventory: map[string]int64{"sword": 1}, Version: int64(seq)}
	return Snapshot{
		Player: p, Initialized: true, Seq: seq,
		Receipts: map[string]Receipt{"request-hash": {Fingerprint: "fingerprint", Result: game.Result{Status: 200, Code: "ok", Player: &p}}},
	}
}

func TestMongoCheckpointRoundTripAndMonotonicWatermark(t *testing.T) {
	store, ctx, _, _ := checkpointTestStore(t, 2)
	newer := checkpoint("alice", 9, 190)
	if err := store.Save(ctx, 0, []Snapshot{newer, checkpoint("bob", 3, 30)}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, 1, []Snapshot{checkpoint("carol", 7, 70)}); err != nil {
		t.Fatal(err)
	}
	// A slow older flush must not roll back state, receipts or watermark.
	stale := checkpoint("alice", 8, 1)
	stale.Receipts = map[string]Receipt{}
	for _, value := range []Snapshot{stale, newer, stale} {
		if err := store.Save(ctx, 0, []Snapshot{value}); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := store.Load(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || !reflect.DeepEqual(loaded[0], newer) || loaded[1].Player.ID != "bob" {
		t.Fatalf("checkpoint roundtrip or stale-save guard failed: %+v", loaded)
	}
	other, err := store.Load(ctx, 1)
	if err != nil || len(other) != 1 || other[0].Player.ID != "carol" {
		t.Fatalf("shard isolation: %+v, %v", other, err)
	}
	// A caller with the wrong shard cannot steal a checkpoint via upsert.
	if err := store.Save(ctx, 1, []Snapshot{checkpoint("alice", 10, 999)}); err == nil {
		t.Fatal("wrong-shard overwrite succeeded")
	}
	loaded, err = store.Load(ctx, 0)
	if err != nil || !reflect.DeepEqual(loaded[0], newer) {
		t.Fatalf("wrong-shard attempt changed checkpoint: %+v, %v", loaded, err)
	}
}

func TestMongoCheckpointLayoutAndInputValidation(t *testing.T) {
	store, ctx, uri, database := checkpointTestStore(t, 2)
	compatible, err := OpenMongo(ctx, uri, database, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := compatible.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if incompatible, err := OpenMongo(ctx, uri, database, 3); err == nil {
		_ = incompatible.Close(ctx)
		t.Fatal("incompatible layout accepted")
	}
	for _, shard := range []int{-1, 2} {
		if _, err := store.Load(ctx, shard); err == nil {
			t.Fatalf("invalid Load shard %d accepted", shard)
		}
		if err := store.Save(ctx, shard, nil); err == nil {
			t.Fatalf("invalid Save shard %d accepted", shard)
		}
	}
	if err := store.Save(ctx, 0, []Snapshot{checkpoint("alice", 0, 100)}); err == nil {
		t.Fatal("zero WAL watermark accepted")
	}
	if err := store.Save(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
}

func TestMongoJournalBindingRejectsUnrelatedLocalHistory(t *testing.T) {
	store, ctx, uri, database := checkpointTestStore(t, 2)
	if err := store.BindJournal(ctx, "journal-uuid-a", true); err == nil {
		t.Fatal("local history was allowed to initialize a missing database binding")
	}
	for i := 0; i < 2; i++ {
		if err := store.BindJournal(ctx, "journal-uuid-a"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.BindJournal(ctx, "journal-uuid-b"); err == nil {
		t.Fatal("different journal identity accepted")
	}
	// A rejection must not modify the existing binding, even across clients.
	reopened, err := OpenMongo(ctx, uri, database, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(ctx)
	if err := reopened.BindJournal(ctx, "journal-uuid-a"); err != nil {
		t.Fatal(err)
	}
	if err := reopened.BindJournal(ctx, "journal-uuid-a", true); err != nil {
		t.Fatalf("matching existing history binding rejected: %v", err)
	}
	for _, invalid := range []string{"", string(make([]byte, 129))} {
		if err := store.BindJournal(ctx, invalid); err == nil {
			t.Fatal("invalid journal identity accepted")
		}
	}
}

func TestMongoEngineRequiresSameJournalLineageOnRestart(t *testing.T) {
	store, ctx, _, _ := checkpointTestStore(t, 2)
	opts := DefaultOptions(t.TempDir())
	opts.Shards = 2
	engine, err := New(ctx, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Execute(ctx, game.Operation{PlayerID: "alice", RequestID: "create", Kind: "create"}); err != nil {
		_ = engine.Close(ctx)
		t.Fatal(err)
	}
	if err := engine.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(ctx, store, opts)
	if err != nil {
		t.Fatalf("same journal restart rejected: %v", err)
	}
	if player, err := restarted.Get(ctx, "alice"); err != nil || player.Coins != 100 {
		t.Fatalf("same journal lost checkpoint: %+v, %v", player, err)
	}
	if err := restarted.Close(ctx); err != nil {
		t.Fatal(err)
	}
	opts.Directory = t.TempDir()
	if unrelated, err := New(ctx, store, opts); err == nil {
		_ = unrelated.Close(ctx)
		t.Fatal("unrelated local directory accepted with existing checkpoint database")
	}
}

func TestMongoEngineRefusesLostDatabaseWithCompletedLocalWAL(t *testing.T) {
	store, ctx, uri, database := checkpointTestStore(t, 2)
	opts := DefaultOptions(t.TempDir())
	opts.Shards = 2
	engine, err := New(ctx, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Execute(ctx, game.Operation{PlayerID: "alice", RequestID: "create", Kind: "create"}); err != nil {
		_ = engine.Close(ctx)
		t.Fatal(err)
	}
	// Close checkpoints the accepted command and completes its local WAL entry.
	if err := engine.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.client.Database(database).Drop(ctx); err != nil {
		t.Fatal(err)
	}
	// Recreating the metadata layout must not let an old WAL silently skip all
	// completed history while an empty database pretends to be a valid checkpoint.
	fresh, err := OpenMongo(ctx, uri, database, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close(ctx)
	if restarted, err := New(ctx, fresh, opts); err == nil {
		_ = restarted.Close(ctx)
		t.Fatal("lost checkpoint database accepted with nonempty local journal history")
	}
}

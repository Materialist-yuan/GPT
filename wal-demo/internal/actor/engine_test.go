package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/game"
	"github.com/Materialist-yuan/GPT/wal-demo/internal/wal"
)

type memoryStore struct {
	mu        sync.Mutex
	snapshots map[int]map[string]Snapshot
	blocked   <-chan struct{}
	entered   chan struct{}
	fail      bool
}

func newMemoryStore() *memoryStore { return &memoryStore{snapshots: map[int]map[string]Snapshot{}} }
func (s *memoryStore) Load(_ context.Context, shard int) ([]Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Snapshot
	for _, snap := range s.snapshots[shard] {
		out = append(out, cloneSnapshot(snap))
	}
	return out, nil
}
func (s *memoryStore) Save(ctx context.Context, shard int, snaps []Snapshot) error {
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
	}
	if s.blocked != nil {
		select {
		case <-s.blocked:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("mongo unavailable")
	}
	if s.snapshots[shard] == nil {
		s.snapshots[shard] = map[string]Snapshot{}
	}
	for _, snap := range snaps {
		if s.snapshots[shard][snap.Player.ID].Seq < snap.Seq {
			s.snapshots[shard][snap.Player.ID] = cloneSnapshot(snap)
		}
	}
	return nil
}
func (s *memoryStore) Ping(context.Context) error { return nil }
func testOptions(dir string) Options {
	o := DefaultOptions(dir)
	o.Shards = 1
	o.BatchWindow = time.Millisecond
	o.FlushInterval = time.Hour
	return o
}
func newTestEngine(t *testing.T, s Store, o Options) *Engine {
	t.Helper()
	e, err := New(context.Background(), s, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = e.Close(ctx)
	})
	return e
}
func runOp(t *testing.T, e *Engine, op game.Operation) game.Result {
	t.Helper()
	r, err := e.Execute(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func createOp(id string) game.Operation {
	return game.Operation{PlayerID: id, RequestID: "create", Kind: "create"}
}

func TestMongoCheckpointDoesNotBlockGameplay(t *testing.T) {
	s := newMemoryStore()
	release := make(chan struct{})
	s.blocked = release
	s.entered = make(chan struct{}, 1)
	o := testOptions(t.TempDir())
	o.FlushInterval = 5 * time.Millisecond
	e := newTestEngine(t, s, o)
	runOp(t, e, createOp("alice"))
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("checkpoint did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	r, err := e.Execute(ctx, game.Operation{PlayerID: "alice", RequestID: "reward", Kind: "reward", Amount: 7})
	if err != nil || r.Player.Coins != 107 {
		t.Fatalf("checkpoint blocked write: %+v %v", r, err)
	}
	p, err := e.Get(ctx, "alice")
	if err != nil || p.Coins != 107 {
		t.Fatalf("checkpoint blocked read: %+v %v", p, err)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snaps, _ := s.Load(context.Background(), 0)
		if len(snaps) > 0 && snaps[0].Player.Coins == 107 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("write made during in-flight checkpoint was lost")
}

func TestConcurrentDuplicateAndRejectedReceipt(t *testing.T) {
	e := newTestEngine(t, newMemoryStore(), testOptions(t.TempDir()))
	runOp(t, e, createOp("alice"))
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.Execute(context.Background(), game.Operation{PlayerID: "alice", RequestID: "reward", Kind: "reward", Amount: 7})
			if err == nil && r.Player.Coins != 107 {
				err = fmt.Errorf("wrong coins %d", r.Player.Coins)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	runOp(t, e, game.Operation{PlayerID: "alice", RequestID: "sword", Kind: "purchase", Item: "sword"})
	failed := game.Operation{PlayerID: "alice", RequestID: "shield", Kind: "purchase", Item: "shield"}
	if r := runOp(t, e, failed); r.Code != "insufficient_coins" {
		t.Fatal(r)
	}
	runOp(t, e, game.Operation{PlayerID: "alice", RequestID: "reward2", Kind: "reward", Amount: 100})
	if r := runOp(t, e, failed); !r.Replayed || r.Code != "insufficient_coins" {
		t.Fatal(r)
	}
	p, _ := e.Get(context.Background(), "alice")
	if p.Coins != 127 || p.Inventory["sword"] != 1 {
		t.Fatal(p)
	}
	st := e.Stats().Shards[0]
	if st.WALRecords != 5 || st.WALBatches >= st.WALRecords+1 {
		t.Fatal(st)
	}
}

func TestBacklogAdmissionAndRecovery(t *testing.T) {
	s := newMemoryStore()
	s.fail = true
	o := testOptions(t.TempDir())
	o.MaxUncheckpointed = 2
	e, err := New(context.Background(), s, o)
	if err != nil {
		t.Fatal(err)
	}
	runOp(t, e, createOp("alice"))
	op := game.Operation{PlayerID: "alice", RequestID: "reward", Kind: "reward", Amount: 7}
	runOp(t, e, op)
	if _, err := e.Execute(context.Background(), game.Operation{PlayerID: "alice", RequestID: "next", Kind: "reward", Amount: 9}); !errors.Is(err, ErrOverloaded) {
		t.Fatal(err)
	}
	if r := runOp(t, e, op); !r.Replayed {
		t.Fatal("duplicate must remain readable at backlog cap")
	}
	if e.Healthy(context.Background()) == nil {
		t.Fatal("full backlog should fail readiness")
	}
	if err := e.Close(context.Background()); err == nil {
		t.Fatal("expected failed final checkpoint")
	}
	s.fail = false
	e = newTestEngine(t, s, o)
	p, err := e.Get(context.Background(), "alice")
	if err != nil || p.Coins != 107 {
		t.Fatalf("recovery: %+v %v", p, err)
	}
	if r := runOp(t, e, op); !r.Replayed {
		t.Fatal("replayed receipt lost")
	}
}

func TestRecoveryUsesRecordedStateAndCheckpointWatermark(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		t.Run(fmt.Sprint(checkpoint), func(t *testing.T) {
			dir := t.TempDir()
			o := testOptions(dir)
			if err := manifest(o); err != nil {
				t.Fatal(err)
			}
			log, err := wal.Open(filepath.Join(dir, "shard-000.wal"))
			if err != nil {
				t.Fatal(err)
			}
			op := game.Operation{PlayerID: "alice", RequestID: "historical", Kind: "create"}
			encoded, _ := json.Marshal(op)
			key := hash([]byte(op.RequestID))
			// This historical result intentionally differs from today's 100-coin create rule.
			player := game.Player{ID: "alice", Coins: 777, Version: 1, Inventory: map[string]int64{"sword": 2}}
			receipt := Receipt{Fingerprint: hash(encoded), Result: game.Result{Status: 201, Code: "ok", Player: &player}}
			payload, _ := json.Marshal(event{Schema: 1, Player: player, Initialized: true, Key: key, Receipt: receipt})
			id, err := log.Append(payload)
			if err != nil {
				t.Fatal(err)
			}
			log.Close()
			s := newMemoryStore()
			if checkpoint {
				s.snapshots[0] = map[string]Snapshot{"alice": {Player: player, Initialized: true, Receipts: map[string]Receipt{key: receipt}, Seq: id}}
			}
			e := newTestEngine(t, s, o)
			p, err := e.Get(context.Background(), "alice")
			if err != nil || p.Coins != 777 {
				t.Fatalf("recovery repriced event %+v %v", p, err)
			}
			if r := runOp(t, e, op); !r.Replayed || r.Player.Coins != 777 || r.Status != 201 {
				t.Fatal(r)
			}
			want := uint64(1)
			if checkpoint {
				want = 0
			}
			if e.Stats().Shards[0].Pending != want {
				t.Fatal(e.Stats())
			}
		})
	}
}

func TestLayoutOwnershipCapacityAndMissingLog(t *testing.T) {
	s := newMemoryStore()
	o := testOptions(t.TempDir())
	o.MaxReceiptsPerPlayer = 1
	e := newTestEngine(t, s, o)
	if other, err := New(context.Background(), s, o); err == nil {
		other.Close(context.Background())
		t.Fatal("concurrent directory owner allowed")
	}
	runOp(t, e, createOp("alice"))
	if r := runOp(t, e, game.Operation{PlayerID: "alice", RequestID: "reward", Kind: "reward", Amount: 7}); r.Status != 429 {
		t.Fatal(r)
	}
	if r := runOp(t, e, createOp("alice")); !r.Replayed {
		t.Fatal("capacity broke idempotency")
	}
	e.Close(context.Background())
	o.Shards = 2
	if other, err := New(context.Background(), s, o); err == nil {
		other.Close(context.Background())
		t.Fatal("layout changed silently")
	}
	s2 := newMemoryStore()
	s2.snapshots[0] = map[string]Snapshot{"alice": {Player: game.Player{ID: "alice"}, Seq: 50}}
	if other, err := New(context.Background(), s2, testOptions(t.TempDir())); err == nil {
		other.Close(context.Background())
		t.Fatal("missing WAL accepted over saved checkpoint")
	}
}

func TestCloseRacingAdmissionDoesNotHang(t *testing.T) {
	e := newTestEngine(t, newMemoryStore(), testOptions(t.TempDir()))
	runOp(t, e, createOp("alice"))
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _ = e.Execute(ctx, game.Operation{PlayerID: "alice", RequestID: fmt.Sprint(i), Kind: "reward", Amount: 1})
		}(i)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if _, err := e.Get(context.Background(), "alice"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestWALOnlyRecoveryRetainsAcceptedAndRejectedHTTPStatuses(t *testing.T) {
	store := newMemoryStore()
	store.fail = true
	o := testOptions(t.TempDir())
	engine, err := New(context.Background(), store, o)
	if err != nil {
		t.Fatal(err)
	}
	create := createOp("alice")
	reward := game.Operation{PlayerID: "alice", RequestID: "reward", Kind: "reward", Amount: 7}
	buy := game.Operation{PlayerID: "alice", RequestID: "buy", Kind: "purchase", Item: "sword"}
	reject := game.Operation{PlayerID: "alice", RequestID: "reject", Kind: "purchase", Item: "sword"}
	runOp(t, engine, create)
	runOp(t, engine, reward)
	runOp(t, engine, buy)
	if result := runOp(t, engine, reject); result.Status != 409 {
		t.Fatal(result)
	}
	if err := engine.Close(context.Background()); err == nil {
		t.Fatal("expected checkpoint error")
	}
	store.fail = false
	engine = newTestEngine(t, store, o)
	for _, tc := range []struct {
		op     game.Operation
		status int
		code   string
	}{{create, 201, "ok"}, {reward, 200, "ok"}, {buy, 200, "ok"}, {reject, 409, "insufficient_coins"}} {
		result := runOp(t, engine, tc.op)
		if !result.Replayed || result.Status != tc.status || result.Code != tc.code {
			t.Fatalf("lost WAL receipt: %+v", result)
		}
	}
}

func TestFullMailboxRejectsWithoutGrowingQueue(t *testing.T) {
	o := testOptions(t.TempDir())
	o.QueueCapacity = 1
	engine := newTestEngine(t, newMemoryStore(), o)
	// Hold the actor at a snapshot handoff to make queue saturation deterministic.
	capture := make(chan []Snapshot)
	engine.shards[0].control <- control{capture: capture}
	finished := make(chan error, 1)
	go func() { _, err := engine.Execute(context.Background(), createOp("alice")); finished <- err }()
	deadline := time.Now().Add(time.Second)
	for len(engine.shards[0].queue) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(engine.shards[0].queue) != 1 {
		<-capture
		t.Fatal("first command not queued")
	}
	_, err := engine.Execute(context.Background(), createOp("bob"))
	if !errors.Is(err, ErrOverloaded) {
		<-capture
		t.Fatalf("full queue error: %v", err)
	}
	if len(engine.shards[0].queue) != 1 {
		<-capture
		t.Fatal("queue grew past capacity")
	}
	<-capture
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryRefusesMissingCompletedPlayerSnapshot(t *testing.T) {
	store := newMemoryStore()
	opts := testOptions(t.TempDir())
	engine, err := New(context.Background(), store, opts)
	if err != nil {
		t.Fatal(err)
	}
	runOp(t, engine, createOp("alice"))
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Simulate deleting only the snapshot collection while retaining completed WAL.
	store.mu.Lock()
	delete(store.snapshots[0], "alice")
	store.mu.Unlock()
	if restarted, err := New(context.Background(), store, opts); err == nil {
		restarted.Close(context.Background())
		t.Fatal("lost completed player snapshot accepted silently")
	}
}

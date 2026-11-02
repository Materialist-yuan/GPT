package actor

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/game"
	"github.com/Materialist-yuan/GPT/wal-demo/internal/wal"
)

type event struct {
	Schema      int         `json:"schema"`
	Player      game.Player `json:"player"`
	Initialized bool        `json:"initialized"`
	Key         string      `json:"key"`
	Receipt     Receipt     `json:"receipt"`
}
type reply struct {
	result game.Result
	player game.Player
	err    error
}
type command struct {
	op    game.Operation
	get   bool
	reply chan reply
}
type control struct {
	capture chan []Snapshot
	saved   []Snapshot
	done    chan error
	stop    bool
}
type shard struct {
	id         int
	engine     *Engine
	log        journal
	queue      chan command
	control    chan control
	states     map[string]Snapshot
	checkpoint map[string]uint64
	pending    map[uint64]string
	fault      error
	statsMu    sync.Mutex
	stats      ShardStats
}
type Engine struct {
	admission            sync.RWMutex
	opts                 Options
	store                Store
	shards               []*shard
	lock                 *os.File
	closed               atomic.Bool
	accepted, overloaded atomic.Uint64
	flushCancel          context.CancelFunc
	flushWG              sync.WaitGroup
	closeOnce            sync.Once
	closeDone            chan struct{}
	closeErr             error
}

// journal permits isolated test benchmarks; production always opens a durable WAL.
type journal interface {
	AppendBatch([]json.RawMessage) ([]uint64, error)
	CompleteBatch([]uint64) error
	LastID() uint64
	Pending() []wal.Entry
	VisitCompleted(func(wal.Entry) error) error
	Close() error
}

func New(ctx context.Context, store Store, opts Options) (*Engine, error) {
	return newEngine(ctx, store, opts, func(path string) (journal, error) { return wal.Open(path) })
}

func newEngine(ctx context.Context, store Store, opts Options, open func(string) (journal, error)) (*Engine, error) {
	if store == nil || opts.Directory == "" || opts.Shards < 1 || opts.Shards > 256 || opts.QueueCapacity < 1 || opts.BatchSize < 1 || opts.BatchSize > 1024 || opts.BatchWindow < 0 || opts.FlushInterval <= 0 || opts.MaxPlayersPerShard < 1 || opts.MaxReceiptsPerPlayer < 1 || opts.MaxReceiptsPerPlayer > 2000 || opts.MaxUncheckpointed < 1 {
		return nil, errors.New("invalid actor options")
	}
	if err := durableDirectory(opts.Directory); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(opts.Directory, "owner.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("actor directory already owned: %w", err)
	}
	e := &Engine{opts: opts, store: store, lock: lock, closeDone: make(chan struct{})}
	failed := true
	defer func() {
		if failed {
			for _, s := range e.shards {
				s.log.Close()
			}
			lock.Close()
		}
	}()
	journalID, err := journalManifest(opts)
	if err != nil {
		return nil, err
	}
	if binder, ok := store.(interface {
		BindJournal(context.Context, string, ...bool) error
	}); ok {
		history := false
		for id := 0; id < opts.Shards; id++ {
			stat, err := os.Stat(filepath.Join(opts.Directory, fmt.Sprintf("shard-%03d.wal", id)))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			if err == nil && stat.Size() > 0 {
				history = true
			}
		}
		if err := binder.BindJournal(ctx, journalID, history); err != nil {
			return nil, fmt.Errorf("bind journal: %w", err)
		}
	}
	for id := 0; id < opts.Shards; id++ {
		journal, err := open(filepath.Join(opts.Directory, fmt.Sprintf("shard-%03d.wal", id)))
		if err != nil {
			return nil, err
		}
		s := &shard{id: id, engine: e, log: journal, queue: make(chan command, opts.QueueCapacity), control: make(chan control), states: map[string]Snapshot{}, checkpoint: map[string]uint64{}, pending: map[uint64]string{}, stats: ShardStats{ID: id}}
		e.shards = append(e.shards, s)
		snapshots, err := store.Load(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("load shard %d: %w", id, err)
		}
		for _, snap := range snapshots {
			if snap.Player.ID == "" || shardID(snap.Player.ID, opts.Shards) != id || snap.Seq > journal.LastID() {
				return nil, fmt.Errorf("snapshot/log ownership or watermark mismatch for player %q", snap.Player.ID)
			}
			if snap.Receipts == nil {
				snap.Receipts = map[string]Receipt{}
			}
			s.states[snap.Player.ID] = cloneSnapshot(snap)
			s.checkpoint[snap.Player.ID] = snap.Seq
		}

		// A completion is evidence that MongoDB confirmed the corresponding
		// snapshot. Losing that snapshot must not silently reset player state.
		if err := journal.VisitCompleted(func(entry wal.Entry) error {
			var ev event
			if err := json.Unmarshal(entry.Payload, &ev); err != nil {
				return fmt.Errorf("decode completed actor event: %w", err)
			}
			if ev.Schema != 1 || ev.Player.ID == "" || shardID(ev.Player.ID, opts.Shards) != id {
				return errors.New("invalid completed actor event")
			}
			if s.checkpoint[ev.Player.ID] < entry.ID {
				return fmt.Errorf("missing or stale snapshot for completed player %q at sequence %d", ev.Player.ID, entry.ID)
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("validate shard %d checkpoint history: %w", id, err)
		}
		var completed []uint64
		for _, entry := range journal.Pending() {
			var ev event
			if err := json.Unmarshal(entry.Payload, &ev); err != nil {
				return nil, fmt.Errorf("decode shard %d WAL: %w", id, err)
			}
			if ev.Schema != 1 || ev.Player.ID == "" || shardID(ev.Player.ID, opts.Shards) != id || ev.Key == "" || ev.Receipt.Fingerprint == "" {
				return nil, fmt.Errorf("unsupported/invalid actor event in shard %d", id)
			}
			if entry.ID <= s.checkpoint[ev.Player.ID] {
				completed = append(completed, entry.ID)
				continue
			}
			snap := s.states[ev.Player.ID]
			if entry.ID <= snap.Seq {
				return nil, errors.New("nonmonotonic player WAL")
			}
			if snap.Receipts == nil {
				snap.Receipts = map[string]Receipt{}
			}
			snap.Player = clonePlayer(ev.Player)
			snap.Initialized = ev.Initialized
			snap.Seq = entry.ID
			snap.Receipts[ev.Key] = cloneReceipt(ev.Receipt)
			s.states[ev.Player.ID] = snap
			s.pending[entry.ID] = ev.Player.ID
		}
		if err := journal.CompleteBatch(completed); err != nil {
			return nil, err
		}
		if len(s.states) > opts.MaxPlayersPerShard {
			return nil, errors.New("restored players exceed configured capacity")
		}
		for _, snap := range s.states {
			if len(snap.Receipts) > opts.MaxReceiptsPerPlayer {
				return nil, errors.New("restored receipts exceed configured capacity")
			}
		}
		s.publish()
	}
	flushCtx, cancel := context.WithCancel(context.Background())
	e.flushCancel = cancel
	for _, s := range e.shards {
		go s.run()
		e.flushWG.Add(1)
		go s.flushLoop(flushCtx)
	}
	failed = false
	return e, nil
}

func manifest(opts Options) error { _, err := journalManifest(opts); return err }
func journalManifest(opts Options) (string, error) {
	path := filepath.Join(opts.Directory, "layout.json")
	type layout struct {
		Schema    int    `json:"schema"`
		Shards    int    `json:"shards"`
		JournalID string `json:"journal_id"`
	}
	b, err := os.ReadFile(path)
	if err == nil {
		var found layout
		if json.Unmarshal(b, &found) != nil || found.Schema != 2 || found.Shards != opts.Shards || len(found.JournalID) != 32 {
			return "", errors.New("actor journal layout changed or lacks identity; explicit migration required")
		}
		decoded, err := hex.DecodeString(found.JournalID)
		if err != nil || len(decoded) != 16 {
			return "", errors.New("invalid journal identity")
		}
		return found.JournalID, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	desired := layout{Schema: 2, Shards: opts.Shards, JournalID: hex.EncodeToString(random[:])}
	b, _ = json.Marshal(desired)
	f, err := os.CreateTemp(opts.Directory, "layout-*.tmp")
	if err != nil {
		return "", err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err = f.Chmod(0600); err == nil {
		var n int
		n, err = f.Write(b)
		if err == nil && n != len(b) {
			err = errors.New("short manifest write")
		}
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return "", err
	}
	if closeErr != nil {
		return "", closeErr
	}
	if err = os.Rename(temp, path); err != nil {
		return "", err
	}
	d, err := os.Open(opts.Directory)
	if err != nil {
		return "", err
	}
	err = errors.Join(d.Sync(), d.Close())
	if err != nil {
		return "", err
	}
	return desired.JournalID, nil
}

func shardID(id string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(n))
}
func (e *Engine) submit(ctx context.Context, c command) (reply, error) {
	if err := ctx.Err(); err != nil {
		return reply{}, err
	}
	e.admission.RLock()
	if e.closed.Load() {
		e.admission.RUnlock()
		return reply{}, ErrClosed
	}
	s := e.shards[shardID(c.op.PlayerID, len(e.shards))]
	// Admission and shutdown are coordinated by shard.run draining all buffered work.
	select {
	case s.queue <- c:
		e.admission.RUnlock()
		e.accepted.Add(1)
	default:
		e.admission.RUnlock()
		e.overloaded.Add(1)
		return reply{}, ErrOverloaded
	}
	select {
	case r := <-c.reply:
		return r, r.err
	case <-ctx.Done():
		return reply{}, ctx.Err()
	case <-e.closeDone:
		return reply{}, ErrClosed
	}
}
func (e *Engine) Execute(ctx context.Context, op game.Operation) (game.Result, error) {
	if err := game.Validate(op); err != nil {
		return game.Result{}, err
	}
	r, err := e.submit(ctx, command{op: op, reply: make(chan reply, 1)})
	return r.result, err
}
func (e *Engine) Get(ctx context.Context, id string) (game.Player, error) {
	r, err := e.submit(ctx, command{op: game.Operation{PlayerID: id}, get: true, reply: make(chan reply, 1)})
	return r.player, err
}
func (e *Engine) Healthy(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.closed.Load() {
		return ErrClosed
	}
	for _, s := range e.Stats().Shards {
		if s.Fault != "" {
			return errors.New(s.Fault)
		}
		if s.Pending >= e.opts.MaxUncheckpointed {
			return ErrOverloaded
		}
	}
	return nil
}
func (e *Engine) Stats() Stats {
	out := Stats{Accepted: e.accepted.Load(), Overloaded: e.overloaded.Load()}
	for _, s := range e.shards {
		s.statsMu.Lock()
		copy := s.stats
		s.statsMu.Unlock()
		copy.Queue = len(s.queue)
		if copy.PendingSinceUnixMilli > 0 {
			copy.PendingAgeMillis = time.Now().UnixMilli() - copy.PendingSinceUnixMilli
		}
		out.Shards = append(out.Shards, copy)
	}
	return out
}
func (s *shard) publish() {
	s.statsMu.Lock()
	s.stats.Players = int64(len(s.states))
	s.stats.Pending = uint64(len(s.pending))
	if len(s.pending) == 0 {
		s.stats.PendingSinceUnixMilli = 0
	} else if s.stats.PendingSinceUnixMilli == 0 {
		s.stats.PendingSinceUnixMilli = time.Now().UnixMilli()
	}
	if s.fault != nil {
		s.stats.Fault = s.fault.Error()
	}
	s.statsMu.Unlock()
}

func (s *shard) run() {
	for {
		select {
		case c := <-s.control:
			if c.capture != nil {
				c.capture <- s.dirty()
			}
			if c.saved != nil {
				err := s.ack(c.saved)
				if c.done != nil {
					c.done <- err
				}
			}
			if c.stop {
				for {
					select {
					case cmd := <-s.queue:
						s.batch([]command{cmd})
					default:
						c.capture <- s.dirty()
						return
					}
				}
			}
		case first := <-s.queue:
			batch := []command{first}
			timer := time.NewTimer(s.engine.opts.BatchWindow)
		collect:
			for len(batch) < s.engine.opts.BatchSize {
				select {
				case cmd := <-s.queue:
					batch = append(batch, cmd)
				case <-timer.C:
					break collect
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			s.batch(batch)
		}
	}
}
func (s *shard) batch(commands []command) {
	working := map[string]Snapshot{}
	answers := make([]reply, len(commands))
	var events []json.RawMessage
	var eventPlayers []string
	next := s.log.LastID()
	for i, c := range commands {
		snap, ok := working[c.op.PlayerID]
		if !ok {
			snap = cloneSnapshot(s.states[c.op.PlayerID])
			if snap.Player.ID == "" {
				snap.Player = game.Player{ID: c.op.PlayerID, Inventory: map[string]int64{}}
				snap.Receipts = map[string]Receipt{}
			}
		}
		if c.get {
			if !snap.Initialized {
				answers[i].err = game.ErrNotFound
			} else {
				answers[i].player = clonePlayer(snap.Player)
			}
			continue
		}
		if s.fault != nil {
			answers[i].err = s.fault
			continue
		}
		key := hash([]byte(c.op.RequestID))
		encoded, _ := json.Marshal(c.op)
		fingerprint := hash(encoded)
		if receipt, ok := snap.Receipts[key]; ok {
			if receipt.Fingerprint != fingerprint {
				answers[i].result = game.Result{Status: 409, Code: "idempotency_conflict"}
			} else {
				answers[i].result = cloneResult(receipt.Result)
				answers[i].result.Replayed = true
			}
			continue
		}
		if uint64(len(s.pending)+len(events)) >= s.engine.opts.MaxUncheckpointed {
			answers[i].err = ErrOverloaded
			s.engine.overloaded.Add(1)
			continue
		}
		if _, known := s.states[c.op.PlayerID]; !known {
			if _, known = working[c.op.PlayerID]; !known && len(s.states)+newPlayers(working, s.states) >= s.engine.opts.MaxPlayersPerShard {
				answers[i].result = game.Result{Status: 429, Code: "player_capacity"}
				continue
			}
		}
		if len(snap.Receipts) >= s.engine.opts.MaxReceiptsPerPlayer {
			answers[i].result = game.Result{Status: 429, Code: "receipt_capacity"}
			continue
		}
		result := decide(&snap, c.op)
		receipt := Receipt{Fingerprint: fingerprint, Result: cloneResult(result)}
		snap.Receipts[key] = receipt
		next++
		snap.Seq = next
		ev := event{Schema: 1, Player: clonePlayer(snap.Player), Initialized: snap.Initialized, Key: key, Receipt: receipt}
		payload, _ := json.Marshal(ev)
		events = append(events, payload)
		eventPlayers = append(eventPlayers, c.op.PlayerID)
		working[c.op.PlayerID] = snap
		answers[i].result = result
	}
	if len(events) > 0 {
		ids, err := s.log.AppendBatch(events)
		if err != nil {
			s.fault = fmt.Errorf("WAL uncertain; restart required: %w", err)
			for i := range answers {
				answers[i] = reply{err: s.fault}
			}
		} else {
			for id, snap := range working {
				s.states[id] = snap
			}
			for i, id := range ids {
				s.pending[id] = eventPlayers[i]
			}
			s.statsMu.Lock()
			s.stats.WALRecords += uint64(len(ids))
			s.stats.WALBatches++
			s.statsMu.Unlock()
		}
	}
	s.publish()
	for i, c := range commands {
		c.reply <- answers[i]
	}
}
func newPlayers(working, existing map[string]Snapshot) int {
	n := 0
	for id := range working {
		if _, ok := existing[id]; !ok {
			n++
		}
	}
	return n
}
func (s *shard) dirty() []Snapshot {
	var out []Snapshot
	for id, snap := range s.states {
		if snap.Seq > s.checkpoint[id] {
			out = append(out, cloneSnapshot(snap))
		}
	}
	return out
}
func (s *shard) ack(saved []Snapshot) error {
	high := map[string]uint64{}
	for _, snap := range saved {
		if snap.Seq > s.checkpoint[snap.Player.ID] {
			high[snap.Player.ID] = snap.Seq
		}
	}
	var ids []uint64
	for id, player := range s.pending {
		if id <= high[player] {
			ids = append(ids, id)
		}
	}
	if err := s.log.CompleteBatch(ids); err != nil {
		s.fault = fmt.Errorf("checkpoint completion uncertain; restart required: %w", err)
		s.publish()
		return err
	}
	for player, seq := range high {
		s.checkpoint[player] = seq
	}
	for _, id := range ids {
		delete(s.pending, id)
	}
	s.publish()
	return nil
}
func (s *shard) flushLoop(ctx context.Context) {
	defer s.engine.flushWG.Done()
	ticker := time.NewTicker(s.engine.opts.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		response := make(chan []Snapshot, 1)
		select {
		case s.control <- control{capture: response}:
		case <-ctx.Done():
			return
		}
		var snapshots []Snapshot
		select {
		case snapshots = <-response:
		case <-ctx.Done():
			return
		}
		if len(snapshots) == 0 {
			continue
		}
		saveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.engine.store.Save(saveCtx, s.id, snapshots)
		cancel()
		s.statsMu.Lock()
		if err != nil {
			s.stats.CheckpointErrors++
			s.stats.LastCheckpointError = "checkpoint save failed"
		} else {
			s.stats.Checkpoints++
			s.stats.LastCheckpointUnixMilli = time.Now().UnixMilli()
			s.stats.LastCheckpointError = ""
		}
		s.statsMu.Unlock()
		if err != nil {
			continue
		}
		done := make(chan error, 1)
		select {
		case s.control <- control{saved: snapshots, done: done}:
		case <-ctx.Done():
			return
		}
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
}
func (e *Engine) Close(ctx context.Context) error {
	e.closeOnce.Do(func() {
		e.admission.Lock()
		e.closed.Store(true)
		e.admission.Unlock()
		go e.shutdown()
	})
	select {
	case <-e.closeDone:
		return e.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (e *Engine) shutdown() {
	defer close(e.closeDone)
	e.flushCancel()
	e.flushWG.Wait()
	for _, s := range e.shards {
		captured := make(chan []Snapshot, 2)
		s.control <- control{capture: captured, stop: true}
		<-captured
		snapshots := <-captured
		if len(snapshots) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := e.store.Save(ctx, s.id, snapshots)
			cancel()
			if err == nil {
				err = s.ack(snapshots)
			}
			e.closeErr = errors.Join(e.closeErr, err)
		}
		e.closeErr = errors.Join(e.closeErr, s.log.Close())
	}
	e.closeErr = errors.Join(e.closeErr, e.lock.Close())
}
func hash(data []byte) string               { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func clonePlayer(p game.Player) game.Player { p.Inventory = cloneInventory(p.Inventory); return p }
func cloneInventory(in map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
func cloneResult(r game.Result) game.Result {
	if r.Player != nil {
		p := clonePlayer(*r.Player)
		r.Player = &p
	}
	return r
}
func cloneReceipt(r Receipt) Receipt { r.Result = cloneResult(r.Result); return r }
func cloneSnapshot(s Snapshot) Snapshot {
	s.Player = clonePlayer(s.Player)
	receipts := map[string]Receipt{}
	for k, v := range s.Receipts {
		receipts[k] = cloneReceipt(v)
	}
	s.Receipts = receipts
	return s
}
func decide(s *Snapshot, op game.Operation) game.Result {
	s.Player.Version++
	r := game.Result{Status: 200, Code: "ok"}
	switch {
	case op.Kind == "create" && s.Initialized:
		r.Status, r.Code = 409, "player_exists"
	case op.Kind == "create":
		s.Initialized = true
		s.Player.Coins = 100
		s.Player.Inventory = map[string]int64{}
		r.Status = 201
	case !s.Initialized:
		r.Status, r.Code = 404, "player_not_found"
	case op.Kind == "reward" && s.Player.Coins > game.MaxCoins-op.Amount:
		r.Status, r.Code = 409, "balance_limit"
	case op.Kind == "reward":
		s.Player.Coins += op.Amount
	case op.Kind == "purchase" && s.Player.Coins < game.Catalog()[op.Item]:
		r.Status, r.Code = 409, "insufficient_coins"
	case op.Kind == "purchase":
		s.Player.Coins -= game.Catalog()[op.Item]
		s.Player.Inventory[op.Item]++
	}
	if s.Initialized {
		p := clonePlayer(s.Player)
		r.Player = &p
	}
	return r
}

// A new directory's name lives in its parent; fsyncing only the WAL or leaf
// directory does not persist a freshly created path after power loss.
func durableDirectory(path string) error {
	var missing []string
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		_, err := os.Stat(current)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		if filepath.Dir(current) == current {
			return err
		}
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	if len(missing) > 0 {
		missing = append(missing, filepath.Dir(missing[len(missing)-1]))
	}
	for _, name := range missing {
		f, err := os.Open(name)
		if err != nil {
			return err
		}
		err = errors.Join(f.Sync(), f.Close())
		if err != nil {
			return err
		}
	}
	return nil
}

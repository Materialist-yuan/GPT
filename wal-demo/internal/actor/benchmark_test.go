package actor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/game"
	"github.com/Materialist-yuan/GPT/wal-demo/internal/wal"
)

// Volatile journal exists ONLY in the test binary. It preserves sequence
// bookkeeping but intentionally does no framing, checksum, I/O or durability.
type volatileJournal struct{ seq uint64 }

func (v *volatileJournal) AppendBatch(p []json.RawMessage) ([]uint64, error) {
	ids := make([]uint64, len(p))
	for i := range p {
		v.seq++
		ids[i] = v.seq
	}
	return ids, nil
}
func (*volatileJournal) CompleteBatch([]uint64) error               { return nil }
func (v *volatileJournal) LastID() uint64                           { return v.seq }
func (*volatileJournal) Pending() []wal.Entry                       { return nil }
func (*volatileJournal) VisitCompleted(func(wal.Entry) error) error { return nil }
func (*volatileJournal) Close() error                               { return nil }

// All variants use the same real actor state machine, receipts, event encoding,
// queues and periodic in-memory snapshot store. No HTTP/Mongo/network is timed.
// Fixed iterations bound receipt growth; use -benchtime=20000x, not a time limit.
func BenchmarkActorDurability(b *testing.B) {
	for _, mode := range []struct {
		name    string
		durable bool
		batch   int
		window  time.Duration
	}{
		{"no-wal-batch64", false, 64, time.Millisecond},
		{"wal-batch64", true, 64, time.Millisecond},
		{"no-wal-batch1", false, 1, 0},
		{"wal-batch1", true, 1, 0},
		{"wal-batch64-0us", true, 64, 0},
		{"wal-batch8-100us", true, 8, 100 * time.Microsecond},
		{"wal-batch64-100us", true, 64, 100 * time.Microsecond},
		{"wal-batch64-10ms", true, 64, 10 * time.Millisecond},
	} {
		b.Run(mode.name, func(b *testing.B) {
			if b.N > 200000 {
				b.Fatal("use fixed -benchtime <=200000x to respect receipt bounds")
			}
			dir := b.TempDir()
			o := DefaultOptions(dir)
			o.BatchSize = mode.batch
			o.BatchWindow = mode.window
			o.MaxUncheckpointed = 250000
			open := func(path string) (journal, error) {
				if mode.durable {
					return wal.Open(path)
				}
				return &volatileJournal{}, nil
			}
			engine, err := newEngine(context.Background(), newMemoryStore(), o, open)
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if err := engine.Close(context.Background()); err != nil {
					b.Error(err)
				}
			}()
			players, workers := 256, 32
			for _, setting := range []struct {
				name   string
				target *int
				limit  int
			}{{"BENCH_PLAYERS", &players, 10000}, {"BENCH_WORKERS", &workers, 1024}} {
				if value := os.Getenv(setting.name); value != "" {
					n, err := strconv.Atoi(value)
					if err != nil || n < 1 || n > setting.limit {
						b.Fatalf("invalid %s", setting.name)
					}
					*setting.target = n
				}
			}
			if (b.N+players-1)/players+1 > o.MaxReceiptsPerPlayer {
				b.Fatal("iterations exceed per-player receipt capacity")
			}
			for i := 0; i < players; i++ {
				if _, err := engine.Execute(context.Background(), createOp(fmt.Sprintf("bench_%d", i))); err != nil {
					b.Fatal(err)
				}
			}
			before := engine.Stats()
			latencies := make([]int64, b.N)
			var index atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			begin := time.Now()
			var wg sync.WaitGroup
			for worker := 0; worker < workers; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := int(index.Add(1) - 1)
						if i >= b.N {
							return
						}
						start := time.Now()
						r, err := engine.Execute(context.Background(), game.Operation{PlayerID: fmt.Sprintf("bench_%d", i%players), RequestID: fmt.Sprintf("r_%d", i), Kind: "reward", Amount: 1})
						latencies[i] = time.Since(start).Nanoseconds()
						if err != nil || r.Status != 200 {
							b.Errorf("operation failed: %v status=%d", err, r.Status)
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(begin)
			b.StopTimer()
			for i := 0; i < players; i++ {
				p, err := engine.Get(context.Background(), fmt.Sprintf("bench_%d", i))
				expected := int64(100 + b.N/players)
				if i < b.N%players {
					expected++
				}
				if err != nil || p.Coins != expected {
					b.Fatalf("balance %d want %d err=%v", p.Coins, expected, err)
				}
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			for _, q := range []struct {
				name string
				p    float64
			}{{"p50-us", .50}, {"p95-us", .95}, {"p99-us", .99}} {
				i := int(float64(len(latencies)-1) * q.p)
				b.ReportMetric(float64(latencies[i])/1000, q.name)
			}
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
			after := engine.Stats()
			var records, batches uint64
			for i, s := range after.Shards {
				records += s.WALRecords - before.Shards[i].WALRecords
				batches += s.WALBatches - before.Shards[i].WALBatches
			}
			if batches > 0 {
				b.ReportMetric(float64(records)/float64(batches), "events/batch")
			}
			if mode.durable {
				var bytes int64
				_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
					if err == nil && !d.IsDir() && filepath.Ext(path) == ".wal" {
						info, e := d.Info()
						if e == nil {
							bytes += info.Size()
						}
					}
					return err
				})
				b.ReportMetric(float64(bytes)/float64(b.N), "WAL-file-B/op")
			}
		})
	}
}

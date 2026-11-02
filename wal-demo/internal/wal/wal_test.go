package wal

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func openTestLog(t *testing.T, path string) *Log {
	t.Helper()
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func appendTestIntent(t *testing.T, l *Log, payload string) uint64 {
	t.Helper()
	id, err := l.Append(json.RawMessage(payload))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestReopenPendingAndMonotonicIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "assets.wal")
	l := openTestLog(t, path)
	one := appendTestIntent(t, l, `{"operation":"reward"}`)
	two := appendTestIntent(t, l, `{"operation":"buy"}`)
	three := appendTestIntent(t, l, `{"operation":"reward"}`)
	if err := l.Complete(two); err != nil {
		t.Fatal(err)
	}
	if err := l.Complete(two); err != nil {
		t.Fatalf("duplicate completion: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = openTestLog(t, path)
	pending := l.Pending()
	if len(pending) != 2 || pending[0].ID != one || pending[1].ID != three {
		t.Fatalf("pending entries: %+v", pending)
	}
	if got := appendTestIntent(t, l, `null`); got != three+1 {
		t.Fatalf("next ID = %d, want %d", got, three+1)
	}
	// Pending must not let callers mutate the log's retained payload.
	pending[0].Payload[0] = '!'
	if !json.Valid(l.Pending()[0].Payload) {
		t.Fatal("Pending returned an alias to retained payload")
	}
}

func TestExclusiveLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	l := openTestLog(t, path)
	if other, err := Open(path); !errors.Is(err, ErrLocked) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	openTestLog(t, path)
}

func TestTruncatesOnlyTornFinalFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	l := openTestLog(t, path)
	one := appendTestIntent(t, l, `{"item":"sword"}`)
	l.Close()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"record":{"type":"intent","id":2`); err != nil {
		t.Fatal(err)
	}
	f.Close()
	l = openTestLog(t, path)
	if entries := l.Pending(); len(entries) != 1 || entries[0].ID != one {
		t.Fatalf("recovered entries = %+v", entries)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("torn tail not removed correctly: %v", err)
	}
	if got := appendTestIntent(t, l, `{"item":"shield"}`); got != 2 {
		t.Fatalf("ID after truncated tail = %d", got)
	}
}

func TestCorruptCompleteFrameRefusedWithoutTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	l := openTestLog(t, path)
	appendTestIntent(t, l, `{"coins":100}`)
	l.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"coins":100`), []byte(`"coins":900`), 1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if bad, err := Open(path); err == nil {
		bad.Close()
		t.Fatal("corrupt complete record was accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, after) {
		t.Fatalf("corrupt complete frame must not be truncated: %v", err)
	}
}

func TestInvalidAndOversizedPayloadDoNotPoisonLog(t *testing.T) {
	l := openTestLog(t, filepath.Join(t.TempDir(), "assets.wal"))
	if _, err := l.Append(json.RawMessage(`not json`)); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	large, _ := json.Marshal(strings.Repeat("a", MaxFrameSize))
	if _, err := l.Append(large); err == nil {
		t.Fatal("oversized record accepted")
	}
	if got := appendTestIntent(t, l, `{"valid":true}`); got != 1 {
		t.Fatalf("validation errors consumed IDs: got %d", got)
	}
	if err := l.Complete(2); err == nil {
		t.Fatal("unknown completion accepted")
	}
}

func TestConcurrentAppendsAreSerialized(t *testing.T) {
	l := openTestLog(t, filepath.Join(t.TempDir(), "assets.wal"))
	const count = 20
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := l.Append(json.RawMessage(`{"coins":1}`))
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
	entries := l.Pending()
	if len(entries) != count {
		t.Fatalf("got %d entries", len(entries))
	}
	for i, entry := range entries {
		if entry.ID != uint64(i+1) {
			t.Fatalf("entry %d has ID %d", i, entry.ID)
		}
	}
}

func TestClosedLogRejectsMutation(t *testing.T) {
	l := openTestLog(t, filepath.Join(t.TempDir(), "assets.wal"))
	id := appendTestIntent(t, l, `{}`)
	l.Close()
	if _, err := l.Append(json.RawMessage(`{}`)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close = %v", err)
	}
	if err := l.Complete(id); !errors.Is(err, ErrClosed) {
		t.Fatalf("Complete after Close = %v", err)
	}
}

func TestIOFailurePoisonsLogUntilReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	l := openTestLog(t, path)
	id := appendTestIntent(t, l, `{}`)
	// Force a real filesystem write error rather than mocking a successful fsync.
	if err := l.file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(json.RawMessage(`{}`)); err == nil {
		t.Fatal("write to closed file unexpectedly succeeded")
	}
	if _, err := l.Append(json.RawMessage(`{}`)); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("Append after I/O failure = %v", err)
	}
	if err := l.Complete(id); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("Complete after I/O failure = %v", err)
	}
	l.Close()
	l = openTestLog(t, path)
	if entries := l.Pending(); len(entries) != 1 || entries[0].ID != id {
		t.Fatalf("recovery lost durable intent: %+v", entries)
	}
	if got := appendTestIntent(t, l, `{}`); got != id+1 {
		t.Fatalf("next ID after reopen = %d", got)
	}
}

func TestBatchAppendAndCompletionReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.wal")
	l := openTestLog(t, path)
	payloads := []json.RawMessage{json.RawMessage(`{"n":1}`), json.RawMessage(`{"n":2}`), json.RawMessage(`{"n":3}`)}
	ids, err := l.AppendBatch(payloads)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 || ids[0] != 1 || ids[2] != 3 || l.LastID() != 3 {
		t.Fatalf("IDs=%v last=%d", ids, l.LastID())
	}
	payloads[0][0] = '!'
	if !json.Valid(l.Pending()[0].Payload) {
		t.Fatal("batch retained caller payload alias")
	}
	if err := l.CompleteBatch([]uint64{ids[0], ids[2], ids[0]}); err != nil {
		t.Fatal(err)
	}
	if err := l.CompleteBatch([]uint64{ids[0], ids[2]}); err != nil {
		t.Fatal(err)
	}
	l.Close()
	l = openTestLog(t, path)
	if pending := l.Pending(); len(pending) != 1 || pending[0].ID != 2 {
		t.Fatalf("pending=%v", pending)
	}
	if l.LastID() != 3 {
		t.Fatalf("last=%d", l.LastID())
	}
	if got := appendTestIntent(t, l, `{}`); got != 4 {
		t.Fatalf("next=%d", got)
	}
}

func TestBatchValidationWritesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.wal")
	l := openTestLog(t, path)
	one := appendTestIntent(t, l, `{}`)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	large, _ := json.Marshal(strings.Repeat("x", MaxFrameSize))
	for _, payloads := range [][]json.RawMessage{
		{json.RawMessage(`{}`), json.RawMessage(`invalid`)},
		{json.RawMessage(`{}`), large},
	} {
		ids, err := l.AppendBatch(payloads)
		if err == nil || ids != nil {
			t.Fatalf("invalid batch returned %v %v", ids, err)
		}
	}
	if err := l.CompleteBatch([]uint64{one, 99}); err == nil {
		t.Fatal("unknown ID accepted")
	}
	if pending := l.Pending(); len(pending) != 1 || pending[0].ID != one {
		t.Fatal("validation changed pending")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("validation wrote bytes: %v", err)
	}
	if got := appendTestIntent(t, l, `{}`); got != 2 {
		t.Fatalf("validation consumed IDs: %d", got)
	}
}

func TestBatchOverflowAndEmpty(t *testing.T) {
	l := openTestLog(t, filepath.Join(t.TempDir(), "batch.wal"))
	if ids, err := l.AppendBatch(nil); err != nil || len(ids) != 0 {
		t.Fatalf("empty append=%v,%v", ids, err)
	}
	if err := l.CompleteBatch(nil); err != nil {
		t.Fatal(err)
	}
	l.lastID = ^uint64(0) - 1
	ids, err := l.AppendBatch([]json.RawMessage{json.RawMessage(`{}`), json.RawMessage(`{}`)})
	if err == nil || ids != nil || len(l.Pending()) != 0 {
		t.Fatalf("overflow=%v,%v", ids, err)
	}
}

func TestBatchIOFailureReturnsNoIDsAndPoisons(t *testing.T) {
	for _, completion := range []bool{false, true} {
		t.Run(map[bool]string{false: "append", true: "complete"}[completion], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "batch.wal")
			l := openTestLog(t, path)
			one := appendTestIntent(t, l, `{}`)
			if err := l.file.Close(); err != nil {
				t.Fatal(err)
			}
			if completion {
				if err := l.CompleteBatch([]uint64{one}); err == nil {
					t.Fatal("completion I/O failure missing")
				}
			} else {
				ids, err := l.AppendBatch([]json.RawMessage{json.RawMessage(`{}`), json.RawMessage(`{}`)})
				if err == nil || ids != nil {
					t.Fatalf("failed append returned %v,%v", ids, err)
				}
			}
			if _, err := l.AppendBatch([]json.RawMessage{json.RawMessage(`{}`)}); !errors.Is(err, ErrPoisoned) {
				t.Fatalf("not poisoned: %v", err)
			}
			if err := l.CompleteBatch([]uint64{one}); !errors.Is(err, ErrPoisoned) {
				t.Fatalf("completion not poisoned: %v", err)
			}
			l.Close()
			l = openTestLog(t, path)
			if p := l.Pending(); len(p) != 1 || p[0].ID != one {
				t.Fatalf("pending after reopen: %v", p)
			}
		})
	}
}

func TestPartialBatchRecoveryKeepsCompleteFrames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.wal")
	l := openTestLog(t, path)
	l.Close()
	one, err := encodeRecord(record{Type: "intent", ID: 1, Payload: json.RawMessage(`{"n":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	two, err := encodeRecord(record{Type: "intent", ID: 2, Payload: json.RawMessage(`{"n":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	// A crash can preserve one full frame and part of the next unacknowledged batch.
	if err := os.WriteFile(path, append(one, two[:len(two)/2]...), 0600); err != nil {
		t.Fatal(err)
	}
	l = openTestLog(t, path)
	if p := l.Pending(); len(p) != 1 || p[0].ID != 1 || l.LastID() != 1 {
		t.Fatalf("partial batch: %v last=%d", p, l.LastID())
	}
	if got := appendTestIntent(t, l, `{}`); got != 2 {
		t.Fatalf("next=%d", got)
	}
}

func TestConcurrentBatchesHaveContiguousIDs(t *testing.T) {
	l := openTestLog(t, filepath.Join(t.TempDir(), "batch.wal"))
	const workers = 12
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids, err := l.AppendBatch([]json.RawMessage{json.RawMessage(`{}`), json.RawMessage(`{}`)})
			if err == nil && (len(ids) != 2 || ids[1] != ids[0]+1) {
				err = errors.New("batch IDs not contiguous")
			}
			_ = l.LastID()
			_ = l.Pending()
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
	if len(l.Pending()) != 2*workers || l.LastID() != 2*workers {
		t.Fatalf("last=%d pending=%d", l.LastID(), len(l.Pending()))
	}
}

func TestVisitCompletedStreamsInOrderAndPreservesAppendOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "visit.wal")
	l := openTestLog(t, path)
	ids, err := l.AppendBatch([]json.RawMessage{json.RawMessage(`{"n":1}`), json.RawMessage(`{"n":2}`), json.RawMessage(`{"n":3}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CompleteBatch([]uint64{ids[2], ids[0]}); err != nil {
		t.Fatal(err)
	}
	var visited []Entry
	if err := l.VisitCompleted(func(e Entry) error { visited = append(visited, e); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(visited) != 2 || visited[0].ID != 1 || visited[1].ID != 3 {
		t.Fatalf("visited=%v", visited)
	}
	if string(visited[0].Payload) != `{"n":1}` || string(visited[1].Payload) != `{"n":3}` {
		t.Fatalf("payload=%v", visited)
	}
	stop := errors.New("visitor failure")
	if err := l.VisitCompleted(func(Entry) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("callback error=%v", err)
	}
	if got := appendTestIntent(t, l, `{"n":4}`); got != 4 {
		t.Fatalf("next=%d", got)
	}
	l.Close()
	l = openTestLog(t, path)
	if p := l.Pending(); len(p) != 2 || p[0].ID != 2 || p[1].ID != 4 {
		t.Fatalf("append offset damaged: %v", p)
	}
}

func TestVisitCompletedRefusesCorruptionAndClosedLogs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "visit.wal")
	l := openTestLog(t, path)
	id := appendTestIntent(t, l, `{"n":1}`)
	if err := l.Complete(id); err != nil {
		t.Fatal(err)
	}
	if err := l.VisitCompleted(nil); err == nil {
		t.Fatal("nil visitor accepted")
	}
	// Mutate an already-open file so this visitor must independently check CRC.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"n":1`), []byte(`"n":9`), 1)
	if _, err := l.file.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := l.VisitCompleted(func(Entry) error { return nil }); err == nil {
		t.Fatal("corruption accepted")
	}
	l.Close()
	if err := l.VisitCompleted(func(Entry) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed visitor=%v", err)
	}
}

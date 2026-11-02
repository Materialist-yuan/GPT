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

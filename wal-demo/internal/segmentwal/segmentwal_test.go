package segmentwal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/wal"
)

func testOpen(t *testing.T, path string, limit int64) *Log {
	t.Helper()
	l, err := Open(path, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func testAppend(t *testing.T, l *Log, players ...string) []uint64 {
	t.Helper()
	var payloads []json.RawMessage
	for _, player := range players {
		payload, _ := json.Marshal(map[string]string{"player": player})
		payloads = append(payloads, payload)
	}
	ids, err := l.AppendBatch(payloads)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func testKey(entry wal.Entry) (string, error) {
	var body struct {
		Player string `json:"player"`
	}
	if err := json.Unmarshal(entry.Payload, &body); err != nil {
		return "", err
	}
	if body.Player == "" {
		return "", errors.New("missing player")
	}
	return body.Player, nil
}

func TestMixedPendingProtectsPrefixAndReclaimPersistsProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "assets.wal")
	l := testOpen(t, path, 1)
	ids := testAppend(t, l, "alice", "bob")
	active := testAppend(t, l, "carol")[0]
	if !reflect.DeepEqual(ids, []uint64{1, 2}) || active != 3 {
		t.Fatalf("IDs=%v active=%d", ids, active)
	}
	if l.Stats().Segments != 2 {
		t.Fatal(l.Stats())
	}
	if err := l.CompleteBatch(ids[:1]); err != nil {
		t.Fatal(err)
	}
	if err := l.Compact(map[string]uint64{"alice": 1, "bob": 2}, testKey); err != nil {
		t.Fatal(err)
	}
	if l.Stats().Segments != 2 {
		t.Fatal("segment with pending bob was deleted")
	}
	if err := l.CompleteBatch(ids[1:]); err != nil {
		t.Fatal(err)
	}
	before := l.Stats()
	if err := l.Compact(map[string]uint64{"alice": 1, "bob": 2}, testKey); err != nil {
		t.Fatal(err)
	}
	after := l.Stats()
	if after.Segments != 1 || after.ReclaimedSegments != 1 || after.ReclaimedBytes == 0 || after.Bytes >= before.Bytes {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy physical segment retained: %v", err)
	}
	if want := map[string]uint64{"alice": 1, "bob": 2}; !reflect.DeepEqual(l.Retired(), want) {
		t.Fatalf("retired=%v", l.Retired())
	}
	copy := l.Retired()
	copy["alice"] = 999
	if l.Retired()["alice"] != 1 {
		t.Fatal("Retired exposes mutable manifest")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = testOpen(t, path, 1)
	if l.LastID() != 3 || l.Retired()["alice"] != 1 {
		t.Fatalf("lost sequence/proof: %d %v", l.LastID(), l.Retired())
	}
	pending := l.Pending()
	if len(pending) != 1 || pending[0].ID != 3 {
		t.Fatalf("pending=%v", pending)
	}
	if next := testAppend(t, l, "alice")[0]; next != 4 {
		t.Fatalf("next=%d", next)
	}
	if err := l.CompleteBatch([]uint64{1, 2, 3, 4}); err != nil {
		t.Fatalf("retired duplicate completion: %v", err)
	}
}

func TestInadequateCheckpointForbidsDeletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	l := testOpen(t, path, 1)
	id := testAppend(t, l, "alice")[0]
	testAppend(t, l, "bob") // Seal alice's segment.
	if err := l.CompleteBatch([]uint64{id}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, proof := range []map[string]uint64{nil, {"alice": 0}} {
		if err := l.Compact(proof, testKey); err == nil {
			t.Fatal("insufficient checkpoint allowed reclamation")
		}
		if l.Stats().Segments != 2 || len(l.Retired()) != 0 {
			t.Fatal("failed validation changed live manifest")
		}
		after, err := os.ReadFile(path)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("validation removed/changed segment: %v", err)
		}
	}
	if err := l.Compact(map[string]uint64{"alice": 1}, testKey); err != nil {
		t.Fatalf("validation error poisoned usable log: %v", err)
	}
}

func TestPendingPrefixPinsLaterCompletedSegments(t *testing.T) {
	l := testOpen(t, filepath.Join(t.TempDir(), "assets.wal"), 1)
	a := testAppend(t, l, "alice")[0]
	b := testAppend(t, l, "bob")[0]
	testAppend(t, l, "carol")
	if err := l.CompleteBatch([]uint64{b}); err != nil {
		t.Fatal(err)
	}
	if err := l.Compact(map[string]uint64{"bob": b}, testKey); err != nil {
		t.Fatal(err)
	}
	if l.Stats().Segments != 3 {
		t.Fatal("non-prefix reclamation bypassed pending alice")
	}
	if err := l.CompleteBatch([]uint64{a}); err != nil {
		t.Fatal(err)
	}
	if err := l.Compact(map[string]uint64{"alice": a, "bob": b}, testKey); err != nil {
		t.Fatal(err)
	}
	if l.Stats().Segments != 1 || l.Stats().ReclaimedSegments != 2 {
		t.Fatal(l.Stats())
	}
}

func TestLegacyAdoptionKeepsPendingIDsAndCompletedHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	old, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := old.AppendBatch([]json.RawMessage{json.RawMessage(`{"player":"alice"}`), json.RawMessage(`{"player":"bob"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := old.CompleteBatch(ids[:1]); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	l := testOpen(t, path, 1)
	if l.LastID() != 2 || len(l.Pending()) != 1 || l.Pending()[0].ID != 2 {
		t.Fatalf("legacy state lost: %d %v", l.LastID(), l.Pending())
	}
	var completed []uint64
	if err := l.VisitCompleted(func(e wal.Entry) error { completed = append(completed, e.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(completed, []uint64{1}) {
		t.Fatalf("completed=%v", completed)
	}
	if got := testAppend(t, l, "carol")[0]; got != 3 {
		t.Fatalf("new ID=%d", got)
	}
}

func TestExclusiveLockSurvivesLegacySegmentDeletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	l := testOpen(t, path, 1)
	a := testAppend(t, l, "alice")[0]
	testAppend(t, l, "bob")
	if err := l.CompleteBatch([]uint64{a}); err != nil {
		t.Fatal(err)
	}
	if err := l.Compact(map[string]uint64{"alice": a}, testKey); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(path, 1); err == nil {
		other.Close()
		t.Fatal("second writer acquired lock after first segment deletion")
	}
	l.Close()
	testOpen(t, path, 1)
}

func TestMissingReferencedFileAndCorruptManifestRefuseStartup(t *testing.T) {
	for _, scenario := range []string{"missing-legacy", "corrupt-manifest", "missing-manifest"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "assets.wal")
			l := testOpen(t, path, 1)
			testAppend(t, l, "alice")
			testAppend(t, l, "bob")
			l.Close()
			manifest := filepath.Join(path+".segments", "manifest.json")
			switch scenario {
			case "missing-legacy":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "corrupt-manifest":
				if err := os.WriteFile(manifest, []byte(`{"corrupt":true}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing-manifest":
				if err := os.Remove(manifest); err != nil {
					t.Fatal(err)
				}
			}
			if other, err := Open(path, 1); err == nil {
				other.Close()
				t.Fatal("damaged journal silently recreated")
			}
		})
	}
}

func TestCompactionCrashSeamsReopenFromPublishedManifest(t *testing.T) {
	for _, stage := range []string{"before-manifest-rename", "after-manifest-sync", "before-unlink", "after-unlink"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "assets.wal")
			l := testOpen(t, path, 1)
			a := testAppend(t, l, "alice")[0]
			b := testAppend(t, l, "bob")[0]
			c := testAppend(t, l, "carol")[0]
			if err := l.CompleteBatch([]uint64{a, b}); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("simulated interrupted GC")
			l.hook = func(at string) error {
				if at == stage {
					return injected
				}
				return nil
			}
			if err := l.Compact(map[string]uint64{"alice": a, "bob": b}, testKey); err == nil {
				t.Fatal("fault seam not reached")
			}
			if _, err := l.AppendBatch([]json.RawMessage{json.RawMessage(`{"player":"unsafe"}`)}); err == nil {
				t.Fatal("uncertain GC allowed further writes")
			}
			l.Close()
			l = testOpen(t, path, 1)
			pending := l.Pending()
			if len(pending) != 1 || pending[0].ID != c {
				t.Fatalf("pending lost across GC interruption: %v", pending)
			}
			if stage == "before-manifest-rename" {
				if len(l.Retired()) != 0 || l.Stats().Segments != 3 {
					t.Fatalf("unpublished GC became visible: %v %+v", l.Retired(), l.Stats())
				}
			} else {
				if l.Retired()["alice"] != a || l.Retired()["bob"] != b || l.Stats().Segments != 1 {
					t.Fatalf("published GC lost progress: %v %+v", l.Retired(), l.Stats())
				}
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("orphan legacy not cleaned: %v", err)
				}
			}
			if next := testAppend(t, l, "dave")[0]; next != c+1 {
				t.Fatalf("ID discontinuity after GC: %d", next)
			}
		})
	}
}

func TestTornActiveTailPreservesGlobalSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	l := testOpen(t, path, 1)
	testAppend(t, l, "alice")
	testAppend(t, l, "bob")
	l.Close()
	files, err := filepath.Glob(filepath.Join(path+".segments", "segment-*.wal"))
	if err != nil || len(files) != 1 {
		t.Fatalf("files=%v err=%v", files, err)
	}
	f, err := os.OpenFile(files[0], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"record":{"type":"intent"`); err != nil {
		t.Fatal(err)
	}
	f.Close()
	l = testOpen(t, path, 1)
	if l.LastID() != 2 || len(l.Pending()) != 2 {
		t.Fatalf("tail damaged earlier intents: last=%d pending=%v", l.LastID(), l.Pending())
	}
	if got := testAppend(t, l, "carol")[0]; got != 3 {
		t.Fatalf("next=%d", got)
	}
}

func TestConcurrentAppendIDsAndClosedMutation(t *testing.T) {
	l := testOpen(t, filepath.Join(t.TempDir(), "assets.wal"), 1)
	const count = 24
	var wg sync.WaitGroup
	ids := make(chan uint64, count)
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := json.RawMessage(fmt.Sprintf(`{"player":"p%d"}`, i))
			batch, err := l.AppendBatch([]json.RawMessage{payload})
			if err != nil {
				errs <- err
				return
			}
			ids <- batch[0]
		}(i)
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		t.Error(err)
	}
	seen := map[uint64]bool{}
	for id := range ids {
		seen[id] = true
	}
	for id := uint64(1); id <= count; id++ {
		if !seen[id] {
			t.Fatalf("missing ID%d", id)
		}
	}
	if len(l.Pending()) != count {
		t.Fatal(l.Pending())
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendBatch([]json.RawMessage{json.RawMessage(`null`)}); err == nil {
		t.Fatal("closed append accepted")
	}
}

func TestCompactionCallbackFailureDoesNotDelete(t *testing.T) {
	l := testOpen(t, filepath.Join(t.TempDir(), "assets.wal"), 1)
	a := testAppend(t, l, "alice")[0]
	testAppend(t, l, "bob")
	l.CompleteBatch([]uint64{a})
	err := l.Compact(map[string]uint64{"alice": a}, func(wal.Entry) (string, error) { return "", errors.New("schema unsupported") })
	if err == nil || !strings.Contains(err.Error(), "schema unsupported") {
		t.Fatalf("callback err=%v", err)
	}
	if l.Stats().Segments != 2 || len(l.Retired()) != 0 {
		t.Fatal("callback failure removed history")
	}
}

func TestCompletionValidationIsWholeBatchAndVisitorUsesGlobalIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	l := testOpen(t, path, 1)
	a := testAppend(t, l, "alice")[0]
	b := testAppend(t, l, "bob")[0]
	c := testAppend(t, l, "carol")[0]
	if err := l.CompleteBatch([]uint64{a, c + 1}); err == nil {
		t.Fatal("future completion accepted")
	}
	if len(l.Pending()) != 3 {
		t.Fatal("invalid completion partially persisted a known ID")
	}
	if err := l.CompleteBatch([]uint64{b, a, b}); err != nil {
		t.Fatal(err)
	}
	var seen []uint64
	if err := l.VisitCompleted(func(e wal.Entry) error { seen = append(seen, e.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, []uint64{a, b}) {
		t.Fatalf("visitor exposed local/reordered IDs: %v", seen)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = testOpen(t, path, 1)
	if pending := l.Pending(); len(pending) != 1 || pending[0].ID != c {
		t.Fatal(pending)
	}
}

func TestRetiredProofMergesAcrossRepeatedCollections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	l := testOpen(t, path, 1)
	for i := uint64(1); i <= 5; i++ {
		id := testAppend(t, l, "alice")[0]
		if id != i {
			t.Fatalf("id=%d want=%d", id, i)
		}
		if err := l.CompleteBatch([]uint64{id}); err != nil {
			t.Fatal(err)
		}
		if err := l.Compact(map[string]uint64{"alice": id}, testKey); err != nil {
			t.Fatal(err)
		}
		if i > 1 && l.Retired()["alice"] != i-1 {
			t.Fatalf("proof=%v want previous sealed event %d", l.Retired(), i-1)
		}
	}
	l.Close()
	l = testOpen(t, path, 1)
	if l.LastID() != 5 || l.Retired()["alice"] != 4 {
		t.Fatalf("recovered last=%d proof=%v", l.LastID(), l.Retired())
	}
	if got := testAppend(t, l, "alice")[0]; got != 6 {
		t.Fatalf("next=%d", got)
	}
}

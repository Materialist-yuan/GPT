package segmentwal

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestReclamationCrashProcessHelper(t *testing.T) {
	dir := os.Getenv("SEGMENT_CRASH_TEST_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	l := testOpen(t, filepath.Join(dir, "assets.wal"), 1)
	a := testAppend(t, l, "alice")[0]
	b := testAppend(t, l, "bob")[0]
	_ = testAppend(t, l, "carol")
	if err := l.CompleteBatch([]uint64{a, b}); err != nil {
		t.Fatal(err)
	}
	l.hook = func(stage string) error {
		if stage == os.Getenv("SEGMENT_CRASH_TEST_STAGE") {
			if err := os.WriteFile(filepath.Join(dir, "reached"), []byte(stage), 0600); err != nil {
				t.Fatal(err)
			}
			for {
				time.Sleep(time.Hour)
			}
		}
		return nil
	}
	if err := l.Compact(map[string]uint64{"alice": a, "bob": b}, testKey); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash stage was not reached")
}

func TestSIGKILLAtReclamationBoundaries(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"before-manifest-rename", "after-manifest-rename", "after-manifest-sync", "before-unlink", "after-unlink"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(executable, "-test.run=^TestReclamationCrashProcessHelper$")
			cmd.Env = append(os.Environ(), "SEGMENT_CRASH_TEST_DIR="+dir, "SEGMENT_CRASH_TEST_STAGE="+stage)
			cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			alive := true
			defer func() {
				if alive {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			for deadline := time.Now().Add(15 * time.Second); ; {
				if _, err := os.Stat(filepath.Join(dir, "reached")); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("child did not reach crash boundary")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			alive = false
			l := testOpen(t, filepath.Join(dir, "assets.wal"), 1)
			pending := l.Pending()
			if len(pending) != 1 || pending[0].ID != 3 {
				t.Fatalf("pending lost: %v", pending)
			}
			if stage == "before-manifest-rename" {
				if len(l.Retired()) != 0 || l.Stats().Segments != 3 {
					t.Fatalf("unpublished state adopted: %+v", l.Stats())
				}
			} else {
				if l.Retired()["alice"] != 1 || l.Retired()["bob"] != 2 || l.Stats().Segments != 1 {
					t.Fatalf("lost published state: %v %+v", l.Retired(), l.Stats())
				}
				if _, err := os.Stat(filepath.Join(dir, "assets.wal")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("legacy residue not reclaimed: %v", err)
				}
			}
			if next := testAppend(t, l, "dave")[0]; next != 4 {
				t.Fatalf("ID reused after crash: %d", next)
			}
		})
	}
}

func TestCleanupPreservesUnknownFilesAndInitialTempIsRecoverable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "assets.wal")
	l := testOpen(t, path, 1)
	testAppend(t, l, "alice")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	dir := path + ".segments"
	unknown := filepath.Join(dir, "segment-abcdefghijklmnopqrst.wal")
	if err := os.WriteFile(unknown, []byte("user notes"), 0600); err != nil {
		t.Fatal(err)
	}
	l = testOpen(t, path, 1)
	if _, err := os.Stat(unknown); err != nil {
		t.Fatal("cleanup removed unknown file")
	}
	l.Close()
	if err := os.Remove(unknown); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest-123456789.tmp"), []byte("partial bootstrap"), 0600); err != nil {
		t.Fatal(err)
	}
	l = testOpen(t, path, 1)
	if len(l.Pending()) != 1 || l.Pending()[0].ID != 1 {
		t.Fatal("bootstrap lost legacy intent")
	}
}

// Package segmentwal adds event segments and checkpoint-safe prefix reclamation
// to the checksummed WAL. Sealed segments may still receive completion markers;
// no new intents are appended to them.
package segmentwal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/wal"
)

type segment struct {
	Name   string `json:"name"`
	Base   uint64 `json:"base"`
	Count  uint64 `json:"count"`
	Sealed bool   `json:"sealed"`
}
type manifestState struct {
	Version  int               `json:"version"`
	Floor    uint64            `json:"floor"`
	Segments []segment         `json:"segments"`
	Retired  map[string]uint64 `json:"retired"`
}
type manifestFrame struct {
	Data  json.RawMessage `json:"data"`
	CRC32 uint32          `json:"crc32"`
}
type opened struct {
	desc segment
	log  *wal.Log
}
type Stats struct {
	Segments          int    `json:"segments"`
	Bytes             int64  `json:"bytes"`
	ReclaimedSegments uint64 `json:"reclaimed_segments"`
	ReclaimedBytes    uint64 `json:"reclaimed_bytes"`
}
type Log struct {
	mu                                sync.Mutex
	path, dir                         string
	limit                             int64
	lock                              *os.File
	m                                 manifestState
	segments                          []opened
	failed                            error
	closed                            bool
	reclaimedSegments, reclaimedBytes uint64
	hook                              func(string) error // fault injection; never set by production callers
}

func Open(path string, segmentBytes int64) (result *Log, err error) {
	if path == "" || segmentBytes < 1 {
		return nil, errors.New("invalid segmented WAL options")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dir := path + ".segments"
	if err = durableDirectory(dir); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "owner.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("segmented WAL locked: %w", err)
	}
	l := &Log{path: path, dir: dir, limit: segmentBytes, lock: lock}
	defer func() {
		if err != nil {
			for _, s := range l.segments {
				_ = s.log.Close()
			}
			_ = lock.Close()
		}
	}()
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if errors.Is(err, os.ErrNotExist) {
		entries, e := os.ReadDir(dir)
		if e != nil {
			return nil, e
		}
		for _, entry := range entries {
			if entry.Name() != "owner.lock" && !manifestTemporary(entry.Name()) {
				return nil, errors.New("nonempty segment directory has no manifest")
			}
		}
		// An interrupted initial manifest publication can leave only its temp
		// file. No numbered segments exist before the first manifest commits.
		for _, entry := range entries {
			if manifestTemporary(entry.Name()) {
				if err = os.Remove(filepath.Join(dir, entry.Name())); err != nil {
					return nil, err
				}
			}
		}
		if err = syncDirectory(dir); err != nil {
			return nil, err
		}
		journal, e := wal.Open(path)
		if e != nil {
			return nil, e
		}
		l.m = manifestState{Version: 1, Segments: []segment{{Name: "legacy"}}, Retired: map[string]uint64{}}
		l.segments = []opened{{desc: l.m.Segments[0], log: journal}}
		if err = l.publish(l.m); err != nil {
			return nil, err
		}
	} else {
		if err != nil {
			return nil, err
		}
		if err = decodeManifest(b, &l.m); err != nil {
			return nil, fmt.Errorf("invalid segment manifest: %w", err)
		}
		for _, desc := range l.m.Segments {
			file := l.filePath(desc)
			if err = regularFile(file); err != nil {
				return nil, fmt.Errorf("referenced WAL segment unavailable: %w", err)
			}
			journal, e := wal.Open(file)
			if e != nil {
				return nil, e
			}
			l.segments = append(l.segments, opened{desc: desc, log: journal})
			if journal.LastID() > math.MaxUint64-desc.Base || (desc.Sealed && journal.LastID() != desc.Count) {
				return nil, errors.New("segment sequence range mismatch")
			}
		}
	}
	// The durable manifest is the authority. Files not named by it can only be
	// leftovers from creation before publish or deletion after publish.
	if err = l.cleanup(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) filePath(s segment) string {
	if s.Name == "legacy" {
		return l.path
	}
	return filepath.Join(l.dir, s.Name)
}
func filename(base uint64) string { return fmt.Sprintf("segment-%020d.wal", base) }
func regularFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("not a regular WAL file: %s", path)
	}
	return nil
}
func (l *Log) writable() error {
	if l.closed {
		return wal.ErrClosed
	}
	if l.failed != nil {
		return errors.Join(wal.ErrPoisoned, l.failed)
	}
	return nil
}
func (l *Log) poison(err error) error { l.failed = err; return err }
func (l *Log) invoke(stage string) error {
	if l.hook != nil {
		return l.hook(stage)
	}
	return nil
}
func cloneManifest(m manifestState) manifestState {
	m.Segments = append([]segment(nil), m.Segments...)
	ret := make(map[string]uint64, len(m.Retired))
	for k, v := range m.Retired {
		ret[k] = v
	}
	m.Retired = ret
	return m
}

func decodeManifest(b []byte, m *manifestState) error {
	var frame manifestFrame
	if err := strictJSON(b, &frame); err != nil {
		return err
	}
	if crc32.ChecksumIEEE(frame.Data) != frame.CRC32 {
		return errors.New("manifest checksum mismatch")
	}
	if err := strictJSON(frame.Data, m); err != nil {
		return err
	}
	if m.Version != 1 || len(m.Segments) == 0 || m.Retired == nil {
		return errors.New("unsupported or incomplete manifest")
	}
	if m.Segments[0].Base != m.Floor {
		return errors.New("manifest floor mismatch")
	}
	for i, s := range m.Segments {
		if s.Name != "legacy" && s.Name != filename(s.Base) {
			return errors.New("invalid segment name")
		}
		if s.Name == "legacy" && (i != 0 || s.Base != 0 || m.Floor != 0) {
			return errors.New("invalid legacy segment")
		}
		if s.Sealed != (i < len(m.Segments)-1) || (!s.Sealed && s.Count != 0) || (s.Sealed && s.Count == 0) {
			return errors.New("invalid segment sealing")
		}
		if s.Base > math.MaxUint64-s.Count {
			return errors.New("segment range overflow")
		}
		if i > 0 && s.Base != m.Segments[i-1].Base+m.Segments[i-1].Count {
			return errors.New("noncontiguous segments")
		}
	}
	if m.Floor > 0 && len(m.Retired) == 0 {
		return errors.New("retired prefix lacks checkpoint proof")
	}
	for key, seq := range m.Retired {
		if key == "" || seq == 0 || seq > m.Floor {
			return errors.New("invalid retired proof")
		}
	}
	return nil
}
func strictJSON(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}

// publish returns success only after the replacement manifest and its directory
// entry are durable. Any failure after rename has an uncertain outcome; callers
// preserve all old files and require reopen.
func (l *Log) publish(m manifestState) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	b, err := json.Marshal(manifestFrame{Data: body, CRC32: crc32.ChecksumIEEE(body)})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(l.dir, "manifest-*.tmp")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		var n int
		n, err = f.Write(b)
		if err == nil && n != len(b) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = l.invoke("before-manifest-rename"); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(l.dir, "manifest.json")); err != nil {
		return err
	}
	if err = l.invoke("after-manifest-rename"); err != nil {
		return err
	}
	if err = syncDirectory(l.dir); err != nil {
		return err
	}
	return l.invoke("after-manifest-sync")
}

func (l *Log) rotate() error {
	active := &l.segments[len(l.segments)-1]
	info, err := os.Stat(l.filePath(active.desc))
	if err != nil {
		return err
	}
	count := active.log.LastID()
	if info.Size() < l.limit || count == 0 {
		return nil
	}
	if count > math.MaxUint64-active.desc.Base {
		return errors.New("WAL sequence overflow")
	}
	base := active.desc.Base + count
	desc := segment{Name: filename(base), Base: base}
	file := l.filePath(desc)
	if _, err := os.Lstat(file); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("new WAL segment already exists: %s", file)
	}
	journal, err := wal.Open(file)
	if err != nil {
		return err
	}
	m := cloneManifest(l.m)
	m.Segments[len(m.Segments)-1].Sealed = true
	m.Segments[len(m.Segments)-1].Count = count
	m.Segments = append(m.Segments, desc)
	if err = l.publish(m); err != nil {
		_ = journal.Close()
		return err
	}
	active.desc = m.Segments[len(m.Segments)-2]
	l.segments = append(l.segments, opened{desc: desc, log: journal})
	l.m = m
	return nil
}

func (l *Log) AppendBatch(payloads []json.RawMessage) ([]uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writable(); err != nil {
		return nil, err
	}
	if len(payloads) == 0 {
		return nil, nil
	}
	// Validate before rotation so invalid requests never change the file layout.
	for _, p := range payloads {
		if !json.Valid(p) || len(p) > wal.MaxFrameSize-128 {
			return nil, errors.New("invalid or oversized segmented WAL payload")
		}
	}
	if uint64(len(payloads)) > math.MaxUint64-l.lastID() {
		return nil, errors.New("WAL sequence overflow")
	}
	if err := l.rotate(); err != nil {
		return nil, l.poison(err)
	}
	active := l.segments[len(l.segments)-1]
	ids, err := active.log.AppendBatch(payloads)
	if err != nil {
		return nil, l.poison(err)
	}
	for i := range ids {
		ids[i] += active.desc.Base
	}
	return ids, nil
}
func (l *Log) CompleteBatch(ids []uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writable(); err != nil {
		return err
	}
	groups := make([][]uint64, len(l.segments))
	last := l.lastID()
	for _, id := range ids {
		if id == 0 || id > last {
			return errors.New("unknown segmented WAL intent")
		}
		if id <= l.m.Floor {
			continue
		}
		found := false
		for i, s := range l.segments {
			if id > s.desc.Base && id <= s.desc.Base+s.log.LastID() {
				groups[i] = append(groups[i], id-s.desc.Base)
				found = true
				break
			}
		}
		if !found {
			return errors.New("intent not covered by retained segments")
		}
	}
	for i, group := range groups {
		if err := l.segments[i].log.CompleteBatch(group); err != nil {
			return l.poison(err)
		}
	}
	return nil
}
func (l *Log) lastID() uint64 {
	s := l.segments[len(l.segments)-1]
	return s.desc.Base + s.log.LastID()
}
func (l *Log) LastID() uint64 { l.mu.Lock(); defer l.mu.Unlock(); return l.lastID() }
func (l *Log) Pending() []wal.Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var entries []wal.Entry
	for _, s := range l.segments {
		for _, e := range s.log.Pending() {
			e.ID += s.desc.Base
			entries = append(entries, e)
		}
	}
	return entries
}
func (l *Log) VisitCompleted(fn func(wal.Entry) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writable(); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("nil visitor")
	}
	for _, s := range l.segments {
		if err := s.log.VisitCompleted(func(e wal.Entry) error { e.ID += s.desc.Base; return fn(e) }); err != nil {
			return err
		}
	}
	return nil
}
func (l *Log) Retired() map[string]uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return cloneManifest(l.m).Retired
}

// Compact never removes active or pending segments. Before unlinking, it
// persists the exact retained set and a per-key lower bound for checkpoint
// validation. A crash can leave extra files, never a required deleted file.
func (l *Log) Compact(checkpoints map[string]uint64, keyOf func(wal.Entry) (string, error)) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writable(); err != nil {
		return err
	}
	if keyOf == nil {
		return errors.New("nil checkpoint key function")
	}
	m := cloneManifest(l.m)
	n := 0
	var removedBytes uint64
	for i, s := range l.segments {
		if i == len(l.segments)-1 || len(s.log.Pending()) > 0 {
			break
		}
		if err := s.log.VisitCompleted(func(e wal.Entry) error {
			e.ID += s.desc.Base
			key, err := keyOf(e)
			if err != nil {
				return err
			}
			if key == "" || checkpoints[key] < e.ID {
				return fmt.Errorf("checkpoint does not cover %q sequence %d", key, e.ID)
			}
			if m.Retired[key] < e.ID {
				m.Retired[key] = e.ID
			}
			return nil
		}); err != nil {
			return err
		}
		info, err := os.Stat(l.filePath(s.desc))
		if err != nil {
			return l.poison(err)
		}
		removedBytes += uint64(info.Size())
		n++
	}
	if n == 0 {
		return nil
	}
	m.Segments = append([]segment(nil), m.Segments[n:]...)
	m.Floor = m.Segments[0].Base
	if err := l.publish(m); err != nil {
		return l.poison(err)
	}
	retired := append([]opened(nil), l.segments[:n]...)
	l.m = m
	l.segments = l.segments[n:]
	// Close even when injection/failure interrupts physical cleanup. The stable
	// sidecar lock remains held until Close; next Open resumes leftover deletion.
	var closeErr error
	for _, s := range retired {
		closeErr = errors.Join(closeErr, s.log.Close())
	}
	if closeErr != nil {
		return l.poison(closeErr)
	}
	if err := l.invoke("before-unlink"); err != nil {
		return l.poison(err)
	}
	for _, s := range retired {
		if err := os.Remove(l.filePath(s.desc)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return l.poison(err)
		}
		if err := l.invoke("after-unlink"); err != nil {
			return l.poison(err)
		}
	}
	if err := syncDirectory(l.dir); err != nil {
		return l.poison(err)
	}
	if err := syncDirectory(filepath.Dir(l.path)); err != nil {
		return l.poison(err)
	}
	l.reclaimedSegments += uint64(n)
	l.reclaimedBytes += removedBytes
	return nil
}

func (l *Log) cleanup() error {
	keep := map[string]bool{"manifest.json": true, "owner.lock": true}
	legacy := false
	for _, s := range l.m.Segments {
		if s.Name == "legacy" {
			legacy = true
		} else {
			keep[s.Name] = true
		}
	}
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if keep[e.Name()] {
			continue
		}
		name := e.Name()
		if segmentFilename(name) || manifestTemporary(name) {
			if err := os.Remove(filepath.Join(l.dir, name)); err != nil {
				return err
			}
		}
	}
	if !legacy && l.m.Floor > 0 {
		if err := os.Remove(l.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return errors.Join(syncDirectory(l.dir), syncDirectory(filepath.Dir(l.path)))
}

func segmentFilename(name string) bool {
	if len(name) != 32 || name[:8] != "segment-" || filepath.Ext(name) != ".wal" {
		return false
	}
	base, err := strconv.ParseUint(name[8:28], 10, 64)
	return err == nil && name == filename(base)
}

func manifestTemporary(name string) bool {
	return len(name) > 13 && name[:9] == "manifest-" && filepath.Ext(name) == ".tmp"
}
func (l *Log) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := Stats{Segments: len(l.segments), ReclaimedSegments: l.reclaimedSegments, ReclaimedBytes: l.reclaimedBytes}
	for _, seg := range l.segments {
		if info, err := os.Stat(l.filePath(seg.desc)); err == nil {
			s.Bytes += info.Size()
		}
	}
	return s
}
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var err error
	for _, s := range l.segments {
		err = errors.Join(err, s.log.Close())
	}
	return errors.Join(err, l.lock.Close())
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
func durableDirectory(path string) error {
	var missing []string
	for p := filepath.Clean(path); ; p = filepath.Dir(p) {
		_, err := os.Stat(p)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, p)
		if filepath.Dir(p) == p {
			return err
		}
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	for _, p := range missing {
		if err := syncDirectory(p); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		return syncDirectory(filepath.Dir(missing[len(missing)-1]))
	}
	return nil
}

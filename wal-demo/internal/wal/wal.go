// Package wal implements a single-process, append-only write-ahead log.
// An intent becomes durable before Append returns; Complete durably records that
// its database operation no longer needs replay. Completed records are retained.
package wal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
)

// MaxFrameSize includes the terminating newline of each on-disk record.
const MaxFrameSize = 1 << 20

var (
	ErrClosed   = errors.New("wal is closed")
	ErrPoisoned = errors.New("wal write failed; close and reopen before reuse")
	ErrLocked   = errors.New("wal is already locked by another process")
)

type Entry struct {
	ID      uint64          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

type record struct {
	Type    string          `json:"type"`
	ID      uint64          `json:"id"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type frame struct {
	Record json.RawMessage `json:"record"`
	CRC32  uint32          `json:"crc32"`
}

type Log struct {
	mu      sync.Mutex
	file    *os.File
	pending map[uint64]json.RawMessage
	lastID  uint64
	failed  error
	closed  bool
}

// Open locks path exclusively and replays its records. Only an unterminated
// final frame is truncated; a malformed or corrupt complete frame is an error.
// The caller must keep the log on durable local storage for crash recovery.
func Open(path string) (*Log, error) {
	dir := filepath.Dir(path)
	if err := makeDirectoryDurable(dir); err != nil {
		return nil, fmt.Errorf("create wal directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	created := err == nil
	if errors.Is(err, os.ErrExist) {
		f, err = os.OpenFile(path, os.O_RDWR, 0o600)
	}
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, path)
		}
		return nil, fmt.Errorf("lock wal: %w", err)
	}
	l := &Log{file: f, pending: make(map[uint64]json.RawMessage)}
	if created {
		// Persist the file before its directory entry. Each record is fsynced too.
		err = f.Sync()
		if err == nil {
			err = syncDirectory(dir)
		}
		if err != nil {
			l.Close()
			return nil, fmt.Errorf("persist new wal: %w", err)
		}
	}
	if err := l.recover(); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

// mkdir itself is not durable until its entry in the parent is synced. Sync
// every newly created directory and the existing ancestor that contains it.
func makeDirectoryDurable(path string) error {
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
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	for _, dir := range missing {
		if err := syncDirectory(dir); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		return syncDirectory(filepath.Dir(missing[len(missing)-1]))
	}
	return nil
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func (l *Log) recover() error {
	r := bufio.NewReaderSize(l.file, MaxFrameSize+1)
	var offset int64
	for {
		line, err := r.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) || len(line) > MaxFrameSize {
			return fmt.Errorf("wal record at byte %d exceeds %d bytes", offset, MaxFrameSize)
		}
		if errors.Is(err, io.EOF) {
			if len(line) > 0 {
				if err := l.file.Truncate(offset); err != nil {
					return fmt.Errorf("truncate torn wal tail: %w", err)
				}
				if err := l.file.Sync(); err != nil {
					return fmt.Errorf("sync truncated wal: %w", err)
				}
			}
			_, err := l.file.Seek(offset, io.SeekStart)
			return err
		}
		if err != nil {
			return fmt.Errorf("read wal at byte %d: %w", offset, err)
		}
		if err := l.recoverFrame(line); err != nil {
			return fmt.Errorf("corrupt wal at byte %d: %w", offset, err)
		}
		offset += int64(len(line))
	}
}

func decodeStrict(data []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after JSON record")
	}
	return nil
}

func (l *Log) recoverFrame(data []byte) error {
	var f frame
	if err := decodeStrict(data, &f); err != nil {
		return err
	}
	if crc32.ChecksumIEEE(f.Record) != f.CRC32 {
		return errors.New("checksum mismatch")
	}
	var rec record
	if err := decodeStrict(f.Record, &rec); err != nil {
		return err
	}
	switch rec.Type {
	case "intent":
		if rec.ID == 0 || l.lastID == math.MaxUint64 || rec.ID != l.lastID+1 || !json.Valid(rec.Payload) {
			return errors.New("invalid intent ID or payload")
		}
		l.pending[rec.ID] = append(json.RawMessage(nil), rec.Payload...)
		l.lastID = rec.ID
	case "complete":
		if _, ok := l.pending[rec.ID]; !ok || len(rec.Payload) != 0 {
			return errors.New("completion references no pending intent")
		}
		delete(l.pending, rec.ID)
	default:
		return fmt.Errorf("unknown record type %q", rec.Type)
	}
	return nil
}

// Append writes and fsyncs one JSON intent. See AppendBatch for recovery rules.
func (l *Log) Append(payload json.RawMessage) (uint64, error) {
	ids, err := l.AppendBatch([]json.RawMessage{payload})
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

// AppendBatch durably appends a group of independently recoverable intents with
// one fsync. All payloads are validated before any bytes are written. A failed
// batch returns no IDs and poisons the log on I/O uncertainty; some complete
// frames may still survive a crash and must be safely replayed by the caller.
func (l *Log) AppendBatch(payloads []json.RawMessage) ([]uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writable(); err != nil {
		return nil, err
	}
	if len(payloads) == 0 {
		return nil, nil
	}
	if uint64(len(payloads)) > math.MaxUint64-l.lastID {
		return nil, errors.New("wal ID space exhausted")
	}
	ids := make([]uint64, len(payloads))
	copies := make([]json.RawMessage, len(payloads))
	var data []byte
	for i, payload := range payloads {
		if !json.Valid(payload) {
			return nil, errors.New("wal payload must be valid JSON")
		}
		id := l.lastID + uint64(i) + 1
		encoded, err := encodeRecord(record{Type: "intent", ID: id, Payload: payload})
		if err != nil {
			return nil, err
		}
		data = append(data, encoded...)
		ids[i] = id
		copies[i] = append(json.RawMessage(nil), payload...)
	}
	if err := l.writeFrames(data); err != nil {
		return nil, err
	}
	for i, id := range ids {
		l.pending[id] = copies[i]
	}
	l.lastID = ids[len(ids)-1]
	return ids, nil
}

// Complete durably acknowledges an intent after its database operation is safe.
func (l *Log) Complete(id uint64) error { return l.CompleteBatch([]uint64{id}) }

// CompleteBatch persists completion markers in one fsync. Repeated IDs and
// previously completed IDs are harmless; an unknown ID rejects the whole batch.
func (l *Log) CompleteBatch(ids []uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writable(); err != nil {
		return err
	}
	seen := make(map[uint64]bool, len(ids))
	var data []byte
	var complete []uint64
	for _, id := range ids {
		if id == 0 || id > l.lastID {
			return errors.New("unknown wal intent ID")
		}
		if _, ok := l.pending[id]; !ok || seen[id] {
			continue
		}
		seen[id] = true
		encoded, err := encodeRecord(record{Type: "complete", ID: id})
		if err != nil {
			return err
		}
		data = append(data, encoded...)
		complete = append(complete, id)
	}
	if len(data) == 0 {
		return nil
	}
	if err := l.writeFrames(data); err != nil {
		return err
	}
	for _, id := range complete {
		delete(l.pending, id)
	}
	return nil
}

// LastID is the greatest successfully appended intent ID, including completed
// intents. After a failed write it must not be treated as a recovery watermark;
// close and reopen the poisoned log to discover its actual durable records.
func (l *Log) LastID() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastID
}

func (l *Log) writable() error {
	if l.closed {
		return ErrClosed
	}
	if l.failed != nil {
		return errors.Join(ErrPoisoned, l.failed)
	}
	return nil
}

func encodeRecord(rec record) ([]byte, error) {
	body, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(frame{Record: body, CRC32: crc32.ChecksumIEEE(body)})
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	if len(data) > MaxFrameSize {
		return nil, fmt.Errorf("wal record exceeds %d bytes", MaxFrameSize)
	}
	return data, nil
}

// writeFrames is called with the mutex held and prevalidated bytes. Exactly one
// Sync follows all writes, regardless of the number of frames in the group.
func (l *Log) writeFrames(data []byte) error {
	for len(data) > 0 {
		n, err := l.file.Write(data)
		if err != nil {
			l.failed = fmt.Errorf("write wal: %w", err)
			return l.failed
		}
		if n == 0 {
			l.failed = io.ErrShortWrite
			return l.failed
		}
		data = data[n:]
	}
	if err := l.file.Sync(); err != nil {
		l.failed = fmt.Errorf("fsync wal: %w", err)
		return l.failed
	}
	return nil
}

// VisitCompleted streams the retained journal's completed intents in append
// order without retaining history or changing the append offset. The callback
// runs under the log mutex and must not call other methods on this Log.
func (l *Log) VisitCompleted(fn func(Entry) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writable(); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("nil completed-intent visitor")
	}
	info, err := l.file.Stat()
	if err != nil {
		return fmt.Errorf("stat wal for completed visit: %w", err)
	}
	reader := bufio.NewReaderSize(io.NewSectionReader(l.file, 0, info.Size()), MaxFrameSize+1)
	var offset int64
	for {
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, io.EOF) && len(line) == 0 {
			return nil
		}
		if err != nil || len(line) > MaxFrameSize {
			return fmt.Errorf("invalid WAL frame during completed visit at byte %d: %w", offset, errors.Join(err, errors.New("incomplete or oversized frame")))
		}
		var f frame
		if err := decodeStrict(line, &f); err != nil {
			return fmt.Errorf("decode wal frame at byte %d: %w", offset, err)
		}
		if crc32.ChecksumIEEE(f.Record) != f.CRC32 {
			return fmt.Errorf("wal checksum mismatch at byte %d", offset)
		}
		var rec record
		if err := decodeStrict(f.Record, &rec); err != nil {
			return fmt.Errorf("decode wal record at byte %d: %w", offset, err)
		}
		switch rec.Type {
		case "intent":
			if rec.ID == 0 || rec.ID > l.lastID || !json.Valid(rec.Payload) {
				return fmt.Errorf("invalid wal intent at byte %d", offset)
			}
			if _, pending := l.pending[rec.ID]; !pending {
				if err := fn(Entry{ID: rec.ID, Payload: append(json.RawMessage(nil), rec.Payload...)}); err != nil {
					return err
				}
			}
		case "complete":
			if rec.ID == 0 || rec.ID > l.lastID || len(rec.Payload) != 0 {
				return fmt.Errorf("invalid wal completion at byte %d", offset)
			}
		default:
			return fmt.Errorf("unknown wal record type at byte %d", offset)
		}
		offset += int64(len(line))
	}
}

// Pending returns copies of unreconciled intents in append order.
func (l *Log) Pending() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	entries := make([]Entry, 0, len(l.pending))
	for id, payload := range l.pending {
		entries = append(entries, Entry{ID: id, Payload: append(json.RawMessage(nil), payload...)})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	return entries
}

// Close releases the process lock. Every successful write is already synced.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return l.file.Close()
}

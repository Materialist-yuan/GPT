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

// Append writes and fsyncs a JSON intent. An I/O error has an uncertain
// durability outcome; the log then refuses further writes until it is reopened.
func (l *Log) Append(payload json.RawMessage) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writable(); err != nil {
		return 0, err
	}
	if !json.Valid(payload) {
		return 0, errors.New("wal payload must be valid JSON")
	}
	if l.lastID == math.MaxUint64 {
		return 0, errors.New("wal ID space exhausted")
	}
	id := l.lastID + 1
	if err := l.writeRecord(record{Type: "intent", ID: id, Payload: payload}); err != nil {
		return 0, err
	}
	l.lastID = id
	l.pending[id] = append(json.RawMessage(nil), payload...)
	return id, nil
}

// Complete durably acknowledges an intent after the database has handled it.
// Completing the same known ID twice is safe; unknown IDs are rejected.
func (l *Log) Complete(id uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writable(); err != nil {
		return err
	}
	if id == 0 || id > l.lastID {
		return errors.New("unknown wal intent ID")
	}
	if _, ok := l.pending[id]; !ok {
		return nil
	}
	if err := l.writeRecord(record{Type: "complete", ID: id}); err != nil {
		return err
	}
	delete(l.pending, id)
	return nil
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

func (l *Log) writeRecord(rec record) error {
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	data, err := json.Marshal(frame{Record: body, CRC32: crc32.ChecksumIEEE(body)})
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > MaxFrameSize {
		return fmt.Errorf("wal record exceeds %d bytes", MaxFrameSize)
	}
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

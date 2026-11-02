// Package actor demonstrates durable single-writer player state with asynchronous checkpoints.
package actor

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/game"
)

var (
	ErrOverloaded = admissionError("actor mailbox or checkpoint backlog is full")
	ErrCapacity   = admissionError("actor player or receipt capacity is full")
	ErrClosed     = errors.New("actor engine is closed")
)

type Receipt struct {
	Fingerprint string      `json:"fingerprint" bson:"fingerprint"`
	Result      game.Result `json:"result" bson:"result"`
}
type Snapshot struct {
	Player      game.Player        `json:"player" bson:"player"`
	Initialized bool               `json:"initialized" bson:"initialized"`
	Receipts    map[string]Receipt `json:"receipts" bson:"receipts"`
	Seq         uint64             `json:"seq" bson:"seq"`
}
type Store interface {
	Load(context.Context, int) ([]Snapshot, error)
	Save(context.Context, int, []Snapshot) error
	Ping(context.Context) error
}
type Options struct {
	Directory                                                                  string
	Shards, QueueCapacity, BatchSize, MaxPlayersPerShard, MaxReceiptsPerPlayer int
	BatchWindow, FlushInterval                                                 time.Duration
	MaxUncheckpointed                                                          uint64
}

func DefaultOptions(dir string) Options {
	return Options{Directory: dir, Shards: 4, QueueCapacity: 256, BatchSize: 64, MaxPlayersPerShard: 10000, MaxReceiptsPerPlayer: 1000, BatchWindow: time.Millisecond, FlushInterval: time.Second, MaxUncheckpointed: 10000}
}

type ShardStats struct {
	ID                      int    `json:"id"`
	Queue                   int    `json:"queue"`
	Players                 int64  `json:"players"`
	Pending                 uint64 `json:"pending"`
	PendingSinceUnixMilli   int64  `json:"pending_since_unix_milli,omitempty"`
	PendingAgeMillis        int64  `json:"pending_age_millis"`
	LastCheckpointUnixMilli int64  `json:"last_checkpoint_unix_milli,omitempty"`
	WALRecords              uint64 `json:"wal_records"`
	WALBatches              uint64 `json:"wal_batches"`
	Checkpoints             uint64 `json:"checkpoints"`
	CheckpointErrors        uint64 `json:"checkpoint_errors"`
	LastCheckpointError     string `json:"last_checkpoint_error,omitempty"`
	Fault                   string `json:"fault,omitempty"`
}
type Stats struct {
	Shards     []ShardStats `json:"shards"`
	Accepted   uint64       `json:"accepted"`
	Overloaded uint64       `json:"overloaded"`
}

type admissionError string

func (e admissionError) Error() string   { return string(e) }
func (e admissionError) HTTPStatus() int { return 429 }

// game.Result excludes HTTP status from the public response JSON. The journal
// must retain it to reproduce both accepted and rejected responses after restart.
func (r Receipt) MarshalJSON() ([]byte, error) {
	type wire struct {
		Fingerprint string      `json:"fingerprint"`
		Result      game.Result `json:"result"`
		Status      int         `json:"status"`
	}
	return json.Marshal(wire{r.Fingerprint, r.Result, r.Result.Status})
}
func (r *Receipt) UnmarshalJSON(data []byte) error {
	var wire struct {
		Fingerprint string      `json:"fingerprint"`
		Result      game.Result `json:"result"`
		Status      int         `json:"status"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.Status < 200 || wire.Status > 599 {
		return errors.New("invalid receipt HTTP status")
	}
	r.Fingerprint = wire.Fingerprint
	r.Result = wire.Result
	r.Result.Status = wire.Status
	return nil
}

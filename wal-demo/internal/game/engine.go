package game

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/wal"
)

type Store interface {
	Apply(context.Context, Operation) (Result, error)
	Get(context.Context, string) (Player, error)
	Ping(context.Context) error
}

// Engine serializes WAL writes and MongoDB projection. On an uncertain write it
// refuses further changes until restart and recovery, preserving WAL order.
type Engine struct {
	mu    sync.Mutex
	log   *wal.Log
	store Store
	fault error
}

func NewEngine(ctx context.Context, log *wal.Log, store Store) (*Engine, error) {
	e := &Engine{log: log, store: store}
	for _, entry := range log.Pending() {
		var op Operation
		if err := json.Unmarshal(entry.Payload, &op); err != nil {
			return nil, fmt.Errorf("decode WAL %d: %w", entry.ID, err)
		}
		if err := Validate(op); err != nil {
			return nil, fmt.Errorf("invalid WAL %d: %w", entry.ID, err)
		}
		if _, err := store.Apply(ctx, op); err != nil {
			return nil, fmt.Errorf("replay WAL %d: %w", entry.ID, err)
		}
		if err := log.Complete(entry.ID); err != nil {
			return nil, fmt.Errorf("complete WAL %d: %w", entry.ID, err)
		}
	}
	return e, nil
}

func (e *Engine) Execute(ctx context.Context, op Operation) (Result, error) {
	if err := Validate(op); err != nil {
		return Result{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fault != nil {
		return Result{}, fmt.Errorf("restart required for WAL recovery: %w", e.fault)
	}
	payload, err := json.Marshal(op)
	if err != nil {
		return Result{}, err
	}
	id, err := e.log.Append(payload)
	if err == nil {
		var result Result
		result, err = e.store.Apply(ctx, op)
		if err == nil {
			err = e.log.Complete(id)
		}
		if err == nil {
			return result, nil
		}
	}
	e.fault = err
	return Result{}, err
}

func (e *Engine) Get(ctx context.Context, id string) (Player, error) { return e.store.Get(ctx, id) }
func (e *Engine) Healthy(ctx context.Context) error {
	e.mu.Lock()
	err := e.fault
	e.mu.Unlock()
	if err != nil {
		return fmt.Errorf("restart required for WAL recovery: %w", err)
	}
	return e.store.Ping(ctx)
}

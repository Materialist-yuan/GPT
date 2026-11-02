// actor-server demonstrates locally durable, sharded game state with asynchronous checkpoints.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/actor"
	"github.com/Materialist-yuan/GPT/wal-demo/internal/httpapi"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	opts, err := configuration()
	if err != nil {
		return err
	}
	addr := setting("HTTP_ADDR", "127.0.0.1:8081")
	token := os.Getenv("APP_API_TOKEN")
	if err := validateListener(addr, token); err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	store, err := actor.OpenMongo(startup, setting("MONGO_URI", "mongodb://127.0.0.1:27017"), setting("MONGO_DATABASE", "game_assets_actor"), opts.Shards)
	if err != nil {
		return err
	}
	defer func() {
		c, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = store.Close(c)
	}()
	engine, err := actor.New(startup, store, opts)
	if err != nil {
		return err
	}
	defer func() {
		c, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		if err := engine.Close(c); err != nil {
			log.Printf("shutdown checkpoint incomplete; retain WAL and recover on restart: %v", err)
		}
	}()
	api := httpapi.New(engine)
	var requests, failures atomic.Uint64
	var nanos atomic.Uint64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("alive\n"))
	})
	mux.HandleFunc("GET /debug/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(engine.Stats())
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "# TYPE demo_http_requests_total counter\ndemo_http_requests_total %d\n# TYPE demo_http_errors_total counter\ndemo_http_errors_total %d\n# TYPE demo_http_request_seconds_total counter\ndemo_http_request_seconds_total %.9f\n# TYPE demo_go_goroutines gauge\ndemo_go_goroutines %d\n", requests.Load(), failures.Load(), float64(nanos.Load())/1e9, runtime.NumGoroutine())
		for _, s := range engine.Stats().Shards {
			fault := 0
			if s.Fault != "" {
				fault = 1
			}
			_, _ = fmt.Fprintf(w, "demo_actor_queue{shard=\"%d\"} %d\ndemo_actor_pending{shard=\"%d\"} %d\ndemo_actor_pending_age_seconds{shard=\"%d\"} %.3f\ndemo_actor_wal_records_total{shard=\"%d\"} %d\ndemo_actor_wal_batches_total{shard=\"%d\"} %d\ndemo_actor_checkpoint_errors_total{shard=\"%d\"} %d\ndemo_actor_checkpoints_total{shard=\"%d\"} %d\ndemo_actor_fault{shard=\"%d\"} %d\n", s.ID, s.Queue, s.ID, s.Pending, s.ID, float64(s.PendingAgeMillis)/1000, s.ID, s.WALRecords, s.ID, s.WALBatches, s.ID, s.CheckpointErrors, s.ID, s.Checkpoints, s.ID, fault)
			_, _ = fmt.Fprintf(w, "demo_actor_wal_segments{shard=\"%d\"} %d\ndemo_actor_wal_bytes{shard=\"%d\"} %d\ndemo_actor_wal_reclaimed_segments_total{shard=\"%d\"} %d\ndemo_actor_wal_reclaimed_bytes_total{shard=\"%d\"} %d\n", s.ID, s.WALSegments, s.ID, s.WALBytes, s.ID, s.WALReclaimedSegments, s.ID, s.WALReclaimedBytes)
		}
	})
	mux.Handle("/", api)
	server := &http.Server{Addr: addr, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			begin := time.Now()
			requests.Add(1)
			defer func() { nanos.Add(uint64(time.Since(begin))) }()
			if r.URL.Path != "/livez" && token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
				failures.Add(1)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			c, done := context.WithTimeout(r.Context(), 5*time.Second)
			defer done()
			tracked := &responseStatus{ResponseWriter: w, status: http.StatusOK}
			mux.ServeHTTP(tracked, r.WithContext(c))
			if tracked.status >= 400 {
				failures.Add(1)
			}
		}),
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("actor API ready: address=%s shards=%d queue_per_shard=%d batch=%d checkpoint=%s", listener.Addr(), opts.Shards, opts.QueueCapacity, opts.BatchSize, opts.FlushInterval)
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	select {
	case err := <-stopped:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		c, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		if err := server.Shutdown(c); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}

type responseStatus struct {
	http.ResponseWriter
	status int
}

func (w *responseStatus) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func validateListener(addr, token string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if token != "" && len(token) < 16 {
		return errors.New("APP_API_TOKEN must have at least 16 characters")
	}
	ip := net.ParseIP(host)
	if (ip == nil || !ip.IsLoopback()) && token == "" {
		return errors.New("non-loopback HTTP_ADDR requires APP_API_TOKEN; use a literal loopback IP for local development")
	}
	return nil
}

func configuration() (actor.Options, error) {
	o := actor.DefaultOptions(setting("WAL_DIRECTORY", "./data/actor"))
	for _, item := range []struct {
		name      string
		target    *int
		low, high int
	}{
		{"ACTOR_SHARDS", &o.Shards, 1, 64}, {"ACTOR_QUEUE", &o.QueueCapacity, 1, 65536},
		{"ACTOR_BATCH", &o.BatchSize, 1, 1024}, {"ACTOR_MAX_PLAYERS", &o.MaxPlayersPerShard, 1, 100000},
		{"ACTOR_MAX_RECEIPTS", &o.MaxReceiptsPerPlayer, 1, 2000},
	} {
		if s := os.Getenv(item.name); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < item.low || n > item.high {
				return o, fmt.Errorf("invalid %s (expected %d..%d)", item.name, item.low, item.high)
			}
			*item.target = n
		}
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
		max    time.Duration
	}{
		{"ACTOR_BATCH_WINDOW", &o.BatchWindow, time.Second}, {"ACTOR_FLUSH_INTERVAL", &o.FlushInterval, 24 * time.Hour},
	} {
		if s := os.Getenv(item.name); s != "" {
			d, err := time.ParseDuration(s)
			if err != nil || d <= 0 || d > item.max {
				return o, fmt.Errorf("invalid %s", item.name)
			}
			*item.target = d
		}
	}
	if s := os.Getenv("ACTOR_MAX_UNCHECKPOINTED"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil || n < 1 || n > 1000000 {
			return o, errors.New("invalid ACTOR_MAX_UNCHECKPOINTED")
		}
		o.MaxUncheckpointed = n
	}
	if s := os.Getenv("ACTOR_WAL_SEGMENT_BYTES"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 1024 || n > 1<<30 {
			return o, errors.New("invalid ACTOR_WAL_SEGMENT_BYTES (expected 1024..1073741824)")
		}
		o.WALSegmentBytes = n
	}
	return o, nil
}

func setting(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

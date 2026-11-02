package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/game"
	"github.com/Materialist-yuan/GPT/wal-demo/internal/httpapi"
	"github.com/Materialist-yuan/GPT/wal-demo/internal/wal"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	store, err := game.OpenMongo(startup, setting("MONGO_URI", "mongodb://127.0.0.1:27017"), setting("MONGO_DATABASE", "game_assets"))
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = store.Close(closeCtx)
	}()
	journal, err := wal.Open(setting("WAL_PATH", "./data/assets.wal"))
	if err != nil {
		return err
	}
	defer journal.Close()
	pending := len(journal.Pending())
	engine, err := game.NewEngine(startup, journal, store)
	if err != nil {
		return err
	}
	log.Printf("WAL recovery complete: %d pending records replayed", pending)
	handler := httpapi.New(engine)
	server := &http.Server{
		Addr: setting("HTTP_ADDR", "127.0.0.1:8080"),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCtx, requestCancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer requestCancel()
			handler.ServeHTTP(w, r.WithContext(requestCtx))
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	stopped := make(chan error, 1)
	go func() { stopped <- server.ListenAndServe() }()
	log.Printf("game assets API listening on %s", server.Addr)
	select {
	case err := <-stopped:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdown); err != nil {
			// Wait for in-flight asset writes before closing WAL/MongoDB.
			_ = server.Close()
			return err
		}
		return nil
	}
}

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

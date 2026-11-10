package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gameplatform/internal/login"
	"gameplatform/internal/storage"
)

func main() {
	if err := run(); err != nil {
		slog.Error("login stopped", "error", err.Error())
		os.Exit(1)
	}
}
func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	cfg := login.Config{JWTSecret: os.Getenv("JWT_SECRET"), GatewayAddress: getenv("GATEWAY_PUBLIC_ADDR", "127.0.0.1:9000"), ServerID: getenv("SERVER_ID", "s1")}
	var err error
	if cfg.PasswordCost, err = envInt("BCRYPT_COST", 12); err != nil {
		return err
	}
	if cfg.AuthLimit, err = envInt("AUTH_RATE_LIMIT", 60); err != nil {
		return err
	}
	if cfg.PasswordConcurrency, err = envInt("BCRYPT_CONCURRENCY", 4); err != nil {
		return err
	}
	if len(cfg.JWTSecret) < 32 {
		return errors.New("JWT_SECRET is required and must contain at least 32 bytes")
	}
	databaseURL, redisURL := os.Getenv("DATABASE_URL"), os.Getenv("REDIS_URL")
	if databaseURL == "" || redisURL == "" {
		return errors.New("DATABASE_URL and REDIS_URL are required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	repo, state, err := storage.Open(startup, databaseURL, redisURL)
	cancel()
	if err != nil {
		return errors.New("storage connection failed; check database and Redis settings")
	}
	defer repo.Close()
	defer state.Close()
	handler, err := login.New(cfg, repo, state, logger)
	if err != nil {
		return err
	}
	server := &http.Server{Addr: getenv("LOGIN_ADDR", ":8080"), Handler: handler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	stopped := make(chan error, 1)
	go func() { stopped <- server.ListenAndServe() }()
	logger.Info("login_listening", "address", server.Addr)
	select {
	case err = <-stopped:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		return err
	}
	return nil
}
func getenv(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
func envInt(name string, fallback int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, nil
}

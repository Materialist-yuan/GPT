package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gameplatform/internal/gateway"
	"gameplatform/internal/storage"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	maxConns, err := positiveInt("GATEWAY_MAX_CONNECTIONS", 10000)
	if err != nil {
		return err
	}
	maxBody, err := positiveInt("GATEWAY_MAX_BODY", 65536)
	if err != nil {
		return err
	}
	rate, err := positiveInt("GATEWAY_MESSAGES_PER_SECOND", 100)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	state, err := storage.NewRedis(ctx, env("REDIS_URL", "redis://127.0.0.1:6379/0"))
	cancel()
	if err != nil {
		// URL parser and dial errors can include credential-bearing connection
		// strings. Keep them out of startup logs, including malformed URLs.
		return errors.New("gateway state connection failed; check REDIS_URL and Redis availability")
	}
	defer state.Close()
	server, err := gateway.New(gateway.Config{ServerID: env("SERVER_ID", "s1"), MaxConnections: maxConns, MaxBody: maxBody, MessagesPerSecond: rate}, state, log)
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", env("GATEWAY_ADDR", ":9000"))
	if err != nil {
		return err
	}
	defer l.Close()
	certFile, keyFile := os.Getenv("GATEWAY_TLS_CERT"), os.Getenv("GATEWAY_TLS_KEY")
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return errors.New("both GATEWAY_TLS_CERT and GATEWAY_TLS_KEY are required")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("load gateway TLS certificate: %w", err)
		}
		l = tls.NewListener(l, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}})
	}
	admin := &http.Server{Addr: env("GATEWAY_ADMIN_ADDR", "127.0.0.1:9091"), Handler: server.MetricsHandler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	errCh := make(chan error, 2)
	go func() { errCh <- server.Serve(l) }()
	go func() {
		err := admin.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()
	log.Info("gateway listening", "tcp_address", l.Addr().String(), "admin_address", admin.Addr, "tls", certFile != "", "server_id", env("SERVER_ID", "s1"))
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var runErr error
	select {
	case <-sigCtx.Done():
	case runErr = <-errCh:
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	gatewayErr := server.Shutdown(shutdownCtx)
	adminErr := admin.Shutdown(shutdownCtx)
	if runErr != nil {
		return runErr
	}
	if gatewayErr != nil {
		return gatewayErr
	}
	return adminErr
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func positiveInt(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

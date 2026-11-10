package main

import (
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestStartupDoesNotExposeMalformedRedisCredentials(t *testing.T) {
	t.Setenv("GATEWAY_MAX_CONNECTIONS", "1")
	t.Setenv("GATEWAY_MAX_BODY", "65536")
	t.Setenv("GATEWAY_MESSAGES_PER_SECOND", "1")
	t.Setenv("REDIS_URL", "redis://user:private-password%zz@localhost:6379")
	err := run(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("expected malformed URL to fail")
	}
	if strings.Contains(err.Error(), "private-password") || strings.Contains(err.Error(), "redis://") {
		t.Fatal("startup error exposed connection credentials")
	}
	if !strings.Contains(err.Error(), "REDIS_URL") {
		t.Fatal("startup error must identify the configuration to check")
	}
}

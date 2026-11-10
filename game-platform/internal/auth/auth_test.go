package auth

import (
	"strings"
	"testing"
	"time"
)

func TestTokenValidation(t *testing.T) {
	s, err := NewSigner(strings.Repeat("s", 32))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	token, err := s.Sign(42, "session", now)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := s.Verify(token, now)
	if err != nil || claims.Subject != "42" {
		t.Fatalf("valid token: %+v %v", claims, err)
	}
	if _, err = s.Verify(token, now.Add(AccessTTL)); err == nil {
		t.Fatal("accepted expired token")
	}
	if _, err = s.Verify(token, now.Add(-time.Second)); err == nil {
		t.Fatal("accepted future token")
	}
	parts := strings.Split(token, ".")
	parts[1] = "eyJzdWIiOiIxIn0"
	if _, err = s.Verify(strings.Join(parts, "."), now); err == nil {
		t.Fatal("accepted forged token")
	}
	other, _ := NewSigner(strings.Repeat("x", 32))
	if _, err = other.Verify(token, now); err == nil {
		t.Fatal("accepted wrong signing key")
	}
	if _, err = NewSigner("short"); err == nil {
		t.Fatal("accepted weak key")
	}
}

func TestPassword(t *testing.T) {
	hash, err := HashPassword("correct-horse-123", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !CheckPassword(hash, "correct-horse-123") || CheckPassword(hash, "wrong") {
		t.Fatal("password verification failed")
	}
}

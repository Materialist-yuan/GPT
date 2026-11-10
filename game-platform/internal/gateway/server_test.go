package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"gameplatform/internal/core"
	"gameplatform/internal/storage"
	"gameplatform/internal/wire"
)

func testServer(t *testing.T) (*Server, *storage.Memory, string) {
	t.Helper()
	m := storage.NewMemory()
	s, err := New(Config{LeaseRefresh: 20 * time.Millisecond, LeaseTTL: time.Second, AuthTimeout: 150 * time.Millisecond, IdleTimeout: time.Second, WriteTimeout: time.Second}, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return s, m, l.Addr().String()
}

func makeTicket(t *testing.T, m *storage.Memory, token, sid string) {
	t.Helper()
	ctx := context.Background()
	if _, err := m.GetSession(ctx, sid); err != nil {
		if err := m.CreateSession(ctx, core.Session{ID: sid, AccountID: 1}, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.PutTicket(ctx, token, core.Grant{AccountID: 1, RoleID: 2, ServerID: "s1", SessionID: sid}, time.Minute); err != nil {
		t.Fatal(err)
	}
}

func connectAuth(t *testing.T, addr, token string) (net.Conn, wire.Frame) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(time.Second))
	body, _ := json.Marshal(map[string]string{"ticket": token})
	if err := wire.WriteFrame(c, wire.Frame{Opcode: wire.Auth, Seq: 1, Body: body}); err != nil {
		t.Fatal(err)
	}
	f, err := wire.ReadFrame(c, wire.DefaultMaxBody)
	if err != nil {
		t.Fatal(err)
	}
	return c, f
}

func TestTicketReplayLeaseAndSessionRevocation(t *testing.T) {
	_, m, addr := testServer(t)
	const ticket = "ticket0123456789abcdef"
	makeTicket(t, m, ticket, "session1")
	c, f := connectAuth(t, addr, ticket)
	if f.Opcode != wire.AuthOK {
		t.Fatalf("auth failed: %s", f.Body)
	}
	_, f = connectAuth(t, addr, ticket)
	if f.Opcode != wire.Error || !bytes.Contains(f.Body, []byte("invalid_ticket")) {
		t.Fatalf("replay accepted: %s", f.Body)
	}
	makeTicket(t, m, "another0123456789abcdef", "session2")
	_, f = connectAuth(t, addr, "another0123456789abcdef")
	if f.Opcode != wire.Error || !bytes.Contains(f.Body, []byte("already_online")) {
		t.Fatalf("duplicate accepted: %s", f.Body)
	}
	want := []byte(`{"hello":"世界"}`)
	if err := wire.WriteFrame(c, wire.Frame{Opcode: wire.Echo, Seq: 2, Body: want}); err != nil {
		t.Fatal(err)
	}
	f, err := wire.ReadFrame(c, wire.DefaultMaxBody)
	if err != nil {
		t.Fatal(err)
	}
	if f.Opcode != wire.EchoOK || f.Seq != 2 || !bytes.Equal(f.Body, want) {
		t.Fatalf("bad echo %+v", f)
	}
	if err := m.DeleteSession(context.Background(), "session1"); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err = wire.ReadFrame(c, wire.DefaultMaxBody); err == nil {
		t.Fatal("revoked session remained connected")
	} else if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		t.Fatal("revoked session not closed")
	}
}

func TestRequiresAuthenticationAndIncreasingSequence(t *testing.T) {
	_, m, addr := testServer(t)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if err := wire.WriteFrame(c, wire.Frame{Opcode: wire.Echo, Seq: 1, Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	f, err := wire.ReadFrame(c, wire.DefaultMaxBody)
	if err != nil {
		t.Fatal(err)
	}
	if f.Opcode != wire.Error {
		t.Fatal("accepted unauthenticated echo")
	}
	makeTicket(t, m, "seqtest0123456789abcdef", "s1")
	a, reply := connectAuth(t, addr, "seqtest0123456789abcdef")
	if reply.Opcode != wire.AuthOK {
		t.Fatalf("auth: %s", reply.Body)
	}
	if err := wire.WriteFrame(a, wire.Frame{Opcode: wire.Ping, Seq: 1, Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	f, err = wire.ReadFrame(a, wire.DefaultMaxBody)
	if err != nil {
		t.Fatal(err)
	}
	if f.Opcode != wire.Error || !bytes.Contains(f.Body, []byte("invalid_message")) {
		t.Fatalf("sequence replay accepted: %s", f.Body)
	}
}

func TestUnauthenticatedConnectionTimesOut(t *testing.T) {
	_, _, addr := testServer(t)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err = c.Read(b[:]); err == nil {
		t.Fatal("expected closure")
	} else if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		t.Fatal("gateway did not expire unauthenticated connection")
	}
}

func TestRejectInvalidServerID(t *testing.T) {
	for _, id := range []string{"bad:server", "server\nname", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {
		if _, err := New(Config{ServerID: id}, storage.NewMemory(), nil); err == nil {
			t.Errorf("accepted invalid server ID %q", id)
		}
	}
}

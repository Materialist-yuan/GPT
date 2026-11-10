package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gameplatform/internal/core"
)

func TestMemoryRefreshRotationHasOneWinner(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	if err := m.CreateSession(ctx, core.Session{ID: "s", AccountID: 42, RefreshHash: "old"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			session, err := m.RotateSession(ctx, "s", "old", fmt.Sprint(i), time.Minute)
			if err == nil {
				winners.Add(1)
				if session.AccountID != 42 {
					t.Errorf("rotation lost account binding: %+v", session)
				}
			} else if !errors.Is(err, core.ErrUnauthorized) {
				t.Errorf("unexpected rotation error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if got := winners.Load(); got != 1 {
		t.Fatalf("rotation winners = %d, want 1", got)
	}
}

func TestMemoryTicketConsumedOnce(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	grant := core.Grant{AccountID: 1, RoleID: 2, ServerID: "s1", SessionID: "s"}
	if err := m.PutTicket(ctx, "ticket", grant, time.Minute); err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int64
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := m.ConsumeTicket(ctx, "ticket")
			if err == nil {
				winners.Add(1)
				if got != grant {
					t.Errorf("grant changed: %+v", got)
				}
			} else if !errors.Is(err, core.ErrUnauthorized) {
				t.Errorf("unexpected consume error: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := winners.Load(); got != 1 {
		t.Fatalf("ticket winners = %d, want 1", got)
	}
}

func TestMemoryRoleLimitAndOwnership(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	account, err := m.CreateAccount(ctx, "alice", "hash")
	if err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := m.CreateRole(ctx, account.ID, "s1", fmt.Sprintf("role%d", i), "warrior")
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, core.ErrLimit) {
				t.Errorf("unexpected role error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if got := winners.Load(); got != 5 {
		t.Fatalf("created %d roles, want 5", got)
	}
	roles, err := m.ListRoles(ctx, account.ID, "s1")
	if err != nil || len(roles) != 5 {
		t.Fatalf("list roles = %+v, %v", roles, err)
	}
	if _, err := m.GetRole(ctx, account.ID+1, roles[0].ID); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("cross-account lookup = %v", err)
	}
	if _, err := m.CreateRole(ctx, account.ID, "s2", "different-server", "warrior"); err != nil {
		t.Fatalf("server-specific role quota: %v", err)
	}
}

func TestMemoryLeaseOwnership(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	assertLease := func(ok bool, err error, want bool) {
		t.Helper()
		if err != nil || ok != want {
			t.Fatalf("lease operation = %v, %v; want %v", ok, err, want)
		}
	}
	ok, err := m.AcquireLease(ctx, "player", "a", time.Minute)
	assertLease(ok, err, true)
	ok, err = m.AcquireLease(ctx, "player", "b", time.Minute)
	assertLease(ok, err, false)
	ok, err = m.RenewLease(ctx, "player", "b", time.Minute)
	assertLease(ok, err, false)
	if err := m.ReleaseLease(ctx, "player", "b"); err != nil {
		t.Fatal(err)
	}
	ok, err = m.AcquireLease(ctx, "player", "b", time.Minute)
	assertLease(ok, err, false)
	if err := m.ReleaseLease(ctx, "player", "a"); err != nil {
		t.Fatal(err)
	}
	ok, err = m.AcquireLease(ctx, "player", "b", time.Minute)
	assertLease(ok, err, true)
	ok, err = m.RenewLease(ctx, "player", "b", time.Minute)
	assertLease(ok, err, true)
}

func TestMemoryExpirationAndRateWindow(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	if err := m.CreateSession(ctx, core.Session{ID: "expired"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := m.PutTicket(ctx, "expired", core.Grant{}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AcquireLease(ctx, "expired", "a", time.Minute); err != nil {
		t.Fatal(err)
	}
	// Move expiration timestamps directly to avoid sleep-based timing assertions.
	m.mu.Lock()
	past := time.Now().Add(-time.Second)
	s := m.sessions["expired"]
	s.expires = past
	m.sessions["expired"] = s
	ticket := m.tickets["expired"]
	ticket.expires = past
	m.tickets["expired"] = ticket
	lease := m.leases["expired"]
	lease.expires = past
	m.leases["expired"] = lease
	m.mu.Unlock()
	if _, err := m.GetSession(ctx, "expired"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expired session = %v", err)
	}
	if _, err := m.ConsumeTicket(ctx, "expired"); !errors.Is(err, core.ErrUnauthorized) {
		t.Fatalf("expired ticket = %v", err)
	}
	if ok, err := m.RenewLease(ctx, "expired", "a", time.Minute); err != nil || ok {
		t.Fatalf("expired lease renewed = %v, %v", ok, err)
	}
	if ok, err := m.AcquireLease(ctx, "expired", "b", time.Minute); err != nil || !ok {
		t.Fatalf("expired lease not acquired = %v, %v", ok, err)
	}
	for i := 0; i < 3; i++ {
		ok, err := m.Allow(ctx, "login", 2, time.Minute)
		if err != nil || ok != (i < 2) {
			t.Fatalf("rate call %d = %v, %v", i, ok, err)
		}
	}
	m.mu.Lock()
	m.windows["login"] = memoryWindow{count: 100, expires: past}
	m.mu.Unlock()
	if ok, err := m.Allow(ctx, "login", 2, time.Minute); err != nil || !ok {
		t.Fatalf("expired rate window = %v, %v", ok, err)
	}
	if ok, err := m.Allow(ctx, "invalid-limit", 0, time.Minute); err != nil || ok {
		t.Fatalf("zero limit = %v, %v", ok, err)
	}
}

func TestMemoryCanceledContext(t *testing.T) {
	m := NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.CreateAccount(ctx, "x", "hash"); !errors.Is(err, context.Canceled) {
		t.Fatalf("create account with canceled context = %v", err)
	}
	if err := m.CreateSession(ctx, core.Session{ID: "s"}, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("create session with canceled context = %v", err)
	}
}

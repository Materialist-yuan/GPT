package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gameplatform/internal/core"
)

func testSuffix(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}

func TestPostgresIntegrationRoleQuota(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to a disposable test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := NewPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err = p.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = p.Migrate(ctx); err != nil {
		t.Fatalf("idempotent migration: %v", err)
	}
	p2, err := NewPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p2.Close() })
	suffix := testSuffix(t)
	a, err := p.CreateAccount(ctx, "test_"+suffix, "test-hash")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = p.pool.Exec(cleanup, "DELETE FROM roles WHERE account_id=$1", a.ID)
		_, _ = p.pool.Exec(cleanup, "DELETE FROM accounts WHERE id=$1", a.ID)
	})
	if _, err = p.CreateAccount(ctx, a.Username, "other-hash"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("duplicate account: %v", err)
	}
	var successes atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			connection := p
			if i%2 == 0 {
				connection = p2
			}
			_, err := connection.CreateRole(ctx, a.ID, "integration", fmt.Sprintf("%s_%d", suffix, i), "warrior")
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, core.ErrLimit) {
				t.Errorf("create role: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if n := successes.Load(); n != 5 {
		t.Fatalf("concurrent role creations=%d, want 5", n)
	}
	roles, err := p.ListRoles(ctx, a.ID, "integration")
	if err != nil || len(roles) != 5 {
		t.Fatalf("roles=%v error=%v", roles, err)
	}
	if _, err = p.GetRole(ctx, a.ID+1, roles[0].ID); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("cross-account role: %v", err)
	}
	other, err := p.CreateRole(ctx, a.ID, "other", roles[0].Name, "warrior")
	if err != nil {
		t.Fatalf("other server quota/name: %v", err)
	}
	if _, err = p.CreateRole(ctx, a.ID, "other", other.Name, "warrior"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("role name uniqueness: %v", err)
	}
}

func TestRedisIntegrationAtomicityAndExpiry(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("set TEST_REDIS_URL to a disposable test Redis")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := NewRedis(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r2, err := NewRedis(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	suffix := testSuffix(t)
	session := core.Session{ID: "test-session-" + suffix, AccountID: (1 << 60) + 1, RefreshHash: "old"}
	if err = r.CreateSession(ctx, session, time.Minute); err != nil {
		t.Fatal(err)
	}
	defer r.DeleteSession(context.Background(), session.ID)
	var rotations atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client := r
			if i%2 == 0 {
				client = r2
			}
			got, err := client.RotateSession(ctx, session.ID, "old", fmt.Sprintf("new-%d", i), time.Minute)
			if err == nil {
				rotations.Add(1)
				if got.AccountID != session.AccountID {
					t.Errorf("lost integer precision in session: %d", got.AccountID)
				}
			} else if !errors.Is(err, core.ErrUnauthorized) {
				t.Errorf("rotate: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if n := rotations.Load(); n != 1 {
		t.Fatalf("refresh rotation winners=%d, want 1", n)
	}
	grant := core.Grant{AccountID: session.AccountID, RoleID: 2, ServerID: "s1", SessionID: session.ID}
	ticket := "test-ticket-" + suffix
	if err = r.PutTicket(ctx, ticket, grant, time.Minute); err != nil {
		t.Fatal(err)
	}
	var consumed atomic.Int64
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client := r
			if i%2 == 0 {
				client = r2
			}
			got, err := client.ConsumeTicket(ctx, ticket)
			if err == nil {
				consumed.Add(1)
				if got != grant {
					t.Errorf("grant changed: %+v", got)
				}
			} else if !errors.Is(err, core.ErrUnauthorized) {
				t.Errorf("consume: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if n := consumed.Load(); n != 1 {
		t.Fatalf("ticket winners=%d, want 1", n)
	}
	lease := "test-lease-" + suffix
	if ok, err := r.AcquireLease(ctx, lease, "old-owner", 50*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire=%v %v", ok, err)
	}
	if ok, err := r2.RenewLease(ctx, lease, "wrong-owner", time.Minute); err != nil || ok {
		t.Fatalf("wrong owner renewed=%v %v", ok, err)
	}
	time.Sleep(100 * time.Millisecond)
	if ok, err := r2.AcquireLease(ctx, lease, "new-owner", time.Minute); err != nil || !ok {
		t.Fatalf("reacquire after expiry=%v %v", ok, err)
	}
	defer r2.ReleaseLease(context.Background(), lease, "new-owner")
	if err = r.ReleaseLease(ctx, lease, "old-owner"); err != nil {
		t.Fatal(err)
	}
	if ok, err := r.AcquireLease(ctx, lease, "third-owner", time.Minute); err != nil || ok {
		t.Fatalf("stale release removed new lease=%v %v", ok, err)
	}
	if ok, err := r2.RenewLease(ctx, lease, "new-owner", time.Minute); err != nil || !ok {
		t.Fatalf("current owner renewal=%v %v", ok, err)
	}
	rate := "test-rate-" + suffix
	for i := 0; i < 3; i++ {
		if ok, err := r.Allow(ctx, rate, 2, 50*time.Millisecond); err != nil || ok != (i < 2) {
			t.Fatalf("rate %d=%v %v", i, ok, err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if ok, err := r2.Allow(ctx, rate, 2, time.Second); err != nil || !ok {
		t.Fatalf("new rate window=%v %v", ok, err)
	}
	expired := "test-expired-ticket-" + suffix
	if err = r.PutTicket(ctx, expired, grant, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err = r.CreateSession(ctx, core.Session{ID: expired, AccountID: 1, RefreshHash: "h"}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err = r2.ConsumeTicket(ctx, expired); !errors.Is(err, core.ErrUnauthorized) {
		t.Fatalf("expired ticket=%v", err)
	}
	if _, err = r2.GetSession(ctx, expired); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expired session=%v", err)
	}
}

package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"gameplatform/internal/core"
)

// Memory is an in-process repository and ephemeral state store for tests and
// explicit local development only. It is not shared across service processes.
type Memory struct {
	mu            sync.Mutex
	nextAccountID int64
	nextRoleID    int64
	accounts      map[string]core.Account
	accountIDs    map[int64]struct{}
	roles         map[int64]core.Role
	sessions      map[string]memorySession
	tickets       map[string]memoryTicket
	leases        map[string]memoryLease
	windows       map[string]memoryWindow
}

type memorySession struct {
	value   core.Session
	expires time.Time
}

type memoryTicket struct {
	value   core.Grant
	expires time.Time
}

type memoryLease struct {
	owner   string
	expires time.Time
}

type memoryWindow struct {
	count   int
	expires time.Time
}

var _ core.Repository = (*Memory)(nil)
var _ core.State = (*Memory)(nil)

func NewMemory() *Memory {
	return &Memory{
		accounts: make(map[string]core.Account), accountIDs: make(map[int64]struct{}),
		roles: make(map[int64]core.Role), sessions: make(map[string]memorySession),
		tickets: make(map[string]memoryTicket), leases: make(map[string]memoryLease),
		windows: make(map[string]memoryWindow),
	}
}

func (m *Memory) Ping(ctx context.Context) error { return ctx.Err() }
func (m *Memory) Close() error                   { return nil }

func (m *Memory) CreateAccount(ctx context.Context, username, passwordHash string) (core.Account, error) {
	if err := ctx.Err(); err != nil {
		return core.Account{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.accounts[username]; exists {
		return core.Account{}, core.ErrConflict
	}
	m.nextAccountID++
	account := core.Account{ID: m.nextAccountID, Username: username, PasswordHash: passwordHash}
	m.accounts[username] = account
	m.accountIDs[account.ID] = struct{}{}
	return account, nil
}

func (m *Memory) AccountByName(ctx context.Context, username string) (core.Account, error) {
	if err := ctx.Err(); err != nil {
		return core.Account{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	account, ok := m.accounts[username]
	if !ok {
		return core.Account{}, core.ErrNotFound
	}
	return account, nil
}

func (m *Memory) CreateRole(ctx context.Context, accountID int64, serverID, name, class string) (core.Role, error) {
	if err := ctx.Err(); err != nil {
		return core.Role{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.accountIDs[accountID]; !exists {
		return core.Role{}, core.ErrNotFound
	}
	count := 0
	for _, role := range m.roles {
		if role.ServerID == serverID && role.Name == name {
			return core.Role{}, core.ErrConflict
		}
		if role.AccountID == accountID && role.ServerID == serverID {
			count++
		}
	}
	if count >= 5 {
		return core.Role{}, core.ErrLimit
	}
	m.nextRoleID++
	role := core.Role{ID: m.nextRoleID, AccountID: accountID, ServerID: serverID, Name: name, Class: class, CreatedAt: time.Now().UTC()}
	m.roles[role.ID] = role
	return role, nil
}

func (m *Memory) ListRoles(ctx context.Context, accountID int64, serverID string) ([]core.Role, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	roles := make([]core.Role, 0)
	for _, role := range m.roles {
		if role.AccountID == accountID && role.ServerID == serverID {
			roles = append(roles, role)
		}
	}
	sort.Slice(roles, func(i, j int) bool { return roles[i].ID < roles[j].ID })
	return roles, nil
}

func (m *Memory) GetRole(ctx context.Context, accountID, roleID int64) (core.Role, error) {
	if err := ctx.Err(); err != nil {
		return core.Role{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	role, ok := m.roles[roleID]
	if !ok || role.AccountID != accountID {
		return core.Role{}, core.ErrNotFound
	}
	return role, nil
}

func (m *Memory) CreateSession(ctx context.Context, session core.Session, ttl time.Duration) error {
	if err := validMemoryTTL(ctx, ttl); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.sessions[session.ID]; ok && time.Now().Before(old.expires) {
		return core.ErrConflict
	}
	m.sessions[session.ID] = memorySession{value: session, expires: time.Now().Add(ttl)}
	return nil
}

func (m *Memory) GetSession(ctx context.Context, id string) (core.Session, error) {
	if err := ctx.Err(); err != nil {
		return core.Session{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	session, ok := m.sessions[id]
	if !ok || !time.Now().Before(session.expires) {
		delete(m.sessions, id)
		return core.Session{}, core.ErrNotFound
	}
	return session.value, nil
}

func (m *Memory) RotateSession(ctx context.Context, id, oldHash, newHash string, ttl time.Duration) (core.Session, error) {
	if err := validMemoryTTL(ctx, ttl); err != nil {
		return core.Session{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	session, ok := m.sessions[id]
	if !ok || !time.Now().Before(session.expires) {
		delete(m.sessions, id)
		return core.Session{}, core.ErrUnauthorized
	}
	if session.value.RefreshHash != oldHash {
		return core.Session{}, core.ErrUnauthorized
	}
	session.value.RefreshHash = newHash
	session.expires = time.Now().Add(ttl)
	m.sessions[id] = session
	return session.value, nil
}

func (m *Memory) DeleteSession(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, id)
	return nil
}

func (m *Memory) PutTicket(ctx context.Context, token string, grant core.Grant, ttl time.Duration) error {
	if err := validMemoryTTL(ctx, ttl); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, exists := m.tickets[token]; exists && time.Now().Before(old.expires) {
		return core.ErrConflict
	}
	m.tickets[token] = memoryTicket{value: grant, expires: time.Now().Add(ttl)}
	return nil
}

func (m *Memory) ConsumeTicket(ctx context.Context, token string) (core.Grant, error) {
	if err := ctx.Err(); err != nil {
		return core.Grant{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ticket, exists := m.tickets[token]
	delete(m.tickets, token)
	if !exists || !time.Now().Before(ticket.expires) {
		return core.Grant{}, core.ErrUnauthorized
	}
	return ticket.value, nil
}

func (m *Memory) AcquireLease(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	if err := validMemoryTTL(ctx, ttl); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if lease, exists := m.leases[key]; exists && time.Now().Before(lease.expires) {
		return false, nil
	}
	m.leases[key] = memoryLease{owner: owner, expires: time.Now().Add(ttl)}
	return true, nil
}

func (m *Memory) RenewLease(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	if err := validMemoryTTL(ctx, ttl); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	lease, exists := m.leases[key]
	if !exists || !time.Now().Before(lease.expires) || lease.owner != owner {
		return false, nil
	}
	lease.expires = time.Now().Add(ttl)
	m.leases[key] = lease
	return true, nil
}

func (m *Memory) ReleaseLease(ctx context.Context, key, owner string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if lease, exists := m.leases[key]; exists && lease.owner == owner {
		delete(m.leases, key)
	}
	return nil
}

func (m *Memory) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	if err := validMemoryTTL(ctx, window); err != nil {
		return false, err
	}
	if limit <= 0 {
		return false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.windows[key]
	if !exists || !time.Now().Before(entry.expires) {
		entry = memoryWindow{expires: time.Now().Add(window)}
	}
	entry.count++
	m.windows[key] = entry
	return entry.count <= limit, nil
}

func validMemoryTTL(ctx context.Context, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		return fmt.Errorf("TTL must be positive")
	}
	return nil
}

package storage

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	"gameplatform/internal/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Postgres struct{ pool *pgxpool.Pool }

func NewPostgres(ctx context.Context, databaseURL string) (*Postgres, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		// Parser errors may embed the entire URL, including its password.
		return nil, errors.New("invalid database configuration")
	}
	config.MaxConns = 16
	config.MinConns = 0
	config.MaxConnIdleTime = 5 * time.Minute
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("database pool: %w", err)
	}
	p := &Postgres{pool: pool}
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := p.Ping(probe); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database ping: %w", err)
	}
	return p, nil
}

func Open(ctx context.Context, databaseURL, redisURL string) (*Postgres, *Redis, error) {
	p, err := NewPostgres(ctx, databaseURL)
	if err != nil {
		return nil, nil, err
	}
	r, err := NewRedis(ctx, redisURL)
	if err != nil {
		_ = p.Close()
		return nil, nil, err
	}
	return p, r, nil
}

func (p *Postgres) Close() error                   { p.pool.Close(); return nil }
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

// Migrate is an explicit startup/deployment step. The transaction-scoped advisory
// lock serializes concurrent migrators; each migration and its version commit
// together. Service processes never run migrations implicitly.
func (p *Postgres) Migrate(ctx context.Context) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(730214002)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		return err
	}
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return fmt.Errorf("invalid migration name %q", entry.Name())
		}
		version, err := strconv.Atoi(prefix)
		if err != nil {
			return err
		}
		var applied bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)", version).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		sql, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", entry.Name(), err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES($1)", version); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func dbError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return core.ErrNotFound
	}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		switch pgerr.Code {
		case "23505":
			return core.ErrConflict
		case "23503":
			return core.ErrNotFound
		}
	}
	return err
}

func (p *Postgres) CreateAccount(ctx context.Context, username, hash string) (core.Account, error) {
	a := core.Account{}
	err := p.pool.QueryRow(ctx, "INSERT INTO accounts(username,password_hash) VALUES($1,$2) RETURNING id,username,password_hash", username, hash).Scan(&a.ID, &a.Username, &a.PasswordHash)
	return a, dbError(err)
}

func (p *Postgres) AccountByName(ctx context.Context, username string) (core.Account, error) {
	a := core.Account{}
	err := p.pool.QueryRow(ctx, "SELECT id,username,password_hash FROM accounts WHERE username=$1", username).Scan(&a.ID, &a.Username, &a.PasswordHash)
	return a, dbError(err)
}

func (p *Postgres) CreateRole(ctx context.Context, accountID int64, serverID, name, class string) (core.Role, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return core.Role{}, err
	}
	defer tx.Rollback(ctx)
	// The account row lock serializes count+insert even across service replicas.
	var lockedID int64
	if err = tx.QueryRow(ctx, "SELECT id FROM accounts WHERE id=$1 FOR UPDATE", accountID).Scan(&lockedID); err != nil {
		return core.Role{}, dbError(err)
	}
	var count int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM roles WHERE account_id=$1 AND server_id=$2", accountID, serverID).Scan(&count); err != nil {
		return core.Role{}, err
	}
	if count >= 5 {
		return core.Role{}, core.ErrLimit
	}
	r := core.Role{}
	err = tx.QueryRow(ctx, "INSERT INTO roles(account_id,server_id,name,class) VALUES($1,$2,$3,$4) RETURNING id,account_id,server_id,name,class,created_at", accountID, serverID, name, class).Scan(&r.ID, &r.AccountID, &r.ServerID, &r.Name, &r.Class, &r.CreatedAt)
	if err != nil {
		return core.Role{}, dbError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return core.Role{}, err
	}
	return r, nil
}

func (p *Postgres) ListRoles(ctx context.Context, accountID int64, serverID string) ([]core.Role, error) {
	rows, err := p.pool.Query(ctx, "SELECT id,account_id,server_id,name,class,created_at FROM roles WHERE account_id=$1 AND server_id=$2 ORDER BY id", accountID, serverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	roles := make([]core.Role, 0)
	for rows.Next() {
		var r core.Role
		if err = rows.Scan(&r.ID, &r.AccountID, &r.ServerID, &r.Name, &r.Class, &r.CreatedAt); err != nil {
			return nil, err
		}
		roles = append(roles, r)
	}
	return roles, rows.Err()
}

func (p *Postgres) GetRole(ctx context.Context, accountID, roleID int64) (core.Role, error) {
	r := core.Role{}
	err := p.pool.QueryRow(ctx, "SELECT id,account_id,server_id,name,class,created_at FROM roles WHERE id=$1 AND account_id=$2", roleID, accountID).Scan(&r.ID, &r.AccountID, &r.ServerID, &r.Name, &r.Class, &r.CreatedAt)
	return r, dbError(err)
}

var _ core.Repository = (*Postgres)(nil)

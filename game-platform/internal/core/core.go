package core

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
	ErrUnauthorized = errors.New("unauthorized")
	ErrLimit        = errors.New("limit reached")
)

type Account struct {
	ID           int64  `json:"id"`
	Username     string `json:"username"`
	PasswordHash string `json:"-"`
}

type Role struct {
	ID        int64     `json:"id"`
	AccountID int64     `json:"-"`
	ServerID  string    `json:"server_id"`
	Name      string    `json:"name"`
	Class     string    `json:"class"`
	CreatedAt time.Time `json:"created_at"`
}

type Server struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Status  string `json:"status"`
	Gateway string `json:"gateway"`
}

type Session struct {
	ID          string `json:"id"`
	AccountID   int64  `json:"account_id"`
	RefreshHash string `json:"refresh_hash"`
}

type Grant struct {
	AccountID int64  `json:"account_id"`
	RoleID    int64  `json:"role_id"`
	ServerID  string `json:"server_id"`
	SessionID string `json:"session_id"`
}

type Repository interface {
	CreateAccount(context.Context, string, string) (Account, error)
	AccountByName(context.Context, string) (Account, error)
	CreateRole(context.Context, int64, string, string, string) (Role, error)
	ListRoles(context.Context, int64, string) ([]Role, error)
	GetRole(context.Context, int64, int64) (Role, error)
	Ping(context.Context) error
}

type State interface {
	CreateSession(context.Context, Session, time.Duration) error
	GetSession(context.Context, string) (Session, error)
	RotateSession(context.Context, string, string, string, time.Duration) (Session, error)
	DeleteSession(context.Context, string) error
	PutTicket(context.Context, string, Grant, time.Duration) error
	ConsumeTicket(context.Context, string) (Grant, error)
	AcquireLease(context.Context, string, string, time.Duration) (bool, error)
	RenewLease(context.Context, string, string, time.Duration) (bool, error)
	ReleaseLease(context.Context, string, string) error
	Allow(context.Context, string, int, time.Duration) (bool, error)
	Ping(context.Context) error
}

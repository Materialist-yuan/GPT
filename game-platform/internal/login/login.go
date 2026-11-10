// Package login exposes account, session, role, and one-use gateway ticket APIs.
package login

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gameplatform/internal/auth"
	"gameplatform/internal/core"
)

type Config struct {
	JWTSecret           string
	GatewayAddress      string
	ServerID            string
	PasswordCost        int
	AuthLimit           int
	PasswordConcurrency int
}

type service struct {
	cfg       Config
	repo      core.Repository
	state     core.State
	signer    *auth.Signer
	logger    *slog.Logger
	dummyHash string
	hashSlots chan struct{}
	requests  atomic.Uint64
	failures  atomic.Uint64
}

type contextKey int

const (
	requestKey contextKey = iota
	sessionKey
)

var usernamePattern = regexp.MustCompile(`^[a-z0-9_]{3,32}$`)
var roleNamePattern = regexp.MustCompile(`^[\p{L}\p{N}_]{2,24}$`)
var serverPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,32}$`)

func New(cfg Config, repo core.Repository, state core.State, logger *slog.Logger) (http.Handler, error) {
	if repo == nil || state == nil {
		return nil, errors.New("repository and state required")
	}
	if cfg.PasswordCost == 0 {
		cfg.PasswordCost = 12
	}
	if cfg.PasswordCost < 10 || cfg.PasswordCost > 14 {
		return nil, errors.New("password cost must be 10..14")
	}
	if cfg.AuthLimit == 0 {
		cfg.AuthLimit = 60
	}
	if cfg.AuthLimit < 1 {
		return nil, errors.New("auth limit must be positive")
	}
	if cfg.PasswordConcurrency == 0 {
		cfg.PasswordConcurrency = 4
	}
	if cfg.PasswordConcurrency < 1 {
		return nil, errors.New("password concurrency must be positive")
	}
	if cfg.ServerID == "" {
		cfg.ServerID = "s1"
	}
	if !serverPattern.MatchString(cfg.ServerID) {
		return nil, errors.New("invalid server id")
	}
	if cfg.GatewayAddress == "" {
		cfg.GatewayAddress = "127.0.0.1:9000"
	}
	_, port, err := net.SplitHostPort(cfg.GatewayAddress)
	if err != nil {
		return nil, errors.New("gateway address must be host:port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, errors.New("gateway port must be 1..65535")
	}
	signer, err := auth.NewSigner(cfg.JWTSecret)
	if err != nil {
		return nil, err
	}
	dummyHash, err := auth.HashPassword("dummy-password-never-used", cfg.PasswordCost)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	s := &service{cfg: cfg, repo: repo, state: state, signer: signer, logger: logger, dummyHash: dummyHash, hashSlots: make(chan struct{}, cfg.PasswordConcurrency)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { s.json(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.Handle("POST /v1/accounts", s.rate(http.HandlerFunc(s.register), cfg.AuthLimit, "auth"))
	mux.Handle("POST /v1/sessions", s.rate(http.HandlerFunc(s.createSession), cfg.AuthLimit, "auth"))
	mux.Handle("POST /v1/sessions/refresh", s.rate(http.HandlerFunc(s.refresh), cfg.AuthLimit, "auth"))
	mux.Handle("DELETE /v1/sessions", s.protect(http.HandlerFunc(s.logout)))
	mux.Handle("GET /v1/servers", s.protect(http.HandlerFunc(s.servers)))
	mux.Handle("GET /v1/roles", s.protect(http.HandlerFunc(s.listRoles)))
	mux.Handle("POST /v1/roles", s.protect(http.HandlerFunc(s.createRole)))
	mux.Handle("POST /v1/tickets", s.protect(http.HandlerFunc(s.ticket)))
	return s.middleware(mux), nil
}

func (s *service) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		id, err := auth.RandomToken()
		if err != nil {
			http.Error(w, "internal error", 500)
			return
		}
		w.Header().Set("X-Request-ID", id)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), requestKey, id), 5*time.Second)
		defer cancel()
		r = r.WithContext(ctx)
		r.Body = http.MaxBytesReader(w, r.Body, 8192)
		recorder := &statusWriter{ResponseWriter: w, status: 200}
		defer func() {
			if recover() != nil {
				s.fail(recorder, r, 500, "internal_error", "internal error")
				s.logger.Error("request panic", "request_id", id)
			}
			s.requests.Add(1)
			if recorder.status >= 400 {
				s.failures.Add(1)
			}
			// Log matched route patterns only: arbitrary URL paths and tokens never enter logs.
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			s.logger.Info("http_request", "request_id", id, "method", r.Method, "route", route, "status", recorder.status, "duration_ms", time.Since(started).Milliseconds())
		}()
		next.ServeHTTP(recorder, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
		w.ResponseWriter.WriteHeader(code)
	}
}
func (w *statusWriter) Write(data []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(data)
}

func (s *service) json(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}
func (s *service) fail(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	s.json(w, status, map[string]any{"error": map[string]any{"code": code, "message": message, "request_id": r.Context().Value(requestKey)}})
}
func (s *service) decode(w http.ResponseWriter, r *http.Request, out any) bool {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		s.fail(w, r, 415, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		s.fail(w, r, 400, "invalid_request", "invalid JSON request")
		return false
	}
	if err := d.Decode(new(any)); err != io.EOF {
		s.fail(w, r, 400, "invalid_request", "request must contain one JSON value")
		return false
	}
	return true
}

func (s *service) rate(next http.Handler, limit int, namespace string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		if host == "" {
			host = "unknown"
		}
		allowed, err := s.state.Allow(r.Context(), namespace+":"+auth.HashToken(host), limit, time.Minute)
		if err != nil {
			s.fail(w, r, 503, "unavailable", "state service unavailable")
			return
		}
		if !allowed {
			w.Header().Set("Retry-After", "60")
			s.fail(w, r, 429, "rate_limited", "too many requests")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *service) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer := r.Header.Get("Authorization")
		if !strings.HasPrefix(bearer, "Bearer ") {
			s.fail(w, r, 401, "unauthorized", "valid access token required")
			return
		}
		claims, err := s.signer.Verify(strings.TrimPrefix(bearer, "Bearer "), time.Now())
		if err != nil {
			s.fail(w, r, 401, "unauthorized", "valid access token required")
			return
		}
		session, err := s.state.GetSession(r.Context(), claims.SessionID)
		if errors.Is(err, core.ErrNotFound) || errors.Is(err, core.ErrUnauthorized) {
			s.fail(w, r, 401, "unauthorized", "session expired")
			return
		}
		if err != nil {
			s.fail(w, r, 503, "unavailable", "state service unavailable")
			return
		}
		if strconv.FormatInt(session.AccountID, 10) != claims.Subject {
			s.fail(w, r, 401, "unauthorized", "invalid session")
			return
		}
		allowed, err := s.state.Allow(r.Context(), "account:"+claims.Subject, 600, time.Minute)
		if err != nil {
			s.fail(w, r, 503, "unavailable", "state service unavailable")
			return
		}
		if !allowed {
			w.Header().Set("Retry-After", "60")
			s.fail(w, r, 429, "rate_limited", "too many requests")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), sessionKey, session)))
	})
}

func currentSession(r *http.Request) core.Session {
	return r.Context().Value(sessionKey).(core.Session)
}
func (s *service) hashSlot(w http.ResponseWriter, r *http.Request) bool {
	select {
	case s.hashSlots <- struct{}{}:
		return true
	default:
		w.Header().Set("Retry-After", "1")
		s.fail(w, r, 503, "busy", "password service busy; retry later")
		return false
	}
}

func (s *service) register(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !s.decode(w, r, &in) {
		return
	}
	in.Username = strings.ToLower(strings.TrimSpace(in.Username))
	if !usernamePattern.MatchString(in.Username) || len(in.Password) < 12 || len(in.Password) > 72 {
		s.fail(w, r, 400, "invalid_request", "username must be 3..32 ASCII letters/digits/underscore; password must be 12..72 bytes")
		return
	}
	if !s.hashSlot(w, r) {
		return
	}
	defer func() { <-s.hashSlots }()
	hash, err := auth.HashPassword(in.Password, s.cfg.PasswordCost)
	if err != nil {
		s.fail(w, r, 500, "internal_error", "password hash failed")
		return
	}
	account, err := s.repo.CreateAccount(r.Context(), in.Username, hash)
	if errors.Is(err, core.ErrConflict) {
		s.fail(w, r, 409, "conflict", "username unavailable")
		return
	}
	if err != nil {
		s.fail(w, r, 503, "unavailable", "account service unavailable")
		return
	}
	s.json(w, 201, account)
}

func (s *service) createSession(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !s.decode(w, r, &in) {
		return
	}
	in.Username = strings.ToLower(strings.TrimSpace(in.Username))
	allowed, err := s.state.Allow(r.Context(), "username:"+auth.HashToken(in.Username), 10, time.Minute)
	if err != nil {
		s.fail(w, r, 503, "unavailable", "state service unavailable")
		return
	}
	if !allowed {
		w.Header().Set("Retry-After", "60")
		s.fail(w, r, 429, "rate_limited", "too many login attempts")
		return
	}
	if !s.hashSlot(w, r) {
		return
	}
	defer func() { <-s.hashSlots }()
	account, lookupErr := s.repo.AccountByName(r.Context(), in.Username)
	hash := s.dummyHash
	if lookupErr == nil {
		hash = account.PasswordHash
	}
	valid := auth.CheckPassword(hash, in.Password)
	if lookupErr != nil && !errors.Is(lookupErr, core.ErrNotFound) {
		s.fail(w, r, 503, "unavailable", "account service unavailable")
		return
	}
	// bcrypt compares only its supported input range. Enforce the same byte
	// bounds as registration so a suffix beyond 72 bytes can never authenticate.
	if !valid || lookupErr != nil || !usernamePattern.MatchString(in.Username) || len(in.Password) < 12 || len(in.Password) > 72 {
		s.fail(w, r, 401, "unauthorized", "invalid username or password")
		return
	}
	sid, err := auth.RandomToken()
	if err != nil {
		s.fail(w, r, 500, "internal_error", "session creation failed")
		return
	}
	refresh, err := auth.RandomToken()
	if err != nil {
		s.fail(w, r, 500, "internal_error", "session creation failed")
		return
	}
	refresh = sid + "." + refresh
	access, err := s.signer.Sign(account.ID, sid, time.Now())
	if err != nil {
		s.fail(w, r, 500, "internal_error", "session creation failed")
		return
	}
	session := core.Session{ID: sid, AccountID: account.ID, RefreshHash: auth.HashToken(refresh)}
	if err = s.state.CreateSession(r.Context(), session, auth.RefreshTTL); err != nil {
		s.fail(w, r, 503, "unavailable", "state service unavailable")
		return
	}
	s.json(w, 200, map[string]any{"access_token": access, "refresh_token": refresh, "expires_in": int(auth.AccessTTL / time.Second), "token_type": "Bearer"})
}

func (s *service) refresh(w http.ResponseWriter, r *http.Request) {
	var in struct {
		RefreshToken string `json:"refresh_token"`
	}
	if !s.decode(w, r, &in) {
		return
	}
	parts := strings.Split(in.RefreshToken, ".")
	if len(parts) != 2 || len(parts[0]) != 43 || len(parts[1]) != 43 {
		s.fail(w, r, 401, "unauthorized", "invalid refresh token")
		return
	}
	suffix, err := auth.RandomToken()
	if err != nil {
		s.fail(w, r, 500, "internal_error", "token creation failed")
		return
	}
	next := parts[0] + "." + suffix
	session, err := s.state.RotateSession(r.Context(), parts[0], auth.HashToken(in.RefreshToken), auth.HashToken(next), auth.RefreshTTL)
	if errors.Is(err, core.ErrNotFound) || errors.Is(err, core.ErrUnauthorized) {
		s.fail(w, r, 401, "unauthorized", "invalid refresh token")
		return
	}
	if err != nil {
		s.fail(w, r, 503, "unavailable", "state service unavailable")
		return
	}
	access, err := s.signer.Sign(session.AccountID, session.ID, time.Now())
	if err != nil {
		s.fail(w, r, 500, "internal_error", "token creation failed")
		return
	}
	s.json(w, 200, map[string]any{"access_token": access, "refresh_token": next, "expires_in": int(auth.AccessTTL / time.Second), "token_type": "Bearer"})
}

func (s *service) logout(w http.ResponseWriter, r *http.Request) {
	if err := s.state.DeleteSession(r.Context(), currentSession(r).ID); err != nil {
		s.fail(w, r, 503, "unavailable", "state service unavailable")
		return
	}
	w.WriteHeader(204)
}

func (s *service) servers(w http.ResponseWriter, r *http.Request) {
	s.json(w, 200, map[string]any{"servers": []core.Server{{ID: s.cfg.ServerID, Name: "Game Server 1", Status: "online", Gateway: s.cfg.GatewayAddress}}})
}
func (s *service) listRoles(w http.ResponseWriter, r *http.Request) {
	server := r.URL.Query().Get("server_id")
	if server == "" {
		server = s.cfg.ServerID
	}
	if server != s.cfg.ServerID {
		s.fail(w, r, 400, "invalid_server", "unknown server")
		return
	}
	roles, err := s.repo.ListRoles(r.Context(), currentSession(r).AccountID, server)
	if err != nil {
		s.fail(w, r, 503, "unavailable", "role service unavailable")
		return
	}
	if roles == nil {
		roles = []core.Role{}
	}
	s.json(w, 200, map[string]any{"roles": roles})
}

func (s *service) createRole(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ServerID string `json:"server_id"`
		Name     string `json:"name"`
		Class    string `json:"class"`
	}
	if !s.decode(w, r, &in) {
		return
	}
	if in.ServerID != s.cfg.ServerID || !roleNamePattern.MatchString(in.Name) || (in.Class != "warrior" && in.Class != "mage" && in.Class != "ranger") {
		s.fail(w, r, 400, "invalid_request", "invalid server, role name (2..24 letters/digits/underscore), or class (warrior/mage/ranger)")
		return
	}
	role, err := s.repo.CreateRole(r.Context(), currentSession(r).AccountID, in.ServerID, in.Name, in.Class)
	if errors.Is(err, core.ErrConflict) {
		s.fail(w, r, 409, "conflict", "role name unavailable")
		return
	}
	if errors.Is(err, core.ErrLimit) {
		s.fail(w, r, 409, "role_limit", "role limit reached")
		return
	}
	if err != nil {
		s.fail(w, r, 503, "unavailable", "role service unavailable")
		return
	}
	s.json(w, 201, role)
}

func (s *service) ticket(w http.ResponseWriter, r *http.Request) {
	var in struct {
		RoleID int64 `json:"role_id"`
	}
	if !s.decode(w, r, &in) {
		return
	}
	if in.RoleID <= 0 {
		s.fail(w, r, 400, "invalid_request", "positive role_id required")
		return
	}
	session := currentSession(r)
	role, err := s.repo.GetRole(r.Context(), session.AccountID, in.RoleID)
	if errors.Is(err, core.ErrNotFound) {
		s.fail(w, r, 404, "not_found", "role not found")
		return
	}
	if err != nil {
		s.fail(w, r, 503, "unavailable", "role service unavailable")
		return
	}
	if role.ServerID != s.cfg.ServerID {
		s.fail(w, r, 400, "invalid_server", "unknown server")
		return
	}
	ticket, err := auth.RandomToken()
	if err != nil {
		s.fail(w, r, 500, "internal_error", "ticket creation failed")
		return
	}
	grant := core.Grant{AccountID: session.AccountID, RoleID: role.ID, ServerID: role.ServerID, SessionID: session.ID}
	if err = s.state.PutTicket(r.Context(), ticket, grant, auth.TicketTTL); err != nil {
		s.fail(w, r, 503, "unavailable", "state service unavailable")
		return
	}
	s.json(w, 200, map[string]any{"ticket": ticket, "expires_in": int(auth.TicketTTL / time.Second), "gateway": s.cfg.GatewayAddress})
}

func (s *service) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if s.repo.Ping(ctx) != nil || s.state.Ping(ctx) != nil {
		s.fail(w, r, 503, "unavailable", "dependencies unavailable")
		return
	}
	s.json(w, 200, map[string]string{"status": "ready"})
}

func (s *service) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# TYPE game_login_requests_total counter\ngame_login_requests_total %d\n# TYPE game_login_errors_total counter\ngame_login_errors_total %d\n", s.requests.Load(), s.failures.Load())
}

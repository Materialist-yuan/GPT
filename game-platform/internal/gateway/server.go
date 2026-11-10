package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"gameplatform/internal/core"
	"gameplatform/internal/wire"
)

type Config struct {
	ServerID          string
	MaxBody           int
	MaxConnections    int
	MessagesPerSecond int
	AuthTimeout       time.Duration
	IdleTimeout       time.Duration
	WriteTimeout      time.Duration
	LeaseTTL          time.Duration
	LeaseRefresh      time.Duration
}

var serverIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,32}$`)

type counters struct {
	accepted, active, rejected, authenticated, authFailed atomic.Int64
	messagesIn, messagesOut, bytesIn, bytesOut            atomic.Int64
	protocolErrors, rateLimited, leaseFailed              atomic.Int64
}

type Server struct {
	cfg      Config
	state    core.State
	log      *slog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	serving  bool
	draining bool
	wg       sync.WaitGroup
	metrics  counters
}

func New(cfg Config, state core.State, logger *slog.Logger) (*Server, error) {
	if state == nil {
		return nil, errors.New("gateway state is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.ServerID == "" {
		cfg.ServerID = "s1"
	}
	if !serverIDPattern.MatchString(cfg.ServerID) {
		return nil, errors.New("invalid server id")
	}
	if cfg.MaxBody == 0 {
		cfg.MaxBody = wire.DefaultMaxBody
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 10000
	}
	if cfg.MessagesPerSecond == 0 {
		cfg.MessagesPerSecond = 100
	}
	if cfg.AuthTimeout == 0 {
		cfg.AuthTimeout = 5 * time.Second
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 60 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = 15 * time.Second
	}
	if cfg.LeaseRefresh == 0 {
		cfg.LeaseRefresh = 5 * time.Second
	}
	if cfg.MaxBody < 1 || cfg.MaxBody > 16*1024*1024 || cfg.MaxConnections < 1 || cfg.MessagesPerSecond < 1 || cfg.AuthTimeout <= 0 || cfg.IdleTimeout <= 0 || cfg.WriteTimeout <= 0 || cfg.LeaseRefresh <= 0 || cfg.LeaseTTL < 2*cfg.LeaseRefresh {
		return nil, errors.New("invalid gateway limits or timeouts")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{cfg: cfg, state: state, log: logger, ctx: ctx, cancel: cancel, conns: make(map[net.Conn]struct{})}, nil
}

// Serve accepts at most MaxConnections handlers, each with one bounded message
// buffer and synchronous writes. There is no unbounded outbound queue.
func (s *Server) Serve(l net.Listener) error {
	s.mu.Lock()
	if s.listener != nil || s.draining {
		s.mu.Unlock()
		return errors.New("gateway already started or shutting down")
	}
	s.listener = l
	s.serving = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.serving = false
		s.mu.Unlock()
	}()
	for {
		c, err := l.Accept()
		if err != nil {
			s.mu.Lock()
			draining := s.draining
			s.mu.Unlock()
			if draining || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if tcp, ok := c.(*net.TCPConn); ok {
			_ = tcp.SetNoDelay(true)
			_ = tcp.SetKeepAlive(true)
			_ = tcp.SetKeepAlivePeriod(30 * time.Second)
		}
		s.mu.Lock()
		if s.draining || len(s.conns) >= s.cfg.MaxConnections {
			s.mu.Unlock()
			s.metrics.rejected.Add(1)
			_ = c.Close()
			continue
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.metrics.accepted.Add(1)
		s.metrics.active.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			defer func() {
				_ = c.Close()
				s.mu.Lock()
				delete(s.conns, c)
				s.mu.Unlock()
				s.metrics.active.Add(-1)
			}()
			s.serveConn(c)
		}()
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	s.cancel()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) serveConn(c net.Conn) {
	_ = c.SetReadDeadline(time.Now().Add(s.cfg.AuthTimeout))
	f, err := s.read(c)
	if err != nil {
		return
	}
	var auth struct {
		Ticket string `json:"ticket"`
	}
	dec := json.NewDecoder(bytes.NewReader(f.Body))
	dec.DisallowUnknownFields()
	if f.Opcode != wire.Auth || f.Seq == 0 || dec.Decode(&auth) != nil || len(auth.Ticket) < 16 || len(auth.Ticket) > 256 || dec.Decode(&struct{}{}) != io.EOF {
		s.metrics.authFailed.Add(1)
		s.fail(c, f.Seq, "invalid_auth", "first message must contain a ticket")
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	grant, err := s.state.ConsumeTicket(ctx, auth.Ticket)
	cancel()
	if err != nil {
		s.metrics.authFailed.Add(1)
		s.fail(c, f.Seq, "invalid_ticket", "ticket invalid, expired or unavailable")
		return
	}
	ctx, cancel = context.WithTimeout(s.ctx, 2*time.Second)
	session, err := s.state.GetSession(ctx, grant.SessionID)
	cancel()
	if err != nil || session.AccountID != grant.AccountID || grant.AccountID <= 0 || grant.RoleID <= 0 || grant.ServerID != s.cfg.ServerID {
		s.metrics.authFailed.Add(1)
		s.fail(c, f.Seq, "invalid_session", "session is no longer valid")
		return
	}
	var ownerBytes [16]byte
	if _, err := rand.Read(ownerBytes[:]); err != nil {
		return
	}
	owner := hex.EncodeToString(ownerBytes[:])
	leaseKey := fmt.Sprintf("%s:%d", grant.ServerID, grant.AccountID)
	ctx, cancel = context.WithTimeout(s.ctx, 2*time.Second)
	acquired, err := s.state.AcquireLease(ctx, leaseKey, owner, s.cfg.LeaseTTL)
	cancel()
	if err != nil || !acquired {
		s.metrics.authFailed.Add(1)
		s.fail(c, f.Seq, "already_online", "account is already online or state is unavailable")
		return
	}
	connCtx, connCancel := context.WithCancel(s.ctx)
	monitorDone := make(chan struct{})
	go func() { defer close(monitorDone); s.monitor(connCtx, c, grant, leaseKey, owner) }()
	defer func() {
		connCancel()
		_ = c.Close()
		<-monitorDone
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.state.ReleaseLease(ctx, leaseKey, owner)
	}()
	if err := s.send(c, wire.Frame{Opcode: wire.AuthOK, Seq: f.Seq, Body: mustJSON(map[string]any{"account_id": grant.AccountID, "role_id": grant.RoleID, "server_id": grant.ServerID})}); err != nil {
		return
	}
	s.metrics.authenticated.Add(1)
	lastSeq := f.Seq
	windowStart, messages := time.Now(), 0
	for {
		_ = c.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		f, err = s.read(c)
		if err != nil {
			return
		}
		if f.Seq <= lastSeq || !json.Valid(f.Body) {
			s.metrics.protocolErrors.Add(1)
			s.fail(c, f.Seq, "invalid_message", "sequence must increase and body must be JSON")
			return
		}
		lastSeq = f.Seq
		if time.Since(windowStart) >= time.Second {
			windowStart, messages = time.Now(), 0
		}
		messages++
		if messages > s.cfg.MessagesPerSecond {
			s.metrics.rateLimited.Add(1)
			s.fail(c, f.Seq, "rate_limited", "message rate exceeded")
			return
		}
		response := wire.Frame{Seq: f.Seq}
		switch f.Opcode {
		case wire.Ping:
			response.Opcode, response.Body = wire.Pong, mustJSON(map[string]any{"server_time": time.Now().UnixMilli()})
		case wire.Echo:
			response.Opcode, response.Body = wire.EchoOK, f.Body
		default:
			s.metrics.protocolErrors.Add(1)
			s.fail(c, f.Seq, "unknown_opcode", "unsupported message type")
			return
		}
		if err := s.send(c, response); err != nil {
			return
		}
	}
}

func (s *Server) monitor(ctx context.Context, c net.Conn, grant core.Grant, key, owner string) {
	tick := time.NewTicker(s.cfg.LeaseRefresh)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			checkCtx, cancel := context.WithTimeout(ctx, min(2*time.Second, s.cfg.LeaseRefresh))
			session, err := s.state.GetSession(checkCtx, grant.SessionID)
			if err == nil && session.AccountID == grant.AccountID {
				var renewed bool
				renewed, err = s.state.RenewLease(checkCtx, key, owner, s.cfg.LeaseTTL)
				if !renewed && err == nil {
					err = core.ErrUnauthorized
				}
			} else if err == nil {
				err = core.ErrUnauthorized
			}
			cancel()
			if err != nil {
				s.metrics.leaseFailed.Add(1)
				_ = c.Close()
				return
			}
		}
	}
}

func (s *Server) read(c net.Conn) (wire.Frame, error) {
	f, err := wire.ReadFrame(c, s.cfg.MaxBody)
	if err == nil {
		s.metrics.messagesIn.Add(1)
		s.metrics.bytesIn.Add(int64(wire.HeaderSize + len(f.Body)))
	} else if errors.Is(err, wire.ErrInvalidFrame) {
		s.metrics.protocolErrors.Add(1)
	}
	return f, err
}

func (s *Server) send(c net.Conn, f wire.Frame) error {
	_ = c.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	if err := wire.WriteFrame(c, f); err != nil {
		return err
	}
	s.metrics.messagesOut.Add(1)
	s.metrics.bytesOut.Add(int64(wire.HeaderSize + len(f.Body)))
	return nil
}

func (s *Server) fail(c net.Conn, seq uint64, code, message string) {
	_ = s.send(c, wire.Frame{Opcode: wire.Error, Seq: seq, Body: mustJSON(map[string]string{"code": code, "message": message})})
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

// MetricsHandler belongs on a private administration listener.
func (s *Server) MetricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		ready := s.serving && !s.draining
		s.mu.Unlock()
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if !ready || s.state.Ping(ctx) != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		values := []struct {
			name, kind string
			value      int64
		}{
			{"connections_total", "counter", s.metrics.accepted.Load()}, {"connections_active", "gauge", s.metrics.active.Load()},
			{"connections_rejected_total", "counter", s.metrics.rejected.Load()}, {"auth_success_total", "counter", s.metrics.authenticated.Load()},
			{"auth_failure_total", "counter", s.metrics.authFailed.Load()}, {"messages_in_total", "counter", s.metrics.messagesIn.Load()},
			{"messages_out_total", "counter", s.metrics.messagesOut.Load()}, {"bytes_in_total", "counter", s.metrics.bytesIn.Load()},
			{"bytes_out_total", "counter", s.metrics.bytesOut.Load()}, {"protocol_errors_total", "counter", s.metrics.protocolErrors.Load()},
			{"rate_limited_total", "counter", s.metrics.rateLimited.Load()}, {"lease_failures_total", "counter", s.metrics.leaseFailed.Load()},
		}
		for _, value := range values {
			_, _ = fmt.Fprintf(w, "# TYPE game_gateway_%s %s\ngame_gateway_%s %d\n", value.name, value.kind, value.name, value.value)
		}
	})
	return mux
}

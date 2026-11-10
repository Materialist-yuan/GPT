package loadtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gameplatform/internal/wire"
)

type Config struct {
	LoginURL           string
	Gateway            string
	ServerID           string
	Bots               int
	Duration           time.Duration
	Ramp               time.Duration
	Rate               float64
	PayloadBytes       int
	PrepareConcurrency int
	Prefix             string
	Timeout            time.Duration
	TLS                bool
	TLSCA              string
	TLSServerName      string
}

func DefaultConfig() Config {
	return Config{LoginURL: "http://127.0.0.1:8080", Gateway: "127.0.0.1:9000", ServerID: "s1", Bots: 50,
		Duration: 30 * time.Second, Ramp: 5 * time.Second, Rate: 2, PayloadBytes: 128,
		PrepareConcurrency: 4, Timeout: 5 * time.Second}
}

var prefixRE = regexp.MustCompile(`^[a-z0-9_]{1,20}$`)

func (c Config) Validate() error {
	u, err := url.Parse(c.LoginURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("login-url must be an http(s) origin without credentials, path, query, or fragment")
	}
	if c.Gateway != "" {
		if _, _, err := net.SplitHostPort(c.Gateway); err != nil {
			return errors.New("gateway must be host:port, or empty to use the ticket response")
		}
	}
	if c.Bots < 1 || c.Bots > 100000 {
		return errors.New("bots must be between 1 and 100000")
	}
	if c.Duration < time.Second || c.Duration > 24*time.Hour || c.Ramp < 0 || c.Ramp >= c.Duration || c.Ramp > 10*time.Minute {
		return errors.New("duration must be 1s..24h; ramp must be nonnegative, below duration and at most 10m")
	}
	if math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) || c.Rate < 0.001 || c.Rate > 1000 {
		return errors.New("rate must be 0.001..1000 messages per bot per second")
	}
	if c.PayloadBytes < 0 || c.PayloadBytes > wire.DefaultMaxBody-32 {
		return errors.New("payload-bytes must be 0..65504 (JSON wrapper adds 11 bytes)")
	}
	if c.PrepareConcurrency < 1 || c.PrepareConcurrency > 64 {
		return errors.New("prepare-concurrency must be 1..64")
	}
	if c.Timeout < 100*time.Millisecond || c.Timeout > 30*time.Second {
		return errors.New("timeout must be 100ms..30s")
	}
	if c.Prefix != "" && !prefixRE.MatchString(c.Prefix) {
		return errors.New("prefix must be 1..20 lowercase letters, digits or underscores")
	}
	if c.ServerID == "" || len(c.ServerID) > 64 {
		return errors.New("server-id must be 1..64 bytes")
	}
	if !c.TLS && (c.TLSCA != "" || c.TLSServerName != "") {
		return errors.New("tls-ca and tls-server-name require -tls")
	}
	return nil
}

type Parameters struct {
	LoginURL     string  `json:"login_url"`
	Gateway      string  `json:"gateway_override"`
	ServerID     string  `json:"server_id"`
	Prefix       string  `json:"account_prefix"`
	Bots         int     `json:"bots"`
	DurationSecs float64 `json:"duration_seconds"`
	RampSecs     float64 `json:"ramp_seconds"`
	Rate         float64 `json:"messages_per_bot_per_second"`
	PayloadBytes int     `json:"payload_data_bytes"`
	TLS          bool    `json:"tls"`
}

type Counters struct {
	ConnectionAttempts int64 `json:"connection_attempts"`
	ConnectionsOK      int64 `json:"connections_succeeded"`
	ConnectionsFailed  int64 `json:"connections_failed"`
	AuthOK             int64 `json:"authentications_succeeded"`
	AuthFailed         int64 `json:"authentications_failed"`
	SessionRefreshOK   int64 `json:"session_refreshes_succeeded"`
	SessionRefreshFail int64 `json:"session_refreshes_failed"`
	TicketFailed       int64 `json:"ticket_requests_failed"`
	PeakOnline         int64 `json:"peak_online"`
	MessagesSent       int64 `json:"messages_attempted"`
	MessagesOK         int64 `json:"messages_succeeded"`
	MessagesFailed     int64 `json:"messages_failed"`
	MessagesCanceled   int64 `json:"messages_canceled_at_end"`
	Timeouts           int64 `json:"timeouts"`
	BytesSent          int64 `json:"tcp_bytes_sent"`
	BytesReceived      int64 `json:"tcp_bytes_received"`
	BotsCanceled       int64 `json:"bots_canceled_before_authentication"`
}

type ErrorSample struct {
	Stage string `json:"stage"`
	Error string `json:"error"`
}

type Report struct {
	SchemaVersion      int           `json:"schema_version"`
	StartedAt          time.Time     `json:"started_at"`
	EndedAt            time.Time     `json:"ended_at"`
	Parameters         Parameters    `json:"parameters"`
	PreparationSeconds float64       `json:"preparation_seconds"`
	Prepared           int64         `json:"prepared_accounts"`
	PreparationFailed  int64         `json:"preparation_failures"`
	LoadSeconds        float64       `json:"load_seconds"`
	Interrupted        bool          `json:"interrupted"`
	Passed             bool          `json:"passed"`
	Counters           Counters      `json:"counters"`
	MessagesPerSecond  float64       `json:"achieved_messages_per_second"`
	MessageErrorRate   float64       `json:"message_error_rate"`
	ConnectLatency     Latency       `json:"connect_latency"`
	AuthLatency        Latency       `json:"auth_latency"`
	MessageLatency     Latency       `json:"message_latency"`
	Errors             []ErrorSample `json:"error_samples"`
}

type counters struct {
	connectionAttempts atomic.Int64
	connectionsOK      atomic.Int64
	connectionsFailed  atomic.Int64
	authOK             atomic.Int64
	authFailed         atomic.Int64
	sessionRefreshOK   atomic.Int64
	sessionRefreshFail atomic.Int64
	ticketFailed       atomic.Int64
	online             atomic.Int64
	peakOnline         atomic.Int64
	messagesSent       atomic.Int64
	messagesOK         atomic.Int64
	messagesFailed     atomic.Int64
	messagesCanceled   atomic.Int64
	timeouts           atomic.Int64
	bytesSent          atomic.Int64
	bytesReceived      atomic.Int64
	botsCanceled       atomic.Int64
}

func (c *counters) snapshot() Counters {
	return Counters{
		ConnectionAttempts: c.connectionAttempts.Load(), ConnectionsOK: c.connectionsOK.Load(), ConnectionsFailed: c.connectionsFailed.Load(),
		AuthOK: c.authOK.Load(), AuthFailed: c.authFailed.Load(), SessionRefreshOK: c.sessionRefreshOK.Load(), SessionRefreshFail: c.sessionRefreshFail.Load(),
		TicketFailed: c.ticketFailed.Load(), PeakOnline: c.peakOnline.Load(), MessagesSent: c.messagesSent.Load(), MessagesOK: c.messagesOK.Load(),
		MessagesFailed: c.messagesFailed.Load(), MessagesCanceled: c.messagesCanceled.Load(), Timeouts: c.timeouts.Load(),
		BytesSent: c.bytesSent.Load(), BytesReceived: c.bytesReceived.Load(), BotsCanceled: c.botsCanceled.Load(),
	}
}

type runner struct {
	cfg      Config
	http     *http.Client
	counts   counters
	connect  *histogram
	auth     *histogram
	message  *histogram
	errorsMu sync.Mutex
	errors   []ErrorSample
	tls      *tls.Config
}

type prepared struct {
	access        string
	refresh       string
	accessExpires time.Time
	roleID        int64
}

type tokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

// Run prepares accounts before starting the timed gateway workload. Each bot has
// one connection, one in-flight request and no automatic reconnect or retry.
func Run(ctx context.Context, cfg Config, progress io.Writer) (Report, error) {
	if err := cfg.Validate(); err != nil {
		return Report{}, err
	}
	if cfg.Prefix == "" {
		buf := make([]byte, 6)
		if _, err := rand.Read(buf); err != nil {
			return Report{}, err
		}
		cfg.Prefix = "b" + hex.EncodeToString(buf)
	}
	var tlsConfig *tls.Config
	if cfg.TLS {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS13, ServerName: cfg.TLSServerName}
		if cfg.TLSCA != "" {
			pem, err := os.ReadFile(cfg.TLSCA)
			if err != nil {
				return Report{}, errors.New("cannot read TLS CA certificate file")
			}
			pool, err := x509.SystemCertPool()
			if err != nil {
				pool = x509.NewCertPool()
			}
			if !pool.AppendCertsFromPEM(pem) {
				return Report{}, errors.New("TLS CA file contains no valid PEM certificate")
			}
			tlsConfig.RootCAs = pool
		}
	}
	transport := &http.Transport{MaxIdleConns: cfg.PrepareConcurrency + 64, MaxIdleConnsPerHost: cfg.PrepareConcurrency + 64,
		MaxConnsPerHost: 128, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: cfg.Timeout, Proxy: http.ProxyFromEnvironment}
	r := &runner{cfg: cfg, http: &http.Client{Transport: transport, Timeout: cfg.Timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return errors.New("redirects are not accepted") }},
		connect: newHistogram(), auth: newHistogram(), message: newHistogram(), errors: []ErrorSample{}, tls: tlsConfig}
	defer transport.CloseIdleConnections()
	report := Report{SchemaVersion: 1, StartedAt: time.Now().UTC(), Parameters: Parameters{
		LoginURL: cfg.LoginURL, Gateway: cfg.Gateway, ServerID: cfg.ServerID, Prefix: cfg.Prefix, Bots: cfg.Bots,
		DurationSecs: cfg.Duration.Seconds(), RampSecs: cfg.Ramp.Seconds(), Rate: cfg.Rate, PayloadBytes: cfg.PayloadBytes, TLS: cfg.TLS}}
	if progress == nil {
		progress = io.Discard
	}
	fmt.Fprintf(progress, "preparing %d accounts (concurrency=%d, prefix=%s)\n", cfg.Bots, cfg.PrepareConcurrency, cfg.Prefix)
	prepStart := time.Now()
	accounts, failed := r.prepare(ctx, progress)
	report.PreparationSeconds = time.Since(prepStart).Seconds()
	report.Prepared, report.PreparationFailed = int64(len(accounts)), failed
	if len(accounts) > 0 && ctx.Err() == nil {
		loadStart := time.Now()
		loadCtx, cancel := context.WithTimeout(ctx, cfg.Duration)
		fmt.Fprintf(progress, "gateway workload: prepared=%d duration=%s ramp=%s rate=%.2f/bot/s\n", len(accounts), cfg.Duration, cfg.Ramp, cfg.Rate)
		var wg sync.WaitGroup
		progressDone := make(chan struct{})
		go func() {
			tick := time.NewTicker(5 * time.Second)
			defer tick.Stop()
			defer close(progressDone)
			for {
				select {
				case <-loadCtx.Done():
					return
				case <-tick.C:
					s := r.counts.snapshot()
					fmt.Fprintf(progress, "elapsed=%.1fs online=%d authenticated=%d messages=%d errors=%d timeouts=%d\n", time.Since(loadStart).Seconds(), r.counts.online.Load(), s.AuthOK, s.MessagesOK, s.MessagesFailed+s.AuthFailed+s.ConnectionsFailed+s.TicketFailed+s.SessionRefreshFail, s.Timeouts)
				}
			}
		}()
		for i, account := range accounts {
			wg.Add(1)
			go func(i int, a prepared) {
				defer wg.Done()
				var delay time.Duration
				if len(accounts) > 1 {
					delay = time.Duration(int64(cfg.Ramp) * int64(i) / int64(len(accounts)-1))
				}
				if !wait(loadCtx, delay) {
					r.counts.botsCanceled.Add(1)
					return
				}
				r.bot(loadCtx, a)
			}(i, account)
		}
		wg.Wait()
		cancel()
		<-progressDone
		report.LoadSeconds = time.Since(loadStart).Seconds()
	}
	report.EndedAt, report.Interrupted = time.Now().UTC(), ctx.Err() != nil
	report.Counters = r.counts.snapshot()
	report.ConnectLatency, report.AuthLatency, report.MessageLatency = r.connect.snapshot(), r.auth.snapshot(), r.message.snapshot()
	report.Errors = r.errors
	if report.LoadSeconds > 0 {
		report.MessagesPerSecond = float64(report.Counters.MessagesOK) / report.LoadSeconds
	}
	completed := report.Counters.MessagesOK + report.Counters.MessagesFailed
	if completed > 0 {
		report.MessageErrorRate = float64(report.Counters.MessagesFailed) / float64(completed)
	}
	report.Passed = !report.Interrupted && report.PreparationFailed == 0 && report.Counters.AuthOK == int64(cfg.Bots) && report.Counters.MessagesOK > 0 &&
		report.Counters.ConnectionsFailed+report.Counters.AuthFailed+report.Counters.TicketFailed+report.Counters.MessagesFailed+report.Counters.SessionRefreshFail == 0
	if !report.Passed {
		return report, errors.New("load run failed or was interrupted; inspect report counters and error_samples")
	}
	return report, nil
}

func wait(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *runner) sample(stage string, err error) {
	r.errorsMu.Lock()
	defer r.errorsMu.Unlock()
	if len(r.errors) < 20 {
		r.errors = append(r.errors, ErrorSample{Stage: stage, Error: err.Error()})
	}
}

func (r *runner) timeout(err error) {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		r.counts.timeouts.Add(1)
	}
}

func (r *runner) prepare(ctx context.Context, progress io.Writer) ([]prepared, int64) {
	jobs := make(chan int)
	var wg sync.WaitGroup
	var preparedMu sync.Mutex
	var failed atomic.Int64
	accounts := make([]prepared, 0, r.cfg.Bots)
	var done atomic.Int64
	for i := 0; i < r.cfg.PrepareConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				a, err := r.prepareOne(ctx, n)
				if err != nil {
					failed.Add(1)
					r.sample("preparation", err)
				} else {
					preparedMu.Lock()
					accounts = append(accounts, a)
					preparedMu.Unlock()
				}
				done.Add(1)
			}
		}()
	}
	progressDone := make(chan struct{})
	progressStopped := make(chan struct{})
	go func() {
		defer close(progressStopped)
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-tick.C:
				fmt.Fprintf(progress, "preparation: completed=%d/%d failed=%d\n", done.Load(), r.cfg.Bots, failed.Load())
			}
		}
	}()
	for i := 0; i < r.cfg.Bots; i++ {
		if ctx.Err() != nil {
			break
		}
		select {
		case jobs <- i:
		case <-ctx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	close(progressDone)
	<-progressStopped
	return accounts, failed.Load()
}

func (r *runner) prepareOne(ctx context.Context, n int) (prepared, error) {
	username := fmt.Sprintf("%s_%06d", r.cfg.Prefix, n)
	secret := make([]byte, 24)
	if _, err := rand.Read(secret); err != nil {
		return prepared{}, errors.New("password generation failed")
	}
	password := hex.EncodeToString(secret)
	account := map[string]string{"username": username, "password": password}
	if err := r.request(ctx, http.MethodPost, "/v1/accounts", "", account, http.StatusCreated, nil); err != nil {
		return prepared{}, err
	}
	var session tokenPair
	if err := r.request(ctx, http.MethodPost, "/v1/sessions", "", account, http.StatusOK, &session); err != nil {
		return prepared{}, err
	}
	if session.AccessToken == "" || session.RefreshToken == "" || session.ExpiresIn < 1 || session.ExpiresIn > 24*60*60 {
		return prepared{}, errors.New("session response has invalid token pair or expiry")
	}
	accessExpires := time.Now().Add(time.Duration(session.ExpiresIn) * time.Second)
	var roles struct {
		Roles []struct {
			ID int64 `json:"id"`
		} `json:"roles"`
	}
	if err := r.request(ctx, http.MethodGet, "/v1/roles?server_id="+url.QueryEscape(r.cfg.ServerID), session.AccessToken, nil, http.StatusOK, &roles); err != nil {
		return prepared{}, err
	}
	var roleID int64
	if len(roles.Roles) > 0 {
		roleID = roles.Roles[0].ID
	} else {
		var role struct {
			ID int64 `json:"id"`
		}
		// Role names are globally unique within the game server, max 24 runes.
		name := username
		if len(name) > 24 {
			name = "r" + hex.EncodeToString(secret[:8])
		}
		body := map[string]string{"server_id": r.cfg.ServerID, "name": name, "class": "warrior"}
		if err := r.request(ctx, http.MethodPost, "/v1/roles", session.AccessToken, body, http.StatusCreated, &role); err != nil {
			return prepared{}, err
		}
		roleID = role.ID
	}
	if roleID <= 0 {
		return prepared{}, errors.New("invalid role id in response")
	}
	return prepared{access: session.AccessToken, refresh: session.RefreshToken, accessExpires: accessExpires, roleID: roleID}, nil
}

// refreshAccess rotates only an expired or soon-expiring access token. Long
// provisioning batches therefore do not invalidate the earliest prepared bots.
func (r *runner) refreshAccess(ctx context.Context, account prepared) (prepared, error) {
	if time.Until(account.accessExpires) > 30*time.Second {
		return account, nil
	}
	var pair tokenPair
	err := r.request(ctx, http.MethodPost, "/v1/sessions/refresh", "", map[string]string{"refresh_token": account.refresh}, http.StatusOK, &pair)
	if err == nil && (pair.AccessToken == "" || pair.RefreshToken == "" || pair.ExpiresIn < 1 || pair.ExpiresIn > 24*60*60) {
		err = errors.New("refresh response has invalid token pair or expiry")
	}
	if err != nil {
		if ctx.Err() == nil {
			r.counts.sessionRefreshFail.Add(1)
		}
		return account, err
	}
	account.access, account.refresh = pair.AccessToken, pair.RefreshToken
	account.accessExpires = time.Now().Add(time.Duration(pair.ExpiresIn) * time.Second)
	r.counts.sessionRefreshOK.Add(1)
	return account, nil
}

func (r *runner) request(ctx context.Context, method, path, token string, body any, status int, result any) error {
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			return errors.New("request encoding failed")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(r.cfg.LoginURL, "/")+path, bytes.NewReader(encoded))
	if err != nil {
		return errors.New("invalid HTTP request")
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		r.timeout(err)
		// Do not include a full URL or request in samples: they may contain secrets.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("HTTP %s failed: %s", method, classifyNetworkError(err))
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 128*1024+1))
	if err != nil || len(data) > 128*1024 {
		return errors.New("HTTP response read failed or exceeded 128KiB")
	}
	if resp.StatusCode != status {
		return fmt.Errorf("HTTP %s %s returned %d (expected %d)", method, strings.Split(path, "?")[0], resp.StatusCode, status)
	}
	if result != nil {
		if err := json.Unmarshal(data, result); err != nil {
			return errors.New("invalid HTTP JSON response")
		}
	}
	return nil
}

func classifyNetworkError(err error) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return "connection or protocol error"
}

type countingConn struct {
	net.Conn
	counts *counters
}

func (c countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.counts.bytesReceived.Add(int64(n))
	return n, err
}

func (c countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.counts.bytesSent.Add(int64(n))
	return n, err
}

func (r *runner) bot(ctx context.Context, account prepared) {
	var err error
	account, err = r.refreshAccess(ctx, account)
	if err != nil {
		if ctx.Err() != nil {
			r.counts.botsCanceled.Add(1)
		} else {
			r.sample("session_refresh", err)
		}
		return
	}
	var ticket struct {
		Ticket  string `json:"ticket"`
		Gateway string `json:"gateway"`
	}
	if err := r.request(ctx, http.MethodPost, "/v1/tickets", account.access, map[string]int64{"role_id": account.roleID}, http.StatusOK, &ticket); err != nil {
		if ctx.Err() != nil {
			r.counts.botsCanceled.Add(1)
			return
		}
		r.counts.ticketFailed.Add(1)
		r.sample("ticket", err)
		return
	}
	if ticket.Ticket == "" {
		r.counts.ticketFailed.Add(1)
		r.sample("ticket", errors.New("ticket response has no ticket"))
		return
	}
	address := ticket.Gateway
	if r.cfg.Gateway != "" {
		address = r.cfg.Gateway
	}
	r.counts.connectionAttempts.Add(1)
	start := time.Now()
	dialer := &net.Dialer{Timeout: r.cfg.Timeout}
	var conn net.Conn
	if r.tls != nil {
		conn, err = (&tls.Dialer{NetDialer: dialer, Config: r.tls}).DialContext(ctx, "tcp", address)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", address)
	}
	if err != nil {
		if ctx.Err() != nil {
			r.counts.botsCanceled.Add(1)
			return
		}
		r.counts.connectionsFailed.Add(1)
		r.timeout(err)
		r.sample("connect", errors.New(classifyNetworkError(err)))
		return
	}
	r.connect.record(time.Since(start))
	r.counts.connectionsOK.Add(1)
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	counted := countingConn{Conn: conn, counts: &r.counts}
	body, _ := json.Marshal(map[string]string{"ticket": ticket.Ticket})
	start = time.Now()
	err = r.exchange(ctx, counted, wire.Frame{Opcode: wire.Auth, Seq: 1, Body: body}, wire.AuthOK, func(frame wire.Frame) error {
		var grant struct {
			RoleID int64 `json:"role_id"`
		}
		if json.Unmarshal(frame.Body, &grant) != nil || grant.RoleID != account.roleID {
			return errors.New("authentication response role mismatch")
		}
		return nil
	})
	if err != nil {
		if ctx.Err() != nil {
			r.counts.botsCanceled.Add(1)
			return
		}
		r.counts.authFailed.Add(1)
		r.timeout(err)
		r.sample("authentication", err)
		return
	}
	r.auth.record(time.Since(start))
	r.counts.authOK.Add(1)
	online := r.counts.online.Add(1)
	defer r.counts.online.Add(-1)
	for old := r.counts.peakOnline.Load(); online > old; old = r.counts.peakOnline.Load() {
		if r.counts.peakOnline.CompareAndSwap(old, online) {
			break
		}
	}
	echo, _ := json.Marshal(map[string]string{"data": strings.Repeat("x", r.cfg.PayloadBytes)})
	interval := time.Duration(float64(time.Second) / r.cfg.Rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	seq := uint64(1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		seq++
		frame := wire.Frame{Opcode: wire.Echo, Seq: seq, Body: echo}
		expected := wire.EchoOK
		if (seq-1)%10 == 0 {
			frame.Opcode, frame.Body, expected = wire.Ping, []byte(`{}`), wire.Pong
		}
		r.counts.messagesSent.Add(1)
		start = time.Now()
		err := r.exchange(ctx, counted, frame, expected, func(got wire.Frame) error {
			if expected == wire.EchoOK && !bytes.Equal(got.Body, echo) {
				return errors.New("echo response payload mismatch")
			}
			if expected == wire.Pong {
				var pong struct {
					ServerTime int64 `json:"server_time"`
				}
				if json.Unmarshal(got.Body, &pong) != nil || pong.ServerTime <= 0 {
					return errors.New("invalid heartbeat response")
				}
			}
			return nil
		})
		if err != nil {
			if ctx.Err() != nil {
				r.counts.messagesCanceled.Add(1)
				return
			}
			r.counts.messagesFailed.Add(1)
			r.timeout(err)
			r.sample("message", err)
			return
		}
		r.counts.messagesOK.Add(1)
		r.message.record(time.Since(start))
	}
}

func (r *runner) exchange(ctx context.Context, conn net.Conn, frame wire.Frame, expected uint8, validate func(wire.Frame) error) error {
	deadline := time.Now().Add(r.cfg.Timeout)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return errors.New("cannot set socket deadline")
	}
	if err := wire.WriteFrame(conn, frame); err != nil {
		return fmt.Errorf("TCP write: %w", safeNetworkError(err))
	}
	got, err := wire.ReadFrame(conn, wire.DefaultMaxBody)
	if err != nil {
		return fmt.Errorf("TCP read: %w", safeNetworkError(err))
	}
	if got.Seq != frame.Seq {
		return errors.New("response sequence mismatch")
	}
	if got.Opcode == wire.Error {
		var data struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(got.Body, &data) == nil && regexp.MustCompile(`^[a-zA-Z0-9_]{1,64}$`).MatchString(data.Code) {
			return fmt.Errorf("gateway rejected request: %s", data.Code)
		}
		return errors.New("gateway rejected request")
	}
	if got.Opcode != expected {
		return errors.New("response opcode mismatch")
	}
	if expected == wire.AuthOK {
		var grant struct {
			AccountID int64  `json:"account_id"`
			RoleID    int64  `json:"role_id"`
			ServerID  string `json:"server_id"`
		}
		if json.Unmarshal(got.Body, &grant) != nil || grant.AccountID <= 0 || grant.RoleID <= 0 || grant.ServerID != r.cfg.ServerID {
			return errors.New("invalid authentication response")
		}
	}
	if validate != nil {
		return validate(got)
	}
	return nil
}

type networkTimeout struct{}

func (networkTimeout) Error() string   { return "timeout" }
func (networkTimeout) Timeout() bool   { return true }
func (networkTimeout) Temporary() bool { return true }

func safeNetworkError(err error) error {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return networkTimeout{}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.New("connection closed")
	}
	return errors.New("connection or protocol error")
}

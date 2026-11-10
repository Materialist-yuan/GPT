package loadtest

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gameplatform/internal/wire"
)

func TestHistogramPercentilesAndConcurrentRecording(t *testing.T) {
	h := newHistogram()
	var wg sync.WaitGroup
	for j := 0; j < 4; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1; i <= 1000; i++ {
				h.record(time.Duration(i) * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	s := h.snapshot()
	if s.Samples != 4000 || s.MeanMS != 0.5005 || s.MaxMS != 1 {
		t.Fatalf("unexpected summary: %+v", s)
	}
	for _, p := range []struct{ got, want float64 }{{s.P50MS, 0.5}, {s.P95MS, 0.95}, {s.P99MS, 0.99}} {
		if p.got < p.want || p.got > p.want*1.1+0.001 {
			t.Fatalf("percentile %.3f outside histogram precision for %.3f", p.got, p.want)
		}
	}
}

func TestConfigRejectsInvalidRatesAndTLSOptions(t *testing.T) {
	for _, rate := range []float64{0, -1, 1e-20, math.NaN(), math.Inf(1), 1001} {
		cfg := DefaultConfig()
		cfg.Rate = rate
		if cfg.Validate() == nil {
			t.Fatalf("accepted invalid rate %v", rate)
		}
	}
	cfg := DefaultConfig()
	cfg.TLSCA = "some-ca.pem"
	if cfg.Validate() == nil {
		t.Fatal("accepted CA option without TLS")
	}
}

func TestRunPreparesDistinctRolesAndValidatesTCPWorkload(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var handlers sync.WaitGroup
	doneAccept := make(chan struct{})
	go func() {
		defer close(doneAccept)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				frame, err := wire.ReadFrame(conn, wire.DefaultMaxBody)
				if err != nil || frame.Opcode != wire.Auth || frame.Seq != 1 {
					return
				}
				var ticket struct {
					Ticket string `json:"ticket"`
				}
				if json.Unmarshal(frame.Body, &ticket) != nil {
					return
				}
				var id int64
				if json.Unmarshal([]byte(ticket.Ticket), &id) != nil {
					return
				}
				body, _ := json.Marshal(map[string]any{"account_id": id, "role_id": id, "server_id": "s1"})
				if wire.WriteFrame(conn, wire.Frame{Opcode: wire.AuthOK, Seq: 1, Body: body}) != nil {
					return
				}
				last := uint64(1)
				for {
					frame, err := wire.ReadFrame(conn, wire.DefaultMaxBody)
					if err != nil || frame.Seq <= last {
						return
					}
					last = frame.Seq
					if frame.Opcode == wire.Ping {
						frame.Opcode, frame.Body = wire.Pong, []byte(`{"server_time":123}`)
					} else if frame.Opcode == wire.Echo {
						frame.Opcode = wire.EchoOK
					} else {
						return
					}
					if wire.WriteFrame(conn, frame) != nil {
						return
					}
				}
			}()
		}
	}()
	var roles atomic.Int64
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch req.Method + " " + req.URL.Path {
		case "POST /v1/accounts":
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{}`)
		case "POST /v1/sessions":
			_, _ = io.WriteString(w, `{"access_token":"not-a-real-secret","refresh_token":"fake-refresh","expires_in":900}`)
		case "GET /v1/roles":
			_, _ = io.WriteString(w, `{"roles":[]}`)
		case "POST /v1/roles":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]int64{"id": roles.Add(1)})
		case "POST /v1/tickets":
			var b struct {
				RoleID int64 `json:"role_id"`
			}
			if json.NewDecoder(req.Body).Decode(&b) != nil {
				w.WriteHeader(400)
				return
			}
			ticket, _ := json.Marshal(b.RoleID)
			_ = json.NewEncoder(w).Encode(map[string]string{"ticket": string(ticket), "gateway": ln.Addr().String()})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer httpServer.Close()
	cfg := DefaultConfig()
	cfg.LoginURL, cfg.Gateway = httpServer.URL, ""
	cfg.Bots, cfg.PrepareConcurrency, cfg.Duration, cfg.Ramp, cfg.Rate = 2, 2, time.Second, 10*time.Millisecond, 30
	report, err := Run(context.Background(), cfg, nil)
	_ = ln.Close()
	<-doneAccept
	handlers.Wait()
	if err != nil {
		t.Fatalf("run failed: %v; report=%+v", err, report)
	}
	if !report.Passed || report.Prepared != 2 || report.Counters.AuthOK != 2 || report.Counters.MessagesOK < 40 || report.MessageLatency.Samples != report.Counters.MessagesOK {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.Counters.MessagesSent != report.Counters.MessagesOK+report.Counters.MessagesFailed+report.Counters.MessagesCanceled {
		t.Fatalf("attempt counters inconsistent: %+v", report.Counters)
	}
	if report.Counters.BytesReceived <= 0 || report.Counters.BytesSent <= 0 {
		t.Fatal("TCP byte counters were not recorded")
	}
}

func TestRefreshAccessRotatesExpiringTokenBeforeTicket(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.URL.Path != "/v1/sessions/refresh" {
			t.Errorf("unexpected request %s %s", req.Method, req.URL.Path)
			w.WriteHeader(404)
			return
		}
		var body struct {
			RefreshToken string `json:"refresh_token"`
		}
		if json.NewDecoder(req.Body).Decode(&body) != nil || body.RefreshToken != "old-refresh" {
			t.Error("incorrect refresh token submitted")
			w.WriteHeader(401)
			return
		}
		calls.Add(1)
		_, _ = io.WriteString(w, `{"access_token":"new-access","refresh_token":"rotated-refresh","expires_in":900}`)
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.LoginURL = server.URL
	r := runner{cfg: cfg, http: server.Client()}
	for _, expiry := range []time.Time{time.Now().Add(-time.Hour), time.Now().Add(time.Second)} {
		a := prepared{access: "old-access", refresh: "old-refresh", accessExpires: expiry, roleID: 123}
		got, err := r.refreshAccess(context.Background(), a)
		if err != nil || got.access != "new-access" || got.refresh != "rotated-refresh" || got.roleID != 123 || time.Until(got.accessExpires) < 14*time.Minute {
			t.Fatalf("refresh failed: account=%+v err=%v", got, err)
		}
		if _, err := r.refreshAccess(context.Background(), got); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 || r.counts.sessionRefreshOK.Load() != 2 || r.counts.sessionRefreshFail.Load() != 0 {
		t.Fatal("valid tokens refreshed unnecessarily or rotation counters incorrect")
	}
}

func TestExchangeRejectsWrongSequenceAndGatewayError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame wire.Frame
	}{
		{"wrong sequence", wire.Frame{Opcode: wire.EchoOK, Seq: 99, Body: []byte(`{}`)}},
		{"gateway error", wire.Frame{Opcode: wire.Error, Seq: 2, Body: []byte(`{"code":"RATE_LIMITED"}`)}},
		{"wrong opcode", wire.Frame{Opcode: wire.Pong, Seq: 2, Body: []byte(`{}`)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			go func() {
				_, _ = wire.ReadFrame(server, wire.DefaultMaxBody)
				_ = wire.WriteFrame(server, tc.frame)
			}()
			r := runner{cfg: DefaultConfig()}
			if err := r.exchange(context.Background(), client, wire.Frame{Opcode: wire.Echo, Seq: 2, Body: []byte(`{}`)}, wire.EchoOK, nil); err == nil {
				t.Fatal("accepted invalid response")
			}
		})
	}
}

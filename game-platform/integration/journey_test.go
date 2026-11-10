package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gameplatform/internal/core"
	"gameplatform/internal/gateway"
	"gameplatform/internal/login"
	"gameplatform/internal/storage"
	"gameplatform/internal/wire"
)

// The real TCP listener and HTTP server exercise the whole player journey,
// including packet boundaries, shared account leases and session revocation.
func TestPlayerJourney(t *testing.T) {
	state := storage.NewMemory()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	gw, err := gateway.New(gateway.Config{LeaseRefresh: 40 * time.Millisecond, LeaseTTL: 200 * time.Millisecond}, state, log)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- gw.Serve(l) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := gw.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if err := <-served; err != nil {
			t.Error(err)
		}
	})
	h, err := login.New(login.Config{JWTSecret: strings.Repeat("s", 32), GatewayAddress: l.Addr().String(), PasswordCost: 10, AuthLimit: 1000}, state, state, log)
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(h)
	defer api.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	request := func(method, path, token string, body any, want int, out any) {
		t.Helper()
		var raw []byte
		if body != nil {
			raw, err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
		}
		r, e := http.NewRequest(method, api.URL+path, bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		r.Header.Set("Content-Type", "application/json")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		resp, e := client.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		b, e := io.ReadAll(resp.Body)
		if e != nil {
			t.Fatal(e)
		}
		if resp.StatusCode != want {
			t.Fatalf("%s %s: got %d want %d body=%s", method, path, resp.StatusCode, want, b)
		}
		if out != nil {
			if e = json.Unmarshal(b, out); e != nil {
				t.Fatal(e)
			}
		}
	}
	credentials := func(name string) map[string]string {
		return map[string]string{"username": name, "password": "integration-password-42"}
	}
	type pair struct {
		Access  string `json:"access_token"`
		Refresh string `json:"refresh_token"`
	}
	var alice, bob pair
	request("POST", "/v1/accounts", "", credentials("player_alice"), 201, nil)
	request("POST", "/v1/accounts", "", credentials("player_bob"), 201, nil)
	request("POST", "/v1/sessions", "", credentials("player_alice"), 200, &alice)
	request("POST", "/v1/sessions", "", credentials("player_bob"), 200, &bob)
	var servers struct {
		Servers []core.Server `json:"servers"`
	}
	request("GET", "/v1/servers", alice.Access, nil, 200, &servers)
	if len(servers.Servers) != 1 || servers.Servers[0].Gateway != l.Addr().String() {
		t.Fatal("incorrect server discovery")
	}
	var role core.Role
	request("POST", "/v1/roles", alice.Access, map[string]string{"server_id": "s1", "name": "AliceHero", "class": "mage"}, 201, &role)
	request("POST", "/v1/tickets", bob.Access, map[string]int64{"role_id": role.ID}, 404, nil)
	ticket := func() string {
		t.Helper()
		var out struct {
			Ticket string `json:"ticket"`
		}
		request("POST", "/v1/tickets", alice.Access, map[string]int64{"role_id": role.ID}, 200, &out)
		return out.Ticket
	}
	connect := func(token string, want uint8) net.Conn {
		t.Helper()
		c, e := net.DialTimeout("tcp", l.Addr().String(), time.Second)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { c.Close() })
		c.SetDeadline(time.Now().Add(2 * time.Second))
		b, _ := json.Marshal(map[string]string{"ticket": token})
		if e = wire.WriteFrame(c, wire.Frame{Opcode: wire.Auth, Seq: 1, Body: b}); e != nil {
			t.Fatal(e)
		}
		f, e := wire.ReadFrame(c, wire.DefaultMaxBody)
		if e != nil {
			t.Fatal(e)
		}
		if f.Opcode != want || f.Seq != 1 {
			t.Fatalf("auth got opcode=%d seq=%d body=%s", f.Opcode, f.Seq, f.Body)
		}
		return c
	}
	first := ticket()
	c := connect(first, wire.AuthOK)
	connect(first, wire.Error).Close()    // The very same ticket cannot be replayed.
	connect(ticket(), wire.Error).Close() // Another ticket cannot bypass the account lease.
	var packets bytes.Buffer
	if e := wire.WriteFrame(&packets, wire.Frame{Opcode: wire.Ping, Seq: 2, Body: []byte(`{}`)}); e != nil {
		t.Fatal(e)
	}
	echo := []byte(`{"move":[1,2],"text":"测试"}`)
	if e := wire.WriteFrame(&packets, wire.Frame{Opcode: wire.Echo, Seq: 3, Body: echo}); e != nil {
		t.Fatal(e)
	}
	// Split the length prefix and coalesce the remaining two messages.
	for _, chunk := range [][]byte{packets.Bytes()[:2], packets.Bytes()[2:7], packets.Bytes()[7:]} {
		if _, e := c.Write(chunk); e != nil {
			t.Fatal(e)
		}
	}
	pong, e := wire.ReadFrame(c, wire.DefaultMaxBody)
	if e != nil || pong.Opcode != wire.Pong || pong.Seq != 2 {
		t.Fatalf("ping: %+v %v", pong, e)
	}
	response, e := wire.ReadFrame(c, wire.DefaultMaxBody)
	if e != nil || response.Opcode != wire.EchoOK || response.Seq != 3 || !bytes.Equal(response.Body, echo) {
		t.Fatalf("echo: %+v %v", response, e)
	}
	oldRefresh := alice.Refresh
	request("POST", "/v1/sessions/refresh", "", map[string]string{"refresh_token": oldRefresh}, 200, &alice)
	request("POST", "/v1/sessions/refresh", "", map[string]string{"refresh_token": oldRefresh}, 401, nil)
	request("DELETE", "/v1/sessions", alice.Access, nil, 204, nil)
	request("GET", "/v1/roles", alice.Access, nil, 401, nil)
	request("POST", "/v1/sessions/refresh", "", map[string]string{"refresh_token": alice.Refresh}, 401, nil)
	c.SetReadDeadline(time.Now().Add(time.Second))
	_, e = wire.ReadFrame(c, wire.DefaultMaxBody)
	if e == nil {
		t.Fatal("gateway kept revoked session connected")
	}
	if ne, ok := e.(net.Error); ok && ne.Timeout() {
		t.Fatal("gateway did not disconnect revoked session in time")
	}
}

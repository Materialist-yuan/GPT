package login_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gameplatform/internal/core"
	"gameplatform/internal/login"
	"gameplatform/internal/storage"
)

type tokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

func newService(t *testing.T, limit int) (*httptest.Server, *storage.Memory) {
	t.Helper()
	memory := storage.NewMemory()
	handler, err := login.New(login.Config{JWTSecret: strings.Repeat("k", 32), PasswordCost: 10, AuthLimit: limit, GatewayAddress: "127.0.0.1:9000"}, memory, memory, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server, memory
}

func request(t *testing.T, server *httptest.Server, method, path, token string, body any, want int) []byte {
	t.Helper()
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, server.URL+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != want {
		t.Fatalf("%s %s status=%d want=%d body=%s", method, path, response.StatusCode, want, data)
	}
	if response.Header.Get("X-Request-ID") == "" {
		t.Fatal("missing request id")
	}
	return data
}

func createAccount(t *testing.T, server *httptest.Server, name string) tokens {
	t.Helper()
	creds := map[string]string{"username": name, "password": "a-valid-password-123"}
	body := request(t, server, "POST", "/v1/accounts", "", creds, 201)
	if bytes.Contains(body, []byte("password")) {
		t.Fatal("response leaked password")
	}
	body = request(t, server, "POST", "/v1/sessions", "", creds, 200)
	var pair tokens
	if err := json.Unmarshal(body, &pair); err != nil {
		t.Fatal(err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatal("missing tokens")
	}
	return pair
}

func TestLoginRoleTicketAndLogoutFlow(t *testing.T) {
	server, state := newService(t, 1000)
	pair := createAccount(t, server, "PlayerOne")
	request(t, server, "GET", "/v1/servers", pair.AccessToken, nil, 200)
	request(t, server, "GET", "/v1/roles?server_id=s1", pair.AccessToken, nil, 200)
	body := request(t, server, "POST", "/v1/roles", pair.AccessToken, map[string]string{"server_id": "s1", "name": "勇士一号", "class": "warrior"}, 201)
	var role core.Role
	if err := json.Unmarshal(body, &role); err != nil || role.ID <= 0 {
		t.Fatalf("role %s %v", body, err)
	}
	body = request(t, server, "POST", "/v1/tickets", pair.AccessToken, map[string]int64{"role_id": role.ID}, 200)
	var ticket struct {
		Ticket  string `json:"ticket"`
		Gateway string `json:"gateway"`
	}
	if err := json.Unmarshal(body, &ticket); err != nil {
		t.Fatal(err)
	}
	grant, err := state.ConsumeTicket(context.Background(), ticket.Ticket)
	if err != nil || grant.RoleID != role.ID || grant.AccountID <= 0 || grant.SessionID == "" {
		t.Fatalf("grant %+v error %v", grant, err)
	}
	if _, err = state.ConsumeTicket(context.Background(), ticket.Ticket); !errors.Is(err, core.ErrUnauthorized) {
		t.Fatal("ticket replay accepted")
	}
	request(t, server, "DELETE", "/v1/sessions", pair.AccessToken, nil, 204)
	request(t, server, "GET", "/v1/servers", pair.AccessToken, nil, 401)
	request(t, server, "POST", "/v1/sessions/refresh", "", map[string]string{"refresh_token": pair.RefreshToken}, 401)
}

func TestRefreshRotationAndRoleOwnership(t *testing.T) {
	server, _ := newService(t, 1000)
	pair := createAccount(t, server, "player_a")
	other := createAccount(t, server, "player_b")
	body := request(t, server, "POST", "/v1/sessions/refresh", "", map[string]string{"refresh_token": pair.RefreshToken}, 200)
	var rotated tokens
	if err := json.Unmarshal(body, &rotated); err != nil {
		t.Fatal(err)
	}
	if rotated.RefreshToken == pair.RefreshToken {
		t.Fatal("refresh token did not rotate")
	}
	request(t, server, "POST", "/v1/sessions/refresh", "", map[string]string{"refresh_token": pair.RefreshToken}, 401)
	request(t, server, "GET", "/v1/servers", rotated.AccessToken, nil, 200)
	body = request(t, server, "POST", "/v1/roles", rotated.AccessToken, map[string]string{"server_id": "s1", "name": "Knight", "class": "warrior"}, 201)
	var role core.Role
	if err := json.Unmarshal(body, &role); err != nil {
		t.Fatal(err)
	}
	request(t, server, "POST", "/v1/tickets", other.AccessToken, map[string]int64{"role_id": role.ID}, 404)
}

func TestValidationAndRateLimit(t *testing.T) {
	server, _ := newService(t, 1000)
	request(t, server, "POST", "/v1/accounts", "", map[string]string{"username": "validname", "password": "short"}, 400)
	request(t, server, "POST", "/v1/accounts", "", map[string]string{"username": "invalid name", "password": "a-valid-password-123"}, 400)
	request(t, server, "POST", "/v1/sessions", "", map[string]string{"username": "nonexistent", "password": "wrong-password"}, 401)
	request(t, server, "GET", "/v1/servers", "bad-token", nil, 401)
	request(t, server, "POST", "/v1/accounts", "", map[string]string{"username": "player", "password": "a-valid-password-123", "extra": "bad"}, 400)
	limited, _ := newService(t, 1)
	request(t, limited, "POST", "/v1/sessions", "", map[string]string{"username": "nonexistent", "password": "wrong-password"}, 401)
	request(t, limited, "POST", "/v1/sessions", "", map[string]string{"username": "nonexistent", "password": "wrong-password"}, 429)
}

func TestRejectWeakConfiguration(t *testing.T) {
	memory := storage.NewMemory()
	if _, err := login.New(login.Config{JWTSecret: "short"}, memory, memory, nil); err == nil {
		t.Fatal("accepted weak signing key")
	}
	for name, cfg := range map[string]login.Config{
		"password_cost":         {PasswordCost: -1},
		"password_concurrency":  {PasswordConcurrency: -1},
		"auth_limit":            {AuthLimit: -1},
		"negative_gateway_port": {GatewayAddress: "localhost:-1"},
		"oversize_gateway_port": {GatewayAddress: "localhost:65536"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg.JWTSecret = strings.Repeat("k", 32)
			if _, err := login.New(cfg, memory, memory, nil); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}

func TestLoginRejectsPasswordSuffixBeyondBcryptLimit(t *testing.T) {
	server, _ := newService(t, 1000)
	password := strings.Repeat("p", 72)
	credentials := map[string]string{"username": "boundary_user", "password": password}
	request(t, server, "POST", "/v1/accounts", "", credentials, 201)
	request(t, server, "POST", "/v1/sessions", "", credentials, 200)
	credentials["password"] = password + "suffix"
	request(t, server, "POST", "/v1/sessions", "", credentials, 401)
}

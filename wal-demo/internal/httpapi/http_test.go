package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/game"
)

type fakeService struct {
	operations []game.Operation
	result     game.Result
	player     game.Player
	err        error
	healthErr  error
	getID      string
}

func (s *fakeService) Execute(_ context.Context, op game.Operation) (game.Result, error) {
	s.operations = append(s.operations, op)
	return s.result, s.err
}

func (s *fakeService) Get(_ context.Context, id string) (game.Player, error) {
	s.getID = id
	return s.player, s.err
}

func (s *fakeService) Healthy(context.Context) error { return s.healthErr }

func request(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	if contentType := w.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "application/json") {
		t.Fatalf("Content-Type = %q; want JSON", contentType)
	}
	return w
}

func TestAssetRoutes(t *testing.T) {
	player := &game.Player{ID: "alice", Coins: 70, Inventory: map[string]int64{"potion": 1}, Version: 2}
	for _, tc := range []struct {
		name      string
		path      string
		body      string
		operation game.Operation
		status    int
	}{
		{"create", "/players", `{"player_id":"alice","request_id":"create-1"}`, game.Operation{PlayerID: "alice", RequestID: "create-1", Kind: "create"}, http.StatusCreated},
		{"reward", "/players/alice/rewards", `{"request_id":"reward-1","amount":25}`, game.Operation{PlayerID: "alice", RequestID: "reward-1", Kind: "reward", Amount: 25}, http.StatusOK},
		{"purchase", "/players/alice/purchases", `{"request_id":"buy-1","item":"potion"}`, game.Operation{PlayerID: "alice", RequestID: "buy-1", Kind: "purchase", Item: "potion"}, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeService{result: game.Result{Status: tc.status, Code: "ok", Player: player, Replayed: true}}
			w := request(t, New(s), http.MethodPost, tc.path, tc.body)
			if w.Code != tc.status {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body)
			}
			if !reflect.DeepEqual(s.operations, []game.Operation{tc.operation}) {
				t.Fatalf("operations = %#v; want %#v", s.operations, tc.operation)
			}
			var body game.Result
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Code != "ok" || !body.Replayed || !reflect.DeepEqual(body.Player, player) {
				t.Fatalf("body = %#v", body)
			}
			if strings.Contains(w.Body.String(), `"Status"`) || strings.Contains(w.Body.String(), `"status"`) {
				t.Fatalf("internal Status leaked: %s", w.Body)
			}
		})
	}
}

func TestReadRoutes(t *testing.T) {
	s := &fakeService{player: game.Player{ID: "alice", Coins: 100, Inventory: map[string]int64{}}}
	handler := New(s)
	w := request(t, handler, http.MethodGet, "/players/alice", "")
	if w.Code != http.StatusOK || s.getID != "alice" {
		t.Fatalf("read status = %d, id = %s, body = %s", w.Code, s.getID, w.Body)
	}
	var player game.Player
	if err := json.Unmarshal(w.Body.Bytes(), &player); err != nil || !reflect.DeepEqual(player, s.player) {
		t.Fatalf("read body = %s, error = %v", w.Body, err)
	}
	w = request(t, handler, http.MethodGet, "/catalog", "")
	var catalog map[string]int64
	if err := json.Unmarshal(w.Body.Bytes(), &catalog); err != nil || w.Code != http.StatusOK || !reflect.DeepEqual(catalog, game.Catalog()) {
		t.Fatalf("catalog status = %d, body = %s, error = %v", w.Code, w.Body, err)
	}
	w = request(t, handler, http.MethodGet, "/healthz", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"ok"`) {
		t.Fatalf("health status = %d, body = %s", w.Code, w.Body)
	}
}

func TestInvalidRequestsNeverExecute(t *testing.T) {
	for _, tc := range []struct {
		name   string
		path   string
		body   string
		status int
	}{
		{"empty body", "/players", "", 400},
		{"null", "/players", "null", 400},
		{"array", "/players", "[]", 400},
		{"unknown field", "/players", `{"player_id":"alice","request_id":"req-1","coins":999}`, 400},
		{"multiple bodies", "/players", `{"player_id":"alice","request_id":"req-1"}{}`, 400},
		{"trailing garbage", "/players", `{"player_id":"alice","request_id":"req-1"}!`, 400},
		{"bad player id", "/players", `{"player_id":"$bad","request_id":"req-1"}`, 400},
		{"long player id", "/players", `{"player_id":"` + strings.Repeat("a", 65) + `","request_id":"req-1"}`, 400},
		{"bad request id", "/players", `{"player_id":"alice","request_id":"a.b"}`, 400},
		{"missing request id", "/players/alice/rewards", `{"amount":1}`, 400},
		{"long request id", "/players/alice/rewards", `{"request_id":"` + strings.Repeat("a", 129) + `","amount":1}`, 400},
		{"bad path id", "/players/$alice/rewards", `{"request_id":"req-1","amount":1}`, 400},
		{"zero reward", "/players/alice/rewards", `{"request_id":"req-1","amount":0}`, 400},
		{"negative reward", "/players/alice/rewards", `{"request_id":"req-1","amount":-1}`, 400},
		{"large reward", "/players/alice/rewards", `{"request_id":"req-1","amount":1000000001}`, 400},
		{"fractional reward", "/players/alice/rewards", `{"request_id":"req-1","amount":1.5}`, 400},
		{"unknown item", "/players/alice/purchases", `{"request_id":"req-1","item":"free-money"}`, 400},
		{"missing item", "/players/alice/purchases", `{"request_id":"req-1"}`, 400},
		{"oversized body", "/players", `{"player_id":"alice","request_id":"req-1"}` + strings.Repeat(" ", maxBodyBytes), 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeService{}
			w := request(t, New(s), http.MethodPost, tc.path, tc.body)
			if w.Code != tc.status {
				t.Fatalf("status = %d; want %d, body = %s", w.Code, tc.status, w.Body)
			}
			if len(s.operations) != 0 {
				t.Fatalf("malformed request executed: %#v", s.operations)
			}
		})
	}
}

func TestErrorsAreMappedWithoutLeakingDetails(t *testing.T) {
	secretError := errors.New("mongodb://secret-password@private-host")
	for _, tc := range []struct {
		name    string
		method  string
		path    string
		body    string
		service *fakeService
		status  int
		code    string
	}{
		{"missing player", "GET", "/players/alice", "", &fakeService{err: game.ErrNotFound}, 404, "player_not_found"},
		{"read unavailable", "GET", "/players/alice", "", &fakeService{err: secretError}, 503, "unavailable"},
		{"write unavailable", "POST", "/players", `{"player_id":"alice","request_id":"req-1"}`, &fakeService{err: secretError}, 503, "unavailable"},
		{"unhealthy", "GET", "/healthz", "", &fakeService{healthErr: secretError}, 503, "unavailable"},
		{"invalid read id", "GET", "/players/$bad", "", &fakeService{}, 400, "invalid_request"},
		{"missing route", "GET", "/missing", "", &fakeService{}, 404, "not_found"},
		{"domain conflict", "POST", "/players", `{"player_id":"alice","request_id":"req-1"}`, &fakeService{result: game.Result{Status: 409, Code: "idempotency_conflict"}}, 409, "idempotency_conflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := request(t, New(tc.service), tc.method, tc.path, tc.body)
			if w.Code != tc.status || !strings.Contains(w.Body.String(), `"code":"`+tc.code+`"`) {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body)
			}
			if strings.Contains(w.Body.String(), "secret-password") {
				t.Fatalf("internal error leaked: %s", w.Body)
			}
		})
	}
}

func TestWhitespaceAndLimitsAccepted(t *testing.T) {
	s := &fakeService{result: game.Result{Status: 200, Code: "ok"}}
	for _, body := range []string{
		`{"request_id":"req-1","amount":1}` + "\n \t",
		`{"request_id":"` + strings.Repeat("a", 128) + `","amount":1000000000}`,
	} {
		w := request(t, New(s), "POST", "/players/"+strings.Repeat("a", 64)+"/rewards", body)
		if w.Code != 200 {
			t.Fatalf("status = %d, body = %s", w.Code, w.Body)
		}
	}
}

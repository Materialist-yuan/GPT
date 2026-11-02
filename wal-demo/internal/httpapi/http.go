// Package httpapi exposes the game asset demo as a small JSON HTTP API.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"

	"github.com/Materialist-yuan/GPT/wal-demo/internal/game"
)

const maxBodyBytes = 8 * 1024

var (
	playerIDPattern  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// Service is implemented by the durable game engine. Transport validation
// happens before Execute, so malformed requests never enter the WAL.
type Service interface {
	Execute(context.Context, game.Operation) (game.Result, error)
	Get(context.Context, string) (game.Player, error)
	Healthy(context.Context) error
}

// New returns an HTTP handler. Authentication is intentionally outside the
// scope of this local development demo.
func New(service Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /players", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			PlayerID  string `json:"player_id"`
			RequestID string `json:"request_id"`
		}
		if !decode(w, r, &body) {
			return
		}
		if !playerIDPattern.MatchString(body.PlayerID) || !requestIDPattern.MatchString(body.RequestID) {
			failure(w, http.StatusBadRequest, "invalid_request")
			return
		}
		execute(w, r, service, game.Operation{PlayerID: body.PlayerID, RequestID: body.RequestID, Kind: "create"})
	})
	mux.HandleFunc("POST /players/{id}/rewards", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RequestID string `json:"request_id"`
			Amount    int64  `json:"amount"`
		}
		if !decode(w, r, &body) {
			return
		}
		if !playerIDPattern.MatchString(r.PathValue("id")) || !requestIDPattern.MatchString(body.RequestID) || body.Amount < 1 || body.Amount > 1_000_000_000 {
			failure(w, http.StatusBadRequest, "invalid_request")
			return
		}
		execute(w, r, service, game.Operation{PlayerID: r.PathValue("id"), RequestID: body.RequestID, Kind: "reward", Amount: body.Amount})
	})
	mux.HandleFunc("POST /players/{id}/purchases", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RequestID string `json:"request_id"`
			Item      string `json:"item"`
		}
		if !decode(w, r, &body) {
			return
		}
		_, knownItem := game.Catalog()[body.Item]
		if !playerIDPattern.MatchString(r.PathValue("id")) || !requestIDPattern.MatchString(body.RequestID) || !knownItem {
			failure(w, http.StatusBadRequest, "invalid_request")
			return
		}
		execute(w, r, service, game.Operation{PlayerID: r.PathValue("id"), RequestID: body.RequestID, Kind: "purchase", Item: body.Item})
	})
	mux.HandleFunc("GET /players/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !playerIDPattern.MatchString(id) {
			failure(w, http.StatusBadRequest, "invalid_request")
			return
		}
		player, err := service.Get(r.Context(), id)
		if errors.Is(err, game.ErrNotFound) {
			failure(w, http.StatusNotFound, "player_not_found")
			return
		}
		if err != nil {
			failure(w, http.StatusServiceUnavailable, "unavailable")
			return
		}
		respond(w, http.StatusOK, player)
	})
	mux.HandleFunc("GET /catalog", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, game.Catalog())
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := service.Healthy(r.Context()); err != nil {
			failure(w, http.StatusServiceUnavailable, "unavailable")
			return
		}
		respond(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		failure(w, http.StatusNotFound, "not_found")
	})
	return mux
}

func execute(w http.ResponseWriter, r *http.Request, service Service, op game.Operation) {
	result, err := service.Execute(r.Context(), op)
	if err != nil || result.Status < 200 || result.Status > 599 {
		failure(w, http.StatusServiceUnavailable, "unavailable")
		return
	}
	respond(w, result.Status, result)
}

func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		decodeFailure(w, err)
		return false
	}
	// A valid request contains exactly one JSON value, with optional whitespace.
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		decodeFailure(w, err)
		return false
	}
	return true
}

func decodeFailure(w http.ResponseWriter, err error) {
	var oversized *http.MaxBytesError
	if errors.As(err, &oversized) {
		failure(w, http.StatusRequestEntityTooLarge, "request_too_large")
		return
	}
	failure(w, http.StatusBadRequest, "invalid_request")
}

func failure(w http.ResponseWriter, status int, code string) {
	respond(w, status, map[string]string{"code": code})
}

func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

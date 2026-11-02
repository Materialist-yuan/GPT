package game

import "errors"

var ErrNotFound = errors.New("player not found")

// Operation is durably written before any asset change reaches MongoDB.
type Operation struct {
	PlayerID  string `json:"player_id"`
	RequestID string `json:"request_id"`
	Kind      string `json:"kind"`
	Amount    int64  `json:"amount,omitempty"`
	Item      string `json:"item,omitempty"`
}

type Player struct {
	ID        string           `json:"id" bson:"_id"`
	Coins     int64            `json:"coins" bson:"coins"`
	Inventory map[string]int64 `json:"inventory" bson:"inventory"`
	Version   int64            `json:"version" bson:"version"`
}

type Result struct {
	Status   int     `json:"-" bson:"status"`
	Code     string  `json:"code" bson:"code"`
	Player   *Player `json:"player,omitempty" bson:"player,omitempty"`
	Replayed bool    `json:"replayed" bson:"-"`
}

// Catalog returns a fresh copy so callers cannot change server prices.
func Catalog() map[string]int64 {
	return map[string]int64{"potion": 30, "sword": 80, "shield": 50}
}

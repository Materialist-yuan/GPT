package game

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const MaxCoins int64 = 1_000_000_000_000

var validID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func Validate(op Operation) error {
	if len(op.PlayerID) == 0 || len(op.PlayerID) > 64 || !validID.MatchString(op.PlayerID) {
		return errors.New("invalid player_id")
	}
	if len(op.RequestID) == 0 || len(op.RequestID) > 128 || !validID.MatchString(op.RequestID) {
		return errors.New("invalid request_id")
	}
	switch op.Kind {
	case "create":
		if op.Amount != 0 || op.Item != "" {
			return errors.New("unexpected create fields")
		}
	case "reward":
		if op.Amount < 1 || op.Amount > 1_000_000_000 || op.Item != "" {
			return errors.New("invalid reward")
		}
	case "purchase":
		if _, ok := Catalog()[op.Item]; !ok || op.Amount != 0 {
			return errors.New("invalid item")
		}
	default:
		return errors.New("invalid operation")
	}
	return nil
}

type receipt struct {
	Fingerprint string `bson:"fingerprint"`
	Result      Result `bson:"result"`
}

type document struct {
	Player      `bson:",inline"`
	Initialized bool               `bson:"initialized"`
	Receipts    map[string]receipt `bson:"receipts"`
}

type MongoStore struct {
	client  *mongo.Client
	players *mongo.Collection
}

func OpenMongo(ctx context.Context, uri, database string) (*MongoStore, error) {
	j := true
	client, err := mongo.Connect(options.Client().ApplyURI(uri).
		SetWriteConcern(&writeconcern.WriteConcern{W: "majority", Journal: &j}))
	if err != nil {
		return nil, err
	}
	s := &MongoStore{client: client, players: client.Database(database).Collection("players")}
	if err := s.Ping(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongo ping: %w", err)
	}
	return s, nil
}

func (s *MongoStore) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }
func (s *MongoStore) Ping(ctx context.Context) error  { return s.client.Ping(ctx, readpref.Primary()) }

func (s *MongoStore) Get(ctx context.Context, id string) (Player, error) {
	var d document
	err := s.players.FindOne(ctx, bson.M{"_id": id, "initialized": true}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Player{}, ErrNotFound
	}
	return d.Player, err
}

// Apply commits the balance, inventory and idempotency receipt in one atomic
// MongoDB document update. A replica set/transaction is not needed for this demo.
func (s *MongoStore) Apply(ctx context.Context, op Operation) (Result, error) {
	if err := Validate(op); err != nil {
		return Result{}, err
	}
	initial := document{Player: Player{ID: op.PlayerID, Inventory: map[string]int64{}}, Receipts: map[string]receipt{}}
	_, err := s.players.UpdateOne(ctx, bson.M{"_id": op.PlayerID}, bson.M{"$setOnInsert": initial}, options.UpdateOne().SetUpsert(true))
	// Concurrent initializations can race on the unique _id index; the winner's
	// placeholder is safe to read and update.
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return Result{}, err
	}
	encoded, _ := json.Marshal(op)
	fingerprint := digest(encoded)
	key := digest([]byte(op.RequestID))
	for attempt := 0; attempt < 32; attempt++ {
		var d document
		if err := s.players.FindOne(ctx, bson.M{"_id": op.PlayerID}).Decode(&d); err != nil {
			return Result{}, err
		}
		if saved, ok := d.Receipts[key]; ok {
			if saved.Fingerprint != fingerprint {
				return Result{Status: 409, Code: "idempotency_conflict"}, nil
			}
			r := saved.Result
			r.Replayed = true
			return r, nil
		}
		previous := d.Version
		r := transition(&d, op)
		saved := receipt{Fingerprint: fingerprint, Result: r}
		// Version changes even for rejected business operations, so their receipt
		// cannot be lost to another writer's compare-and-swap.
		update := bson.M{"$set": bson.M{
			"coins": d.Coins, "inventory": d.Inventory, "initialized": d.Initialized,
			"version": d.Version, "receipts." + key: saved,
		}}
		written, err := s.players.UpdateOne(ctx, bson.M{"_id": op.PlayerID, "version": previous}, update)
		if err != nil {
			return Result{}, err
		}
		if written.MatchedCount == 1 {
			return r, nil
		}
	}
	return Result{}, errors.New("mongo update contention; retry via WAL recovery")
}

func digest(value []byte) string { sum := sha256.Sum256(value); return hex.EncodeToString(sum[:]) }

func transition(d *document, op Operation) Result {
	d.Version++
	r := Result{Status: 200, Code: "ok"}
	switch {
	case op.Kind == "create" && d.Initialized:
		r.Status, r.Code = 409, "player_exists"
	case op.Kind == "create":
		d.Initialized, d.Coins = true, 100
		d.Inventory = map[string]int64{}
		r.Status = 201
	case !d.Initialized:
		r.Status, r.Code = 404, "player_not_found"
	case op.Kind == "reward" && d.Coins > MaxCoins-op.Amount:
		r.Status, r.Code = 409, "balance_limit"
	case op.Kind == "reward":
		d.Coins += op.Amount
	case op.Kind == "purchase" && d.Coins < Catalog()[op.Item]:
		r.Status, r.Code = 409, "insufficient_coins"
	case op.Kind == "purchase":
		d.Coins -= Catalog()[op.Item]
		if d.Inventory == nil {
			d.Inventory = map[string]int64{}
		}
		d.Inventory[op.Item]++
	}
	if d.Initialized {
		p := d.Player
		p.Inventory = make(map[string]int64, len(d.Inventory))
		for item, count := range d.Inventory {
			p.Inventory[item] = count
		}
		r.Player = &p
	}
	return r
}

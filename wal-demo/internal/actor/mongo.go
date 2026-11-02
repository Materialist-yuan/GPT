package actor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const snapshotSchema = 1

// MongoStore persists checkpoints, not individual commands. A failed Save may
// have saved some snapshots; retrying the whole batch is safe and monotonic.
type MongoStore struct {
	client    *mongo.Client
	snapshots *mongo.Collection
	shards    int
}

type checkpointDocument struct {
	ID       string `bson:"_id"`
	Shard    int    `bson:"shard"`
	Schema   int    `bson:"schema"`
	Snapshot `bson:",inline"`
}

func OpenMongo(ctx context.Context, uri, database string, shards int) (*MongoStore, error) {
	if shards < 1 {
		return nil, errors.New("actor MongoDB shard count must be positive")
	}
	journal := true
	client, err := mongo.Connect(options.Client().ApplyURI(uri).
		SetWriteConcern(&writeconcern.WriteConcern{W: "majority", Journal: &journal}))
	if err != nil {
		return nil, err
	}
	s := &MongoStore{client: client, snapshots: client.Database(database).Collection("actor_snapshots"), shards: shards}
	failed := true
	defer func() {
		if failed {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = client.Disconnect(closeCtx)
		}
	}()
	if err := s.Ping(ctx); err != nil {
		return nil, fmt.Errorf("actor MongoDB ping: %w", err)
	}
	metadata := client.Database(database).Collection("actor_metadata")
	_, err = metadata.UpdateOne(ctx, bson.M{"_id": "layout"}, bson.M{"$setOnInsert": bson.M{
		"schema": snapshotSchema, "shards": shards,
	}}, options.UpdateOne().SetUpsert(true))
	// Concurrent opens with the same layout can race during initial creation.
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return nil, fmt.Errorf("save actor layout: %w", err)
	}
	var layout struct {
		Schema int `bson:"schema"`
		Shards int `bson:"shards"`
	}
	if err := metadata.FindOne(ctx, bson.M{"_id": "layout"}).Decode(&layout); err != nil {
		return nil, fmt.Errorf("read actor layout: %w", err)
	}
	if layout.Schema != snapshotSchema || layout.Shards != shards {
		return nil, fmt.Errorf("actor layout mismatch: stored schema=%d shards=%d, requested schema=%d shards=%d; explicit migration required",
			layout.Schema, layout.Shards, snapshotSchema, shards)
	}
	if _, err := s.snapshots.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "shard", Value: 1}}, Options: options.Index().SetName("actor_shard"),
	}); err != nil {
		return nil, fmt.Errorf("create actor checkpoint index: %w", err)
	}
	failed = false
	return s, nil
}

func (s *MongoStore) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }
func (s *MongoStore) Ping(ctx context.Context) error  { return s.client.Ping(ctx, readpref.Primary()) }

// BindJournal prevents pairing a checkpoint database with an unrelated local
// journal whose sequence numbers happen to overlap. This identifies journal
// lineage; it is not a lease or distributed ownership fencing.
// When hasLocalHistory is true, the binding must already exist. A fresh binding
// in that case could hide a lost database behind previously completed WAL.
func (s *MongoStore) BindJournal(ctx context.Context, journalID string, hasLocalHistory ...bool) error {
	if len(journalID) == 0 || len(journalID) > 128 {
		return errors.New("invalid actor journal identity")
	}
	metadata := s.snapshots.Database().Collection("actor_metadata")
	filter := bson.M{"_id": "layout", "journal_id": journalID}
	if len(hasLocalHistory) == 0 || !hasLocalHistory[0] {
		filter = bson.M{"_id": "layout", "$or": bson.A{
			bson.M{"journal_id": bson.M{"$exists": false}},
			bson.M{"journal_id": journalID},
		}}
	}
	updated, err := metadata.UpdateOne(ctx, filter, bson.M{"$set": bson.M{"journal_id": journalID}})
	if err != nil {
		return fmt.Errorf("bind actor journal identity: %w", err)
	}
	if updated.MatchedCount != 1 {
		return errors.New("actor checkpoint database belongs to another journal or its history binding is missing; explicit recovery or migration required")
	}
	return nil
}

func (s *MongoStore) Load(ctx context.Context, shard int) ([]Snapshot, error) {
	if shard < 0 || shard >= s.shards {
		return nil, errors.New("invalid checkpoint shard")
	}
	cursor, err := s.snapshots.Find(ctx, bson.M{"shard": shard}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var loaded []Snapshot
	for cursor.Next(ctx) {
		var doc checkpointDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		if doc.Schema != snapshotSchema || doc.ID != doc.Player.ID || doc.Seq == 0 {
			return nil, fmt.Errorf("invalid actor checkpoint for player %q", doc.ID)
		}
		loaded = append(loaded, doc.Snapshot)
	}
	return loaded, cursor.Err()
}

// Save atomically replaces each player's entire checkpoint (state, receipts and
// WAL watermark) only when its sequence advances. Filtering by both identity
// and shard makes a conflicting shard fail on the unique _id index instead of
// silently overwriting another owner's checkpoint. This is not distributed
// ownership fencing: the caller still requires exclusive ownership of its WAL.
func (s *MongoStore) Save(ctx context.Context, shard int, snapshots []Snapshot) error {
	if shard < 0 || shard >= s.shards {
		return errors.New("invalid checkpoint shard")
	}
	models := make([]mongo.WriteModel, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.Player.ID == "" || snapshot.Seq == 0 || snapshot.Seq > math.MaxInt64 {
			return errors.New("invalid actor checkpoint identity or sequence")
		}
		doc := checkpointDocument{ID: snapshot.Player.ID, Shard: shard, Schema: snapshotSchema, Snapshot: snapshot}
		// $literal prevents persisted user values from being interpreted as
		// aggregation expressions. Missing seq means this is the initial upsert.
		pipeline := mongo.Pipeline{bson.D{{Key: "$replaceWith", Value: bson.M{"$cond": bson.A{
			bson.M{"$gt": bson.A{int64(snapshot.Seq), bson.M{"$ifNull": bson.A{"$seq", int64(-1)}}}},
			bson.M{"$literal": doc}, "$$ROOT",
		}}}}}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": snapshot.Player.ID, "shard": shard}).SetUpdate(pipeline).SetUpsert(true))
	}
	if len(models) == 0 {
		return nil
	}
	_, err := s.snapshots.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("save actor checkpoints (partial success possible): %w", err)
	}
	return nil
}

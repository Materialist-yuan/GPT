package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"gameplatform/internal/core"
	"github.com/redis/go-redis/v9"
)

type Redis struct{ client *redis.Client }

func NewRedis(ctx context.Context, redisURL string) (*Redis, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		// Parser errors may embed the entire URL, including its password.
		return nil, errors.New("invalid Redis configuration")
	}
	options.DialTimeout = 5 * time.Second
	options.ReadTimeout = 3 * time.Second
	options.WriteTimeout = 3 * time.Second
	options.PoolSize = 32
	options.ContextTimeoutEnabled = true
	r := &Redis{client: redis.NewClient(options)}
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = r.Ping(probe); err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return r, nil
}

func (r *Redis) Close() error                   { return r.client.Close() }
func (r *Redis) Ping(ctx context.Context) error { return r.client.Ping(ctx).Err() }

func redisKey(kind, value string) string {
	hash := sha256.Sum256([]byte(value))
	return "game:v1:" + kind + ":" + hex.EncodeToString(hash[:])
}

func ttlMillis(ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, errors.New("ttl must be positive")
	}
	n := ttl.Milliseconds()
	if n == 0 {
		n = 1
	}
	return n, nil
}

var createSessionScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'account_id', ARGV[1], 'refresh_hash', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`)

func (r *Redis) CreateSession(ctx context.Context, s core.Session, ttl time.Duration) error {
	ms, err := ttlMillis(ttl)
	if err != nil {
		return err
	}
	n, err := createSessionScript.Run(ctx, r.client, []string{redisKey("session", s.ID)}, strconv.FormatInt(s.AccountID, 10), s.RefreshHash, ms).Int()
	if err != nil {
		return err
	}
	if n != 1 {
		return core.ErrConflict
	}
	return nil
}

func (r *Redis) GetSession(ctx context.Context, id string) (core.Session, error) {
	fields, err := r.client.HGetAll(ctx, redisKey("session", id)).Result()
	if err != nil {
		return core.Session{}, err
	}
	if len(fields) == 0 {
		return core.Session{}, core.ErrNotFound
	}
	accountID, err := strconv.ParseInt(fields["account_id"], 10, 64)
	if err != nil || fields["refresh_hash"] == "" {
		return core.Session{}, core.ErrUnauthorized
	}
	return core.Session{ID: id, AccountID: accountID, RefreshHash: fields["refresh_hash"]}, nil
}

var rotateSessionScript = redis.NewScript(`
local current = redis.call('HGET', KEYS[1], 'refresh_hash')
if not current or current ~= ARGV[1] then return false end
local account = redis.call('HGET', KEYS[1], 'account_id')
if not account then return false end
redis.call('HSET', KEYS[1], 'refresh_hash', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return account
`)

func (r *Redis) RotateSession(ctx context.Context, id, oldHash, newHash string, ttl time.Duration) (core.Session, error) {
	ms, err := ttlMillis(ttl)
	if err != nil {
		return core.Session{}, err
	}
	value, err := rotateSessionScript.Run(ctx, r.client, []string{redisKey("session", id)}, oldHash, newHash, ms).Text()
	if errors.Is(err, redis.Nil) {
		return core.Session{}, core.ErrUnauthorized
	}
	if err != nil {
		return core.Session{}, err
	}
	accountID, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return core.Session{}, core.ErrUnauthorized
	}
	return core.Session{ID: id, AccountID: accountID, RefreshHash: newHash}, nil
}

func (r *Redis) DeleteSession(ctx context.Context, id string) error {
	return r.client.Del(ctx, redisKey("session", id)).Err()
}

func (r *Redis) PutTicket(ctx context.Context, token string, grant core.Grant, ttl time.Duration) error {
	if _, err := ttlMillis(ttl); err != nil {
		return err
	}
	data, err := json.Marshal(grant)
	if err != nil {
		return err
	}
	ok, err := r.client.SetNX(ctx, redisKey("ticket", token), data, ttl).Result()
	if err != nil {
		return err
	}
	if !ok {
		return core.ErrConflict
	}
	return nil
}

func (r *Redis) ConsumeTicket(ctx context.Context, token string) (core.Grant, error) {
	data, err := r.client.GetDel(ctx, redisKey("ticket", token)).Bytes()
	if errors.Is(err, redis.Nil) {
		return core.Grant{}, core.ErrUnauthorized
	}
	if err != nil {
		return core.Grant{}, err
	}
	var grant core.Grant
	if err = json.Unmarshal(data, &grant); err != nil {
		return core.Grant{}, core.ErrUnauthorized
	}
	return grant, nil
}

func (r *Redis) AcquireLease(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	if _, err := ttlMillis(ttl); err != nil {
		return false, err
	}
	return r.client.SetNX(ctx, redisKey("lease", key), owner, ttl).Result()
}

var renewLeaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
`)

func (r *Redis) RenewLease(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	ms, err := ttlMillis(ttl)
	if err != nil {
		return false, err
	}
	n, err := renewLeaseScript.Run(ctx, r.client, []string{redisKey("lease", key)}, owner, ms).Int()
	return n == 1, err
}

var releaseLeaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
`)

func (r *Redis) ReleaseLease(ctx context.Context, key, owner string) error {
	return releaseLeaseScript.Run(ctx, r.client, []string{redisKey("lease", key)}, owner).Err()
}

var allowScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return n
`)

func (r *Redis) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	if limit <= 0 {
		return false, nil
	}
	ms, err := ttlMillis(window)
	if err != nil {
		return false, err
	}
	n, err := allowScript.Run(ctx, r.client, []string{redisKey("rate", key)}, ms).Int64()
	if err != nil {
		return false, err
	}
	return n <= int64(limit), nil
}

var _ core.State = (*Redis)(nil)

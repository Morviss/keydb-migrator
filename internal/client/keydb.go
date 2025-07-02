package client

import (
	"context"
	"fmt"
	"keydb-migrator/internal/config"
	"time"
	"github.com/go-redis/redis/v8"
)

// KeyDBClient wraps redis client with additional functionality
type KeyDBClient struct {
	client *redis.Client
	config config.KeyDBConfig
}

// New creates a new KeyDB client
func New(cfg config.KeyDBConfig) (*KeyDBClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.Database,
		PoolSize: cfg.PoolSize,
	})

	return &KeyDBClient{
		client: client,
		config: cfg,
	}, nil
}

// Ping tests the connection
func (k *KeyDBClient) Ping(ctx context.Context) error {
	return k.client.Ping(ctx).Err()
}

// Scan scans for keys matching pattern
func (k *KeyDBClient) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	return k.client.Scan(ctx, cursor, match, count)
}

// Type returns the type of key
func (k *KeyDBClient) Type(ctx context.Context, key string) *redis.StatusCmd {
	return k.client.Type(ctx, key)
}

// TTL returns the time to live of key
func (k *KeyDBClient) TTL(ctx context.Context, key string) *redis.DurationCmd {
	return k.client.TTL(ctx, key)
}

// Get returns the value of key
func (k *KeyDBClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return k.client.Get(ctx, key)
}

// Set sets the value of key
func (k *KeyDBClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return k.client.Set(ctx, key, value, expiration)
}

// HGetAll returns all fields and values of hash
func (k *KeyDBClient) HGetAll(ctx context.Context, key string) *redis.StringStringMapCmd {
	return k.client.HGetAll(ctx, key)
}

// HMSet sets multiple hash fields
func (k *KeyDBClient) HMSet(ctx context.Context, key string, values ...interface{}) *redis.BoolCmd {
	return k.client.HMSet(ctx, key, values...)
}

// LRange returns a range of elements from list
func (k *KeyDBClient) LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return k.client.LRange(ctx, key, start, stop)
}

// RPush appends elements to list
func (k *KeyDBClient) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return k.client.RPush(ctx, key, values...)
}

// SMembers returns all members of set
func (k *KeyDBClient) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	return k.client.SMembers(ctx, key)
}

// SAdd adds members to set
func (k *KeyDBClient) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return k.client.SAdd(ctx, key, members...)
}

// ZRangeWithScores returns range of elements from sorted set with scores
func (k *KeyDBClient) ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	return k.client.ZRangeWithScores(ctx, key, start, stop)
}

// ZAdd adds members to sorted set
func (k *KeyDBClient) ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return k.client.ZAdd(ctx, key, members...)
}

// Expire sets expiration for key
func (k *KeyDBClient) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return k.client.Expire(ctx, key, expiration)
}

// Close closes the connection
func (k *KeyDBClient) Close() error {
	return k.client.Close()
}

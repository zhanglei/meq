package storage

import (
	"fmt"

	"github.com/go-redis/redis"
)

// RedisStore redis store
type RedisStore struct {
	Client *redis.ClusterClient
}

// NewRedisStore new store
func NewRedisStore() *RedisStore {
	return &RedisStore{}
}

// Init init redis store
func (rs *RedisStore) Init(params map[string]interface{}) error {
	addrs, ok := params["addrs"]
	if !ok {
		return fmt.Errorf("unfind store addrs")
	}
	Option := &redis.ClusterOptions{
		Addrs:    addrs.([]string),
		PoolSize: 500,
	}
	rs.Client = redis.NewClusterClient(Option)
	return nil
}

// Subscription ...
func (rs *RedisStore) Subscription(ssid int32, ttype int, usid int32) error {
	return nil
}

// UnSubscription ...
func (rs *RedisStore) UnSubscription(ssid int32, ttype int, usid int32) error {
	return nil
}

// Store ...
func (rs *RedisStore) Store(ssid int32, ttype int, usid int32, payload []byte, msgid int64, ttl int64) error {
	return nil
}

// Get ...
func (rs *RedisStore) Get(ssid int32, ttype int, usid int32, limit int) ([][]byte, error) {
	return nil, nil
}

// Ack ...
func (rs *RedisStore) Ack(ssid int32, ttype int, usid int32, msgid int64) error {
	return nil
}

// Del ...
func (rs *RedisStore) Del(ssid int32, ttype int, usid int32, msgid int64) error {
	return nil
}

// Len ...
func Len(ssid int32, ttype int, usid int32) (int, error) {
	return 0, nil
}

// Name ...
func (rs *RedisStore) Name() string {
	return "redisStore"
}

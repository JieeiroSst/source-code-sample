package redis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yourusername/hexagon-app/internal/core/domain"
	"github.com/yourusername/hexagon-app/internal/core/ports"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	"go.uber.org/zap"
)

type cacheRepository struct {
	client *redis.Client
	ttl    time.Duration
	logger *zap.Logger
}

func NewCacheRepository(client *redis.Client, cfg *config.RedisConfig, logger *zap.Logger) ports.CacheRepository {
	return &cacheRepository{
		client: client,
		ttl:    cfg.CacheTTL,
		logger: logger,
	}
}

func (r *cacheRepository) Get(ctx context.Context, key string, dest interface{}) error {
	val, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return domain.ErrCacheNotFound
	}
	if err != nil {
		r.logger.Error("Failed to get from cache", zap.String("key", key), zap.Error(err))
		return err
	}

	if err := json.Unmarshal([]byte(val), dest); err != nil {
		r.logger.Error("Failed to unmarshal cache value", zap.String("key", key), zap.Error(err))
		return err
	}

	r.logger.Debug("Cache hit", zap.String("key", key))
	return nil
}

func (r *cacheRepository) Set(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		r.logger.Error("Failed to marshal cache value", zap.String("key", key), zap.Error(err))
		return err
	}

	if err := r.client.Set(ctx, key, data, r.ttl).Err(); err != nil {
		r.logger.Error("Failed to set cache", zap.String("key", key), zap.Error(err))
		return err
	}

	r.logger.Debug("Cache set", zap.String("key", key), zap.Duration("ttl", r.ttl))
	return nil
}

func (r *cacheRepository) Delete(ctx context.Context, key string) error {
	if err := r.client.Del(ctx, key).Err(); err != nil {
		r.logger.Error("Failed to delete from cache", zap.String("key", key), zap.Error(err))
		return err
	}

	r.logger.Debug("Cache deleted", zap.String("key", key))
	return nil
}

func (r *cacheRepository) Exists(ctx context.Context, key string) (bool, error) {
	count, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		r.logger.Error("Failed to check cache existence", zap.String("key", key), zap.Error(err))
		return false, err
	}

	return count > 0, nil
}

package redis

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	"go.uber.org/zap"
)

var (
	once   sync.Once
	client *redis.Client
	err    error
)

func NewRedisClient(cfg *config.RedisConfig, logger *zap.Logger) (*redis.Client, error) {
	once.Do(func() {
		client = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
			Password: cfg.Password,
			DB:       cfg.DB,
		})
		ctx := context.Background()
		err = client.Ping(ctx).Err()
		if err == nil {
			logger.Info("Redis connected")
		}
	})
	return client, err
}

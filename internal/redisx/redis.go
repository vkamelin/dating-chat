package redisx

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"meet-you-chat/internal/config"
)

func New(cfg config.Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:            cfg.RedisAddr,
		Password:        cfg.RedisPassword,
		DB:              cfg.RedisDB,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DialTimeout:     5 * time.Second,
		PoolTimeout:     5 * time.Second,
		MinIdleConns:    1,
		MaxIdleConns:    5,
		ConnMaxIdleTime: 5 * time.Minute,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}


package events

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"meet-you-chat/internal/chat"
	"meet-you-chat/internal/config"
)

type Consumer struct {
	cfg    config.Config
	svc    *chat.Service
	logger *slog.Logger
	redis  *redis.Client
}

func NewConsumer(cfg config.Config, svc *chat.Service, logger *slog.Logger, redisClient *redis.Client) *Consumer {
	return &Consumer{cfg: cfg, svc: svc, logger: logger, redis: redisClient}
}

func (c *Consumer) Run(ctx context.Context) error {
	if err := c.ensureGroup(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		streams, err := c.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.cfg.RedisGroup,
			Consumer: c.cfg.RedisConsumer,
			Streams:  []string{c.cfg.RedisStream, ">"},
			Count:    10,
			Block:    5 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Error("redis stream read failed", "error", err)
			time.Sleep(time.Second)
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				if err := c.handleMessage(ctx, message); err == nil {
					_ = c.redis.XAck(ctx, c.cfg.RedisStream, c.cfg.RedisGroup, message.ID).Err()
				}
			}
		}
	}
}

func (c *Consumer) Close() error {
	return nil
}

func (c *Consumer) ensureGroup(ctx context.Context) error {
	err := c.redis.XGroupCreateMkStream(ctx, c.cfg.RedisStream, c.cfg.RedisGroup, "0").Err()
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func (c *Consumer) handleMessage(ctx context.Context, msg redis.XMessage) error {
	eventType, _ := msg.Values["type"].(string)
	fields := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		switch vv := v.(type) {
		case string:
			fields[k] = vv
		case []byte:
			fields[k] = string(vv)
		default:
			b, _ := json.Marshal(vv)
			fields[k] = string(b)
		}
	}
	return c.svc.ProcessStreamMessage(ctx, eventType, fields)
}

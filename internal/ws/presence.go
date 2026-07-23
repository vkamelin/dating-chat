package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"meet-you-chat/internal/chat"
	"meet-you-chat/internal/config"
)

const presenceKeyPrefix = "chat_presence:user:"

type presenceEvent struct {
	UserID    uint64    `json:"user_id"`
	IsOnline  bool      `json:"is_online"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Presence keeps a short-lived, per-connection index in Redis. A sorted set
// avoids marking a user offline while they are connected from another device.
type Presence struct {
	cfg    config.Config
	redis  *redis.Client
	repo   *chat.Repository
	hub    *Hub
	logger *slog.Logger
}

func NewPresence(cfg config.Config, redisClient *redis.Client, repo *chat.Repository, hub *Hub, logger *slog.Logger) *Presence {
	return &Presence{cfg: cfg, redis: redisClient, repo: repo, hub: hub, logger: logger}
}

func (p *Presence) Connect(ctx context.Context, userID uint64, connectionID string, now time.Time) {
	if p.updateConnection(ctx, userID, connectionID, now) {
		p.publish(ctx, presenceEvent{UserID: userID, IsOnline: true, UpdatedAt: now.UTC()})
	}
}

func (p *Presence) Refresh(ctx context.Context, userID uint64, connectionID string, now time.Time) {
	if p.updateConnection(ctx, userID, connectionID, now) {
		p.publish(ctx, presenceEvent{UserID: userID, IsOnline: true, UpdatedAt: now.UTC()})
	}
}

func (p *Presence) Disconnect(ctx context.Context, userID uint64, connectionID string, now time.Time) {
	key := presenceKey(userID)
	result, err := p.redis.Eval(ctx, `
local now = tonumber(ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local was_online = redis.call('ZCARD', KEYS[1]) > 0
redis.call('ZREM', KEYS[1], ARGV[2])
local is_online = redis.call('ZCARD', KEYS[1]) > 0
if is_online then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3])) else redis.call('DEL', KEYS[1]) end
if was_online and not is_online then return 1 end
return 0
`, []string{key}, now.Unix(), connectionID, p.cfg.PresenceTTLSeconds).Int()
	if err != nil {
		p.logger.Warn("presence disconnect failed", "user_id", userID, "error", err)
		return
	}
	if result == 1 {
		p.publish(context.Background(), presenceEvent{UserID: userID, IsOnline: false, UpdatedAt: now.UTC()})
	}
}

func (p *Presence) Run(ctx context.Context) error {
	pubsub := p.redis.Subscribe(ctx, p.cfg.PresenceChannel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		return err
	}
	channel := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-channel:
			if !ok {
				return fmt.Errorf("presence subscription closed")
			}
			var event presenceEvent
			if err := json.Unmarshal([]byte(message.Payload), &event); err != nil || event.UserID == 0 {
				p.logger.Warn("invalid presence event")
				continue
			}
			p.deliver(event)
		}
	}
}

func (p *Presence) updateConnection(ctx context.Context, userID uint64, connectionID string, now time.Time) bool {
	result, err := p.redis.Eval(ctx, `
local now = tonumber(ARGV[1])
local expires_at = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local was_online = redis.call('ZCARD', KEYS[1]) > 0
redis.call('ZADD', KEYS[1], expires_at, ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
if not was_online then return 1 end
return 0
`, []string{presenceKey(userID)}, now.Unix(), now.Add(time.Duration(p.cfg.PresenceTTLSeconds)*time.Second).Unix(), connectionID, p.cfg.PresenceTTLSeconds).Int()
	if err != nil {
		p.logger.Warn("presence refresh failed", "user_id", userID, "error", err)
		return false
	}
	return result == 1
}

func (p *Presence) publish(ctx context.Context, event presenceEvent) {
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}
	if err := p.redis.Publish(ctx, p.cfg.PresenceChannel, payload).Err(); err != nil {
		p.logger.Warn("presence publish failed", "user_id", event.UserID, "error", err)
	}
}

func (p *Presence) deliver(event presenceEvent) {
	recipients, err := p.repo.LoadPresenceRecipients(context.Background(), event.UserID)
	if err != nil {
		p.logger.Warn("presence recipients load failed", "user_id", event.UserID, "error", err)
		return
	}
	p.hub.BroadcastUsers(recipients, mustJSON(NewPresenceUpdatedEvent(event.UserID, event.IsOnline, event.UpdatedAt)))
}

func presenceKey(userID uint64) string {
	return presenceKeyPrefix + strconv.FormatUint(userID, 10)
}

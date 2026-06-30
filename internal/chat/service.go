package chat

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"meet-you-chat/internal/config"
)

type Service struct {
	cfg    config.Config
	repo   *Repository
	hub    Broadcaster
	logger *slog.Logger
	limit  *rateLimiter
}

type Broadcaster interface {
	BroadcastUsers([]uint64, []byte)
	BroadcastUsersExcluding([]uint64, []uint64, []byte)
}

func NewService(cfg config.Config, repo *Repository, hub Broadcaster, logger *slog.Logger) *Service {
	return &Service{
		cfg:    cfg,
		repo:   repo,
		hub:    hub,
		logger: logger,
		limit:  newRateLimiter(cfg.ChatMessageRatePerMin),
	}
}

func (s *Service) Repository() *Repository {
	return s.repo
}

type SendRequest struct {
	RequestID       string
	ChatID          uint64
	ClientMessageID string
	Body            string
}

type ReadRequest struct {
	RequestID       string
	ChatID          uint64
	LastReadMessage uint64
}

func (s *Service) SendMessage(ctx context.Context, userID uint64, req SendRequest) (*Message, error) {
	if !s.limit.Allow(userID) {
		return nil, NewServiceError("WS_RATE_LIMIT_EXCEEDED", "Rate limit exceeded.")
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	chatRow, err := s.repo.LoadChatForUpdate(ctx, tx, req.ChatID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, NewServiceError("CHAT_NOT_FOUND", "Chat not found.")
		}
		return nil, err
	}
	if !s.repo.LoadAllowedChatStatus(chatRow.Status) {
		return nil, NewServiceError("CHAT_NOT_ACTIVE", "Chat is not active.")
	}
	participant, err := s.repo.LoadChatParticipant(ctx, tx, req.ChatID, userID)
	if err != nil {
		return nil, err
	}
	if !participant {
		return nil, NewServiceError("CHAT_ACCESS_DENIED", "Access denied.")
	}

	existing, err := s.repo.LoadMessageByClientMessageID(ctx, tx, req.ChatID, userID, req.ClientMessageID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return existing, nil
	}

	seq, err := s.repo.NextSequenceNumber(ctx, tx, req.ChatID)
	if err != nil {
		return nil, err
	}

	msg, err := s.repo.InsertUserMessage(ctx, tx, chatRow, userID, req.ClientMessageID, req.Body, seq)
	if err != nil {
		return nil, NewServiceError("CHAT_MESSAGE_SAVE_FAILED", "Failed to save message.")
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	participants, err := s.repo.LoadChatParticipants(ctx, req.ChatID)
	if err == nil {
		s.broadcastToUsers(participants, map[string]any{
			"type":    "chat.message.created",
			"message": msg,
		})
	}
	return msg, nil
}

func (s *Service) MarkRead(ctx context.Context, userID, chatID, lastReadMessageID uint64) (time.Time, error) {
	contains, err := s.repo.ChatContainsMessage(ctx, chatID, lastReadMessageID)
	if err != nil {
		return time.Time{}, err
	}
	if !contains {
		return time.Time{}, NewServiceError("CHAT_READ_INVALID_MESSAGE", "Message does not belong to chat.")
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	participant, err := s.repo.LoadChatParticipant(ctx, tx, chatID, userID)
	if err != nil {
		return time.Time{}, err
	}
	if !participant {
		return time.Time{}, NewServiceError("CHAT_ACCESS_DENIED", "Access denied.")
	}

	readAt, err := s.repo.UpdateParticipantRead(ctx, tx, chatID, userID, lastReadMessageID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, NewServiceError("CHAT_READ_INVALID_MESSAGE", "Message does not belong to chat.")
		}
		return time.Time{}, err
	}

	if err := tx.Commit(); err != nil {
		return time.Time{}, err
	}
	return readAt, nil
}

func (s *Service) ProcessStreamMessage(ctx context.Context, eventType string, fields map[string]string) error {
	switch eventType {
	case "chat.message.created":
		chatID, err := parseUintField(fields["chat_id"])
		if err != nil {
			return err
		}
		messageID, err := parseUintField(fields["message_id"])
		if err != nil {
			return err
		}
		msg, err := s.repo.LoadMessageByID(ctx, messageID)
		if err != nil {
			return err
		}
		if msg == nil || msg.ChatID != chatID {
			return nil
		}
		if strings.EqualFold(msg.MessageType, "system") {
			buttons, err := s.repo.LoadButtonsForMessage(ctx, msg.ID)
			if err == nil {
				msg.Buttons = buttons
			}
		}
		participants, err := s.repo.LoadChatParticipants(ctx, chatID)
		if err == nil {
			s.broadcastToUsers(participants, map[string]any{
				"type":    "chat.message.created",
				"message": msg,
			})
		}
	case "chat.message.updated":
		chatID, err := parseUintField(fields["chat_id"])
		if err != nil {
			return err
		}
		messageID, err := parseUintField(fields["message_id"])
		if err != nil {
			return err
		}
		msg, err := s.repo.LoadMessageByID(ctx, messageID)
		if err != nil {
			return err
		}
		if msg == nil || msg.ChatID != chatID {
			return nil
		}
		participants, err := s.repo.LoadChatParticipants(ctx, chatID)
		if err == nil {
			s.broadcastToUsers(participants, map[string]any{
				"type":    "chat.message.updated",
				"message": msg,
			})
		}
	case "chat.read.updated":
		chatID, err := parseUintField(fields["chat_id"])
		if err != nil {
			return err
		}
		userID, err := parseUintField(fields["user_id"])
		if err != nil {
			return err
		}
		lastReadMessageID, err := parseUintField(fields["last_read_message_id"])
		if err != nil {
			return err
		}
		readAt := time.Now().UTC()
		participants, err := s.repo.LoadChatParticipants(ctx, chatID)
		if err == nil {
			s.broadcastToUsersExcluding(participants, []uint64{userID}, map[string]any{
				"type":                 "chat.read.updated",
				"chat_id":              chatID,
				"user_id":              userID,
				"last_read_message_id": lastReadMessageID,
				"read_at":              readAt.UTC(),
			})
		}
	case "chat.status.changed":
		chatID, err := parseUintField(fields["chat_id"])
		if err != nil {
			return err
		}
		participants, err := s.repo.LoadChatParticipants(ctx, chatID)
		if err == nil {
			s.broadcastRawToUsers(participants, fields)
		}
	default:
		return nil
	}
	return nil
}

func (s *Service) broadcastToUsers(userIDs []uint64, event any) {
	payload, err := json.Marshal(event)
	if err != nil {
		s.logger.Error("marshal event failed", "error", err)
		return
	}
	s.hub.BroadcastUsers(userIDs, payload)
}

func (s *Service) broadcastToUsersExcluding(userIDs []uint64, excluded []uint64, event any) {
	payload, err := json.Marshal(event)
	if err != nil {
		s.logger.Error("marshal event failed", "error", err)
		return
	}
	s.hub.BroadcastUsersExcluding(userIDs, excluded, payload)
}

func (s *Service) broadcastRawToUsers(userIDs []uint64, fields map[string]string) {
	payload, err := json.Marshal(fields)
	if err != nil {
		s.logger.Error("marshal event failed", "error", err)
		return
	}
	s.hub.BroadcastUsers(userIDs, payload)
}

func parseUintField(v string) (uint64, error) {
	var n uint64
	_, err := fmt.Sscanf(strings.TrimSpace(v), "%d", &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

type ServiceError struct {
	Code    string
	Message string
}

func (e *ServiceError) Error() string { return e.Message }

func NewServiceError(code, message string) *ServiceError {
	return &ServiceError{Code: code, Message: message}
}

type rateLimiter struct {
	mu       sync.Mutex
	rate     float64
	burst    float64
	buckets  map[uint64]*rateBucket
}

type rateBucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(perMinute int) *rateLimiter {
	rate := float64(perMinute) / 60.0
	return &rateLimiter{
		rate:    rate,
		burst:   float64(perMinute),
		buckets: make(map[uint64]*rateBucket),
	}
}

func (r *rateLimiter) Allow(userID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.buckets[userID]
	now := time.Now()
	if !ok {
		r.buckets[userID] = &rateBucket{tokens: r.burst - 1, last: now}
		return true
	}
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * r.rate
	if b.tokens > r.burst {
		b.tokens = r.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

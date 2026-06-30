package ws

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

type Hub struct {
	mu      sync.RWMutex
	users   map[uint64]map[string]*Client
	logger  *slog.Logger
}

func NewHub(logger *slog.Logger) *Hub {
	return &Hub{
		users:  make(map[uint64]map[string]*Client),
		logger: logger,
	}
}

type Client struct {
	UserID       uint64
	SessionID    string
	ConnectionID string
	Conn         *websocket.Conn
	Send         chan []byte
	Cancel       context.CancelFunc
}

func NewClient(userID uint64, sessionID, connectionID string, conn *websocket.Conn, cancel context.CancelFunc) *Client {
	return &Client{
		UserID:       userID,
		SessionID:    sessionID,
		ConnectionID: connectionID,
		Conn:         conn,
		Send:         make(chan []byte, 32),
		Cancel:       cancel,
	}
}

func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.users[c.UserID]; !ok {
		h.users[c.UserID] = make(map[string]*Client)
	}
	h.users[c.UserID][c.ConnectionID] = c
}

func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if userClients, ok := h.users[c.UserID]; ok {
		delete(userClients, c.ConnectionID)
		if len(userClients) == 0 {
			delete(h.users, c.UserID)
		}
	}
}

func (h *Hub) BroadcastUsers(userIDs []uint64, payload []byte) {
	if len(userIDs) == 0 {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, userID := range userIDs {
		for _, client := range h.users[userID] {
			client.Enqueue(payload)
		}
	}
}

func (h *Hub) BroadcastUsersExcluding(userIDs []uint64, excluded []uint64, payload []byte) {
	exclude := make(map[uint64]struct{}, len(excluded))
	for _, id := range excluded {
		exclude[id] = struct{}{}
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, userID := range userIDs {
		if _, skip := exclude[userID]; skip {
			continue
		}
		for _, client := range h.users[userID] {
			client.Enqueue(payload)
		}
	}
}

func (h *Hub) CloseAll() {
	h.mu.RLock()
	clients := make([]*Client, 0)
	for _, userClients := range h.users {
		for _, c := range userClients {
			clients = append(clients, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range clients {
		c.Cancel()
		_ = c.Conn.Close(websocket.StatusNormalClosure, "shutdown")
	}
}

func (c *Client) Enqueue(payload []byte) {
	select {
	case c.Send <- payload:
	default:
		c.Cancel()
	}
}

func NewChatMessageCreatedEvent(msg any) map[string]any {
	return map[string]any{
		"type":    "chat.message.created",
		"message": msg,
	}
}

func NewChatMessageUpdatedEvent(msg any) map[string]any {
	return map[string]any{
		"type":    "chat.message.updated",
		"message": msg,
	}
}

func NewChatReadUpdatedEvent(chatID, userID, lastReadMessageID uint64, readAt time.Time) map[string]any {
	return map[string]any{
		"type":                 "chat.read.updated",
		"chat_id":              chatID,
		"user_id":              userID,
		"last_read_message_id": lastReadMessageID,
		"read_at":              readAt.UTC(),
	}
}

func NewErrorEvent(requestID, code, message string) map[string]any {
	return map[string]any{
		"type":       "error",
		"request_id": requestID,
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	}
}

func NewPongEvent(requestID string) map[string]any {
	return map[string]any{
		"type":       "pong",
		"request_id": requestID,
	}
}

func NewChatMessageSentEvent(requestID string, msg *chatMessageDTO) map[string]any {
	return map[string]any{
		"type":       "chat.message.sent",
		"request_id": requestID,
		"message":    msg,
	}
}

func NewChatReadOKEvent(requestID string, chatID, lastReadMessageID uint64, readAt time.Time) map[string]any {
	return map[string]any{
		"type":                 "chat.read.ok",
		"request_id":           requestID,
		"chat_id":              chatID,
		"last_read_message_id": lastReadMessageID,
		"read_at":              readAt.UTC(),
	}
}

type chatMessageDTO struct {
	ID              uint64          `json:"id"`
	ChatID          uint64          `json:"chat_id"`
	SequenceNumber  uint64          `json:"sequence_number"`
	MessageType     string          `json:"message_type"`
	SystemType      *string         `json:"system_type"`
	SenderUserID    *uint64         `json:"sender_user_id"`
	ClientMessageID *string         `json:"client_message_id,omitempty"`
	Body            *string         `json:"body"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	Buttons         any             `json:"buttons,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
}

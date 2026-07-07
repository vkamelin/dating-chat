package ws

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"nhooyr.io/websocket"

	"meet-you-chat/internal/auth"
	"meet-you-chat/internal/chat"
	"meet-you-chat/internal/config"
)

type Handler struct {
	cfg    config.Config
	auth   *auth.Authenticator
	svc    *chat.Service
	hub    *Hub
	redis  *redis.Client
	logger *slog.Logger
}

func NewHandler(cfg config.Config, authenticator *auth.Authenticator, svc *chat.Service, hub *Hub, redisClient *redis.Client, logger *slog.Logger) *Handler {
	return &Handler{cfg: cfg, auth: authenticator, svc: svc, hub: hub, redis: redisClient, logger: logger}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token, code, message := h.extractToken(r)
	if code != "" {
		writeHTTPError(w, http.StatusUnauthorized, code, message)
		return
	}

	identity, err := h.auth.Authenticate(r.Context(), token)
	if err != nil {
		writeHTTPError(w, http.StatusUnauthorized, "AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.")
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	connectionID := newUUID()
	client := NewClient(identity.UserID, identity.SessionID, connectionID, conn, cancel)
	h.hub.Register(client)

	presenceKey := "chat_online_user:" + strconv.FormatUint(identity.UserID, 10) + ":" + identity.SessionID + ":" + connectionID
	connectedAt := time.Now().UTC()
	h.refreshPresence(ctx, presenceKey, identity.UserID, identity.SessionID, connectedAt, connectedAt)

	go h.presenceLoop(ctx, presenceKey, identity.UserID, identity.SessionID, connectedAt)
	go h.writeLoop(ctx, client)
	h.readLoop(ctx, client, presenceKey)
}

func (h *Handler) extractToken(r *http.Request) (string, string, string) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if header != "" && !strings.Contains(header, ",") && strings.HasPrefix(strings.ToLower(header), "bearer ") {
		token := strings.TrimSpace(header[len("Bearer "):])
		if token != "" {
			return token, "", ""
		}
	}
	return "", "AUTH_ACCESS_TOKEN_REQUIRED", "Access token is required."
}

func (h *Handler) readLoop(ctx context.Context, client *Client, presenceKey string) {
	defer func() {
		h.hub.Unregister(client)
		_ = h.redis.Del(context.Background(), presenceKey).Err()
		_ = client.Conn.Close(websocket.StatusNormalClosure, "bye")
		client.Cancel()
	}()

	for {
		msgType, data, err := client.Conn.Read(ctx)
		if err != nil {
			return
		}
		if msgType != websocket.MessageText || int64(len(data)) > h.cfg.WSMaxMessageBytes {
			h.sendError(client, "", "WS_INVALID_MESSAGE", "Invalid message.")
			continue
		}

		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			h.sendError(client, "", "WS_INVALID_MESSAGE", "Invalid message.")
			continue
		}

		switch envelope.Type {
		case "ping":
			var req struct {
				RequestID string `json:"request_id"`
			}
			if err := json.Unmarshal(data, &req); err != nil || !isUUID(req.RequestID) {
				h.sendError(client, "", "WS_INVALID_MESSAGE", "Invalid message.")
				continue
			}
			h.sendJSON(client, NewPongEvent(req.RequestID))
		case "chat.message.send":
			var req struct {
				RequestID       string `json:"request_id"`
				ChatID          uint64 `json:"chat_id"`
				ClientMessageID string `json:"client_message_id"`
				Body            string `json:"body"`
			}
			if err := json.Unmarshal(data, &req); err != nil || !isUUID(req.RequestID) || req.ChatID == 0 {
				h.sendError(client, req.RequestID, "WS_INVALID_MESSAGE", "Invalid message.")
				continue
			}
			if !isUUID(req.ClientMessageID) {
				h.sendError(client, req.RequestID, "CHAT_CLIENT_MESSAGE_ID_INVALID", "Client message id is invalid.")
				continue
			}
			body := strings.TrimSpace(req.Body)
			if body == "" {
				h.sendError(client, req.RequestID, "CHAT_MESSAGE_BODY_REQUIRED", "Message body is required.")
				continue
			}
			if len([]rune(body)) > h.cfg.ChatMessageMaxLength {
				h.sendError(client, req.RequestID, "CHAT_MESSAGE_BODY_TOO_LONG", "Message body is too long.")
				continue
			}
			msg, err := h.svc.SendMessage(ctx, client.UserID, chat.SendRequest{
				RequestID:       req.RequestID,
				ChatID:          req.ChatID,
				ClientMessageID: req.ClientMessageID,
				Body:            body,
			})
			if err != nil {
				h.sendServiceError(client, req.RequestID, err)
				continue
			}
			h.sendJSON(client, NewChatMessageSentEvent(req.RequestID, toDTO(msg)))
		case "chat.read":
			var req struct {
				RequestID        string `json:"request_id"`
				ChatID           uint64 `json:"chat_id"`
				LastReadMessageID uint64 `json:"last_read_message_id"`
			}
			if err := json.Unmarshal(data, &req); err != nil || !isUUID(req.RequestID) || req.ChatID == 0 || req.LastReadMessageID == 0 {
				h.sendError(client, req.RequestID, "WS_INVALID_MESSAGE", "Invalid message.")
				continue
			}
			readAt, err := h.svc.MarkRead(ctx, client.UserID, req.ChatID, req.LastReadMessageID)
			if err != nil {
				h.sendServiceError(client, req.RequestID, err)
				continue
			}
			h.sendJSON(client, NewChatReadOKEvent(req.RequestID, req.ChatID, req.LastReadMessageID, readAt))
			h.notifyReadUpdate(req.ChatID, client.UserID, req.LastReadMessageID, readAt)
		default:
			h.sendError(client, "", "WS_UNSUPPORTED_MESSAGE_TYPE", "Unsupported message type.")
		}
	}
}

func (h *Handler) writeLoop(ctx context.Context, client *Client) {
	pingTicker := time.NewTicker(time.Duration(h.cfg.WSPingIntervalSeconds) * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case payload, ok := <-client.Send:
			if !ok {
				return
			}
			if err := client.Conn.Write(ctx, websocket.MessageText, payload); err != nil {
				client.Cancel()
				return
			}
		case <-pingTicker.C:
			pingCtx, cancel := context.WithTimeout(ctx, time.Duration(h.cfg.WSPongTimeoutSeconds)*time.Second)
			if err := client.Conn.Ping(pingCtx); err != nil {
				cancel()
				client.Cancel()
				return
			}
			cancel()
		case <-ctx.Done():
			return
		}
	}
}

func (h *Handler) presenceLoop(ctx context.Context, key string, userID uint64, sessionID string, connectedAt time.Time) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.refreshPresence(ctx, key, userID, sessionID, connectedAt, time.Now().UTC())
		case <-ctx.Done():
			return
		}
	}
}

func (h *Handler) refreshPresence(ctx context.Context, key string, userID uint64, sessionID string, connectedAt, lastSeenAt time.Time) {
	payload := map[string]any{
		"user_id":      userID,
		"session_id":   sessionID,
		"connected_at": connectedAt.UTC(),
		"last_seen_at": lastSeenAt.UTC(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_ = h.redis.Set(ctx, key, data, 90*time.Second).Err()
}

func (h *Handler) sendJSON(client *Client, v any) {
	payload, err := json.Marshal(v)
	if err != nil {
		return
	}
	client.Enqueue(payload)
}

func (h *Handler) sendError(client *Client, requestID, code, message string) {
	h.sendJSON(client, NewErrorEvent(requestID, code, message))
}

func (h *Handler) sendServiceError(client *Client, requestID string, err error) {
	var se *chat.ServiceError
	if errors.As(err, &se) {
		h.sendError(client, requestID, se.Code, se.Message)
		return
	}
	h.sendError(client, requestID, "CHAT_MESSAGE_SAVE_FAILED", "Failed to save message.")
}

func (h *Handler) notifyReadUpdate(chatID, userID, lastReadMessageID uint64, readAt time.Time) {
	participants, err := h.svc.Repository().LoadChatParticipants(context.Background(), chatID)
	if err != nil {
		return
	}
	h.hub.BroadcastUsersExcluding(participants, []uint64{userID}, mustJSON(NewChatReadUpdatedEvent(chatID, userID, lastReadMessageID, readAt)))
}

func toDTO(msg *chat.Message) *chatMessageDTO {
	if msg == nil {
		return nil
	}
	dto := &chatMessageDTO{
		ID:             msg.ID,
		ChatID:         msg.ChatID,
		SequenceNumber: msg.SequenceNumber,
		MessageType:    msg.MessageType,
		SystemType:     msg.SystemType,
		SenderUserID:   msg.SenderUserID,
		Body:           msg.Body,
		Payload:        msg.Payload,
		Buttons:        msg.Buttons,
		CreatedAt:      msg.CreatedAt,
	}
	if msg.ClientMessageID != nil {
		cm := *msg.ClientMessageID
		dto.ClientMessageID = &cm
	}
	return dto
}

func writeHTTPError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "error",
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return []byte(`{"type":"error","error":{"code":"WS_INVALID_MESSAGE","message":"Invalid message."}}`)
	}
	return data
}

func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	dst := make([]byte, 36)
	hex.Encode(dst[0:8], b[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], b[10:16])
	return string(dst)
}

func isUUID(v string) bool {
	if len(v) != 36 {
		return false
	}
	for i, r := range v {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
				return false
			}
		}
	}
	return true
}

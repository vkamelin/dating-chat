package chat

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

type Chat struct {
	ID                uint64
	UserAID           uint64
	UserBID           uint64
	Status            string
	UserMessageCount   uint64
	LastMessageID     sql.NullInt64
	LastMessageAt     sql.NullTime
	LastUserMessageAt sql.NullTime
	UpdatedAt         time.Time
}

type Message struct {
	ID              uint64          `json:"id"`
	ChatID          uint64          `json:"chat_id"`
	SequenceNumber  uint64          `json:"sequence_number"`
	MessageType     string          `json:"message_type"`
	SystemType      *string         `json:"system_type"`
	SenderUserID    *uint64         `json:"sender_user_id"`
	ClientMessageID *string         `json:"client_message_id,omitempty"`
	Body            *string         `json:"body"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	Buttons         []MessageButton `json:"buttons,omitempty"`
	Status          string          `json:"-"`
	CreatedByService string         `json:"-"`
	CreatedAt       time.Time       `json:"created_at"`
}

type MessageButton struct {
	Key        string `json:"key"`
	Label      string `json:"label"`
	ActionType string `json:"action_type"`
	Style      *string `json:"style,omitempty"`
	IsEnabled  bool   `json:"is_enabled"`
}

type ReadResult struct {
	ReadAt time.Time
}

func (r *Repository) LoadChatForUpdate(ctx context.Context, tx *sql.Tx, chatID uint64) (Chat, error) {
	var c Chat
	var lastMessageID sql.NullInt64
	var lastMessageAt sql.NullTime
	var lastUserMessageAt sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT id, user_a_id, user_b_id, status, user_message_count, last_message_id, last_message_at, last_user_message_at, updated_at
		FROM chats
		WHERE id = ?
		FOR UPDATE
	`, chatID).Scan(&c.ID, &c.UserAID, &c.UserBID, &c.Status, &c.UserMessageCount, &lastMessageID, &lastMessageAt, &lastUserMessageAt, &c.UpdatedAt)
	if err != nil {
		return Chat{}, err
	}
	c.LastMessageID = lastMessageID
	c.LastMessageAt = lastMessageAt
	c.LastUserMessageAt = lastUserMessageAt
	return c, nil
}

func (r *Repository) LoadChatParticipant(ctx context.Context, tx *sql.Tx, chatID, userID uint64) (bool, error) {
	var id uint64
	err := tx.QueryRowContext(ctx, `
		SELECT id
		FROM chat_participants
		WHERE chat_id = ? AND user_id = ?
		LIMIT 1
	`, chatID, userID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *Repository) LoadOtherParticipantIDs(ctx context.Context, tx *sql.Tx, chatID, excludeUserID uint64) ([]uint64, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT user_id
		FROM chat_participants
		WHERE chat_id = ? AND user_id <> ?
	`, chatID, excludeUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uint64
	for rows.Next() {
		var userID uint64
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		ids = append(ids, userID)
	}
	return ids, rows.Err()
}

func (r *Repository) LoadMessageByClientMessageID(ctx context.Context, tx *sql.Tx, chatID, senderUserID uint64, clientMessageID string) (*Message, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT id, chat_id, sequence_number, message_type, system_type, sender_user_id, client_message_id, body, payload_json, status, created_by_service, created_at
		FROM chat_messages
		WHERE chat_id = ? AND sender_user_id = ? AND client_message_id = ?
		LIMIT 1
	`, chatID, senderUserID, clientMessageID)
	msg, err := scanMessage(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &msg, nil
}

func (r *Repository) LoadMessageByID(ctx context.Context, messageID uint64) (*Message, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, chat_id, sequence_number, message_type, system_type, sender_user_id, client_message_id, body, payload_json, status, created_by_service, created_at
		FROM chat_messages
		WHERE id = ?
		LIMIT 1
	`, messageID)
	msg, err := scanMessage(row)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (r *Repository) LoadButtonsForMessage(ctx context.Context, messageID uint64) ([]MessageButton, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT button_key, label, action_type, style, is_enabled
		FROM chat_message_buttons
		WHERE message_id = ?
		ORDER BY sort_order ASC, id ASC
	`, messageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MessageButton
	for rows.Next() {
		var b MessageButton
		var style sql.NullString
		var isEnabled int64
		if err := rows.Scan(&b.Key, &b.Label, &b.ActionType, &style, &isEnabled); err != nil {
			return nil, err
		}
		if style.Valid {
			v := style.String
			b.Style = &v
		}
		b.IsEnabled = isEnabled != 0
		out = append(out, b)
	}
	return out, rows.Err()
}

func (r *Repository) LoadChatParticipants(ctx context.Context, chatID uint64) ([]uint64, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT user_id
		FROM chat_participants
		WHERE chat_id = ?
	`, chatID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uint64
	for rows.Next() {
		var userID uint64
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		ids = append(ids, userID)
	}
	return ids, rows.Err()
}

// LoadPresenceRecipients returns only users who may see the caller's presence.
func (r *Repository) LoadPresenceRecipients(ctx context.Context, userID uint64) ([]uint64, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT DISTINCT p_other.user_id
		FROM chats c
		INNER JOIN chat_participants p_me ON p_me.chat_id = c.id AND p_me.user_id = ?
		INNER JOIN chat_participants p_other ON p_other.chat_id = c.id AND p_other.user_id <> ?
		WHERE c.status IN ('active', 'date_suggested', 'warning_inactive')
	`, userID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *Repository) InsertUserMessage(ctx context.Context, tx *sql.Tx, chat Chat, senderUserID uint64, clientMessageID, body string, sequenceNumber uint64) (*Message, error) {
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO chat_messages (
			chat_id, sequence_number, message_type, system_type, sender_user_id,
			client_message_id, body, payload_json, status, created_by_service, created_at
		) VALUES (?, ?, 'user', NULL, ?, ?, ?, NULL, 'created', 'chat-ws', ?)
	`, chat.ID, sequenceNumber, senderUserID, clientMessageID, body, now)
	if err != nil {
		return nil, err
	}
	messageID, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO chat_message_receipts (
			message_id, chat_id, user_id, delivered_at, read_at, created_at, updated_at
		)
		SELECT ?, ?, user_id, NULL, NULL, ?, ?
		FROM chat_participants
		WHERE chat_id = ? AND user_id <> ?
	`, messageID, chat.ID, now, now, chat.ID, senderUserID); err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE chats
		SET user_message_count = user_message_count + 1,
			last_message_id = ?,
			last_message_at = ?,
			last_user_message_at = ?,
			updated_at = ?
		WHERE id = ?
	`, messageID, now, now, now, chat.ID); err != nil {
		return nil, err
	}

	bodyCopy := body
	cmID := clientMessageID
	msg := &Message{
		ID:               uint64(messageID),
		ChatID:           chat.ID,
		SequenceNumber:   sequenceNumber,
		MessageType:      "user",
		SenderUserID:     &senderUserID,
		ClientMessageID:  &cmID,
		Body:             &bodyCopy,
		Status:           "created",
		CreatedByService: "chat-ws",
		CreatedAt:        now,
	}
	return msg, nil
}

func (r *Repository) UpdateParticipantRead(ctx context.Context, tx *sql.Tx, chatID, userID, lastReadMessageID uint64) (time.Time, error) {
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		UPDATE chat_participants
		SET last_read_message_id = ?,
			last_read_at = ?
		WHERE chat_id = ? AND user_id = ?
	`, lastReadMessageID, now, chatID, userID)
	if err != nil {
		return time.Time{}, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return time.Time{}, err
	}
	if affected == 0 {
		return time.Time{}, sql.ErrNoRows
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE chat_message_receipts
		SET read_at = COALESCE(read_at, ?),
			updated_at = ?
		WHERE chat_id = ? AND user_id = ? AND message_id <= ?
	`, now, now, chatID, userID, lastReadMessageID); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

func (r *Repository) ChatContainsMessage(ctx context.Context, chatID, messageID uint64) (bool, error) {
	var id uint64
	err := r.db.QueryRowContext(ctx, `
		SELECT id
		FROM chat_messages
		WHERE id = ? AND chat_id = ?
		LIMIT 1
	`, messageID, chatID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func scanMessage(row interface {
	Scan(dest ...any) error
}) (Message, error) {
	var msg Message
	var systemType sql.NullString
	var senderUserID sql.NullInt64
	var clientMessageID sql.NullString
	var body sql.NullString
	var payloadJSON sql.NullString
	var messageType string
	var status string
	var createdByService string
	var createdAt time.Time
	if err := row.Scan(
		&msg.ID,
		&msg.ChatID,
		&msg.SequenceNumber,
		&messageType,
		&systemType,
		&senderUserID,
		&clientMessageID,
		&body,
		&payloadJSON,
		&status,
		&createdByService,
		&createdAt,
	); err != nil {
		return Message{}, err
	}
	msg.MessageType = messageType
	if systemType.Valid {
		v := systemType.String
		msg.SystemType = &v
	}
	if senderUserID.Valid {
		v := uint64(senderUserID.Int64)
		msg.SenderUserID = &v
	}
	if clientMessageID.Valid {
		v := clientMessageID.String
		msg.ClientMessageID = &v
	}
	if body.Valid {
		v := body.String
		msg.Body = &v
	}
	if payloadJSON.Valid && strings.TrimSpace(payloadJSON.String) != "" {
		msg.Payload = json.RawMessage(payloadJSON.String)
	}
	msg.Status = status
	msg.CreatedByService = createdByService
	msg.CreatedAt = createdAt.UTC()
	return msg, nil
}

func (r *Repository) LoadChatStatus(ctx context.Context, chatID uint64) (string, error) {
	var status string
	err := r.db.QueryRowContext(ctx, `
		SELECT status
		FROM chats
		WHERE id = ?
		LIMIT 1
	`, chatID).Scan(&status)
	if err != nil {
		return "", err
	}
	return status, nil
}

func (r *Repository) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
}

func (r *Repository) NextSequenceNumber(ctx context.Context, tx *sql.Tx, chatID uint64) (uint64, error) {
	var seq sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT MAX(sequence_number)
		FROM chat_messages
		WHERE chat_id = ?
	`, chatID).Scan(&seq); err != nil {
		return 0, err
	}
	if !seq.Valid {
		return 1, nil
	}
	return uint64(seq.Int64) + 1, nil
}

func (r *Repository) InsertSystemMessage(ctx context.Context, tx *sql.Tx, chatID uint64, messageType, systemType string, senderUserID *uint64, body string, payloadJSON []byte, service string) (*Message, error) {
	now := time.Now().UTC()
	var sender any
	if senderUserID != nil {
		sender = *senderUserID
	} else {
		sender = nil
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO chat_messages (
			chat_id, sequence_number, message_type, system_type, sender_user_id,
			client_message_id, body, payload_json, status, created_by_service, created_at
		) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, 'created', ?, ?)
	`, chatID, 0, messageType, systemType, sender, body, payloadJSON, service, now)
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	bodyCopy := body
	msg := &Message{
		ID:               uint64(id),
		ChatID:           chatID,
		MessageType:      messageType,
		SystemType:      &systemType,
		Body:             &bodyCopy,
		Status:           "created",
		CreatedByService: service,
		CreatedAt:        now,
	}
	if senderUserID != nil {
		msg.SenderUserID = senderUserID
	}
	if len(payloadJSON) > 0 {
		msg.Payload = json.RawMessage(payloadJSON)
	}
	return msg, nil
}

func (r *Repository) UpdateMessageSequence(ctx context.Context, tx *sql.Tx, messageID, sequenceNumber uint64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE chat_messages
		SET sequence_number = ?
		WHERE id = ?
	`, sequenceNumber, messageID)
	return err
}

func (r *Repository) LoadUserChatStatus(ctx context.Context, tx *sql.Tx, chatID, userID uint64) (bool, error) {
	var id uint64
	err := tx.QueryRowContext(ctx, `
		SELECT id
		FROM chat_participants
		WHERE chat_id = ? AND user_id = ?
		LIMIT 1
	`, chatID, userID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *Repository) LoadChatForMessage(ctx context.Context, messageID uint64) (uint64, error) {
	var chatID uint64
	err := r.db.QueryRowContext(ctx, `
		SELECT chat_id
		FROM chat_messages
		WHERE id = ?
		LIMIT 1
	`, messageID).Scan(&chatID)
	return chatID, err
}

func (r *Repository) DecodePayload(payload json.RawMessage) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil
	}
	return out
}

func (r *Repository) LoadAllowedChatStatus(status string) bool {
	switch strings.ToLower(status) {
	case "active", "date_suggested", "warning_inactive":
		return true
	default:
		return false
	}
}

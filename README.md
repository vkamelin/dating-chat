# Meet You Chat

Standalone Go WebSocket chat service for the dating app realtime layer.

## Build

```bash
go mod tidy
go build -o meet-you-chat ./cmd/chat
```

## Run locally

Set the required environment variables and run the binary:

```bash
HTTP_ADDR=:8080 \
MYSQL_DSN='user:password@tcp(127.0.0.1:3306)/dating?parseTime=true&charset=utf8mb4&loc=UTC' \
REDIS_ADDR=127.0.0.1:6379 \
AUTH_JWT_ALGORITHM=RS256 \
AUTH_JWT_PUBLIC_KEY_PATH='/opt/meet-you-chat/jwt-public.pem' \
AUTH_JWT_ISSUER='https://api.meet-you.ru' \
AUTH_JWT_AUDIENCE='mobile-app' \
./meet-you-chat
```

## Configuration

Environment variables:

```text
HTTP_ADDR=:8080

MYSQL_DSN=user:password@tcp(127.0.0.1:3306)/dating?parseTime=true&charset=utf8mb4&loc=UTC

REDIS_ADDR=127.0.0.1:6379
REDIS_PASSWORD=
REDIS_DB=0

AUTH_JWT_ALGORITHM=RS256
AUTH_JWT_PUBLIC_KEY_PATH=
AUTH_JWT_ISSUER=
AUTH_JWT_AUDIENCE=
AUTH_JWT_CLOCK_SKEW=30

CHAT_EVENTS_STREAM=chat.events
CHAT_EVENTS_GROUP=chat-ws
CHAT_EVENTS_CONSUMER=chat-ws-1

WS_MAX_MESSAGE_BYTES=65536
WS_PING_INTERVAL_SECONDS=25
WS_PONG_TIMEOUT_SECONDS=60

CHAT_MESSAGE_MAX_LENGTH=4000
CHAT_MESSAGE_RATE_LIMIT_PER_MINUTE=60
```

`AUTH_JWT_ALGORITHM` must be `RS256`. The chat service validates tokens with the same issuer, audience, signature, subject, session, and expiry rules as the API.

## HTTP Endpoints

* `GET /health` returns 200 when the process is alive.
* `GET /ready` returns 200 only when MySQL and Redis are reachable.
* `GET /ws` upgrades to WebSocket.

## WebSocket Authentication

Send the access token in the `Authorization` header:

```http
Authorization: Bearer <access_token>
```

The token must be a JWT with `sub` and `sid` claims. The service verifies:

1. JWT signature.
2. Expiration.
3. Issuer match.
4. Audience match.
5. `iat` and `exp` validity with clock skew.
6. Session existence in `users_sessions`.
7. Session revocation and expiration.
8. User existence and `active` status.

## WebSocket Protocol

### Send message

```json
{
  "type": "chat.message.send",
  "request_id": "uuid",
  "chat_id": 10,
  "client_message_id": "uuid",
  "body": "Привет"
}
```

Success:

```json
{
  "type": "chat.message.sent",
  "request_id": "uuid",
  "message": {
    "id": 1001,
    "chat_id": 10,
    "sequence_number": 55,
    "message_type": "user",
    "system_type": null,
    "sender_user_id": 123,
    "client_message_id": "uuid",
    "body": "Привет",
    "created_at": "2026-07-01T10:00:00Z"
  }
}
```

Realtime delivery to online participants:

```json
{
  "type": "chat.message.created",
  "message": {
    "id": 1001,
    "chat_id": 10,
    "sequence_number": 55,
    "message_type": "user",
    "system_type": null,
    "sender_user_id": 123,
    "body": "Привет",
    "created_at": "2026-07-01T10:00:00Z"
  }
}
```

### Mark read

```json
{
  "type": "chat.read",
  "request_id": "uuid",
  "chat_id": 10,
  "last_read_message_id": 1001
}
```

Success:

```json
{
  "type": "chat.read.ok",
  "request_id": "uuid",
  "chat_id": 10,
  "last_read_message_id": 1001,
  "read_at": "2026-07-01T10:00:05Z"
}
```

Delivery to the other participant:

```json
{
  "type": "chat.read.updated",
  "chat_id": 10,
  "user_id": 123,
  "last_read_message_id": 1001,
  "read_at": "2026-07-01T10:00:05Z"
}
```

### Ping

```json
{
  "type": "ping",
  "request_id": "uuid"
}
```

Response:

```json
{
  "type": "pong",
  "request_id": "uuid"
}
```

## Redis Stream Consumer

The service consumes `CHAT_EVENTS_STREAM` through consumer group `CHAT_EVENTS_GROUP` and process events from the backend:

* `chat.message.created`
* `chat.message.updated`
* `chat.read.updated`
* `chat.status.changed`

System messages are loaded from MySQL before delivery, and buttons are expanded from `chat_message_buttons` without exposing internal payloads.

## Expected MySQL Tables

### `users`

* `id BIGINT UNSIGNED`
* `status VARCHAR(32)`

### `users_sessions`

* `id CHAR(36)`
* `user_id BIGINT UNSIGNED`
* `expires_at DATETIME`
* `revoked_at DATETIME NULL`

### `chats`

* `id BIGINT UNSIGNED`
* `user_a_id BIGINT UNSIGNED`
* `user_b_id BIGINT UNSIGNED`
* `status VARCHAR(32)`
* `user_message_count INT UNSIGNED`
* `last_message_id BIGINT UNSIGNED NULL`
* `last_message_at DATETIME NULL`
* `last_user_message_at DATETIME NULL`
* `updated_at DATETIME`

### `chat_participants`

* `id BIGINT UNSIGNED`
* `chat_id BIGINT UNSIGNED`
* `user_id BIGINT UNSIGNED`
* `last_read_message_id BIGINT UNSIGNED NULL`
* `last_read_at DATETIME NULL`

### `chat_messages`

* `id BIGINT UNSIGNED`
* `chat_id BIGINT UNSIGNED`
* `sequence_number BIGINT UNSIGNED`
* `message_type VARCHAR(32)`
* `system_type VARCHAR(64) NULL`
* `sender_user_id BIGINT UNSIGNED NULL`
* `client_message_id CHAR(36) NULL`
* `body TEXT NULL`
* `payload_json JSON NULL`
* `status VARCHAR(32)`
* `created_by_service VARCHAR(64)`
* `created_at DATETIME`

### `chat_message_buttons`

* `id BIGINT UNSIGNED`
* `message_id BIGINT UNSIGNED`
* `button_key VARCHAR(64)`
* `label VARCHAR(255)`
* `action_type VARCHAR(64)`
* `style VARCHAR(32) NULL`
* `sort_order SMALLINT UNSIGNED`
* `is_enabled TINYINT(1)`
* `expires_at DATETIME NULL`

### `chat_message_receipts`

* `id BIGINT UNSIGNED`
* `message_id BIGINT UNSIGNED`
* `chat_id BIGINT UNSIGNED`
* `user_id BIGINT UNSIGNED`
* `delivered_at DATETIME NULL`
* `read_at DATETIME NULL`
* `created_at DATETIME`
* `updated_at DATETIME`

## systemd Example

```ini
[Unit]
Description=Meet You Chat Service
After=network.target mysql.service redis.service

[Service]
Type=simple
WorkingDirectory=/opt/meet-you-chat
ExecStart=/opt/meet-you-chat/meet-you-chat
Restart=always
RestartSec=3
Environment=HTTP_ADDR=:8080
Environment=MYSQL_DSN=user:password@tcp(127.0.0.1:3306)/dating?parseTime=true&charset=utf8mb4&loc=UTC
Environment=REDIS_ADDR=127.0.0.1:6379
Environment=AUTH_JWT_ALGORITHM=RS256
Environment=AUTH_JWT_PUBLIC_KEY_PATH=/opt/meet-you-chat/jwt-public.pem
Environment=AUTH_JWT_ISSUER=https://api.meet-you.ru
Environment=AUTH_JWT_AUDIENCE=mobile-app
Environment=AUTH_JWT_CLOCK_SKEW=30

[Install]
WantedBy=multi-user.target
```

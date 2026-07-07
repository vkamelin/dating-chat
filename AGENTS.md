# AGENTS.md for `dating-chat`

`dating-chat` is the standalone realtime service for the dating product. It is written in Go and owns WebSocket connections, chat message delivery, read receipts, and verification of realtime sessions.

## What this component is responsible for

- WebSocket upgrades and connection lifecycle.
- Authentication for realtime sessions.
- Delivery of chat messages and read updates.
- Consumption of backend chat events from Redis streams.
- Lightweight health and readiness endpoints.

## Architecture rules

- Keep the service small and focused on realtime transport.
- Do not move core product logic into the chat service.
- Do not add a framework if the current standard library plus existing packages are enough.
- Keep request handling and event processing allocation-light and latency-aware.
- Treat MySQL as persistent storage and Redis as transport/state for stream consumption.
- Keep auth checks strict and explicit.
- Preserve message-size limits, ping/pong handling, and rate limits.

## Security and performance guardrails

- Verify JWTs exactly as configured, including algorithm, signature, expiry, issuer, and audience when present.
- Do not log tokens, chat bodies, or other sensitive payloads.
- Reject oversized messages and invalid protocol frames early.
- Keep the WebSocket protocol backward compatible unless a change is clearly versioned.
- Favor simple event handlers and direct data mapping over deep abstractions.

## Files and directories to understand first

- `internal/config/config.go` for environment variables and defaults.
- `internal/auth/*` for JWT verification and session checks.
- `internal/chat/*` for message persistence and chat service logic.
- `internal/ws/*` for WebSocket protocol handling.
- `internal/events/*` for Redis stream consumption.
- `internal/db/*` and `internal/redisx/*` for infrastructure adapters.
- `internal/logging/*` for log formatting and redaction.
- `cmd/chat/main.go` for process startup.

## Working conventions

- Prefer small changes that preserve throughput and keep latency predictable.
- When adding a new message type or event, update the protocol and the backend contract together.
- When a requirement is unclear or contradictory, mark it as `Requires clarification`.
- Do not add stateful cross-cutting features unless the API and worker side truly need them.

## Useful commands

- `cd dating-chat && go mod tidy`
- `cd dating-chat && go build -o meet-you-chat ./cmd/chat`
- `cd dating-chat && HTTP_ADDR=:8080 MYSQL_DSN='...' REDIS_ADDR=127.0.0.1:6379 AUTH_JWT_ALGORITHM=RS256 AUTH_JWT_PUBLIC_KEY_PATH='...' AUTH_JWT_ISSUER='...' AUTH_JWT_AUDIENCE='...' ./meet-you-chat`

## Key references

- `README.md`
- `../README.md`
- `../geo.md`
- `../flutter_mobile_authentication.md`

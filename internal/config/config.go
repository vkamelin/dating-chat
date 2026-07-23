package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr string

	MySQLDSN string

	RedisAddr     string
	RedisPassword  string
	RedisDB        int
	RedisStream    string
	RedisGroup     string
	RedisConsumer  string

	AuthJWTAlgorithm     string
	AuthJWTPublicKeyPath string
	AuthJWTIssuer        string
	AuthJWTAudience      string
	AuthJWTClockSkew     int

	WSMaxMessageBytes       int64
	WSPingIntervalSeconds   int
	WSPongTimeoutSeconds    int
	PresenceTTLSeconds       int
	PresenceHeartbeatSeconds int
	PresenceChannel          string
	ChatMessageMaxLength     int
	ChatMessageRatePerMin    int
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:              getenv("HTTP_ADDR", ":8080"),
		MySQLDSN:              getenv("MYSQL_DSN", ""),
		RedisAddr:             getenv("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPassword:         getenv("REDIS_PASSWORD", ""),
		RedisDB:               getenvInt("REDIS_DB", 0),
		RedisStream:           getenv("CHAT_EVENTS_STREAM", "chat.events"),
		RedisGroup:            getenv("CHAT_EVENTS_GROUP", "chat-ws"),
		RedisConsumer:         getenv("CHAT_EVENTS_CONSUMER", "chat-ws-1"),
		AuthJWTAlgorithm:      getenv("AUTH_JWT_ALGORITHM", getenv("AUTH_JWT_ALG", "")),
		AuthJWTPublicKeyPath:  getenv("AUTH_JWT_PUBLIC_KEY_PATH", ""),
		AuthJWTIssuer:         getenv("AUTH_JWT_ISSUER", ""),
		AuthJWTAudience:       getenv("AUTH_JWT_AUDIENCE", ""),
		AuthJWTClockSkew:      getenvInt("AUTH_JWT_CLOCK_SKEW", 30),
		WSMaxMessageBytes:       getenvInt64("WS_MAX_MESSAGE_BYTES", 65536),
		WSPingIntervalSeconds:  getenvInt("WS_PING_INTERVAL_SECONDS", 25),
		WSPongTimeoutSeconds:   getenvInt("WS_PONG_TIMEOUT_SECONDS", 60),
		PresenceTTLSeconds:       getenvInt("PRESENCE_TTL_SECONDS", 90),
		PresenceHeartbeatSeconds: getenvInt("PRESENCE_HEARTBEAT_SECONDS", 30),
		PresenceChannel:          getenv("PRESENCE_EVENTS_CHANNEL", "chat.presence"),
		ChatMessageMaxLength:     getenvInt("CHAT_MESSAGE_MAX_LENGTH", 4000),
		ChatMessageRatePerMin:    getenvInt("CHAT_MESSAGE_RATE_LIMIT_PER_MINUTE", 60),
	}

	if cfg.MySQLDSN == "" {
		return Config{}, fmt.Errorf("MYSQL_DSN is required")
	}
	if cfg.WSPingIntervalSeconds <= 0 {
		return Config{}, fmt.Errorf("WS_PING_INTERVAL_SECONDS must be positive")
	}
	if cfg.WSPongTimeoutSeconds <= 0 {
		return Config{}, fmt.Errorf("WS_PONG_TIMEOUT_SECONDS must be positive")
	}
	if cfg.PresenceTTLSeconds <= 0 || cfg.PresenceHeartbeatSeconds <= 0 || cfg.PresenceHeartbeatSeconds >= cfg.PresenceTTLSeconds {
		return Config{}, fmt.Errorf("presence heartbeat must be positive and shorter than presence TTL")
	}
	if cfg.ChatMessageMaxLength <= 0 {
		return Config{}, fmt.Errorf("CHAT_MESSAGE_MAX_LENGTH must be positive")
	}
	if cfg.ChatMessageRatePerMin <= 0 {
		return Config{}, fmt.Errorf("CHAT_MESSAGE_RATE_LIMIT_PER_MINUTE must be positive")
	}
	cfg.AuthJWTAlgorithm = strings.ToUpper(strings.TrimSpace(cfg.AuthJWTAlgorithm))
	if cfg.AuthJWTAlgorithm == "" {
		return Config{}, fmt.Errorf("AUTH_JWT_ALGORITHM is required")
	}
	if cfg.AuthJWTAlgorithm != "RS256" {
		return Config{}, fmt.Errorf("AUTH_JWT_ALGORITHM must be RS256")
	}
	if cfg.AuthJWTPublicKeyPath == "" {
		return Config{}, fmt.Errorf("AUTH_JWT_PUBLIC_KEY_PATH is required")
	}
	if cfg.AuthJWTIssuer == "" {
		return Config{}, fmt.Errorf("AUTH_JWT_ISSUER is required")
	}
	if cfg.AuthJWTAudience == "" {
		return Config{}, fmt.Errorf("AUTH_JWT_AUDIENCE is required")
	}
	if cfg.AuthJWTClockSkew < 0 {
		return Config{}, fmt.Errorf("AUTH_JWT_CLOCK_SKEW must not be negative")
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getenvInt64(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

package config

import (
	"fmt"
	"os"
	"strconv"
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

	AuthJWTAlg           string
	AuthJWTSecret        string
	AuthJWTPublicKeyPath string
	AuthJWTIssuer        string
	AuthJWTAudience      string

	WSMaxMessageBytes      int64
	WSPingIntervalSeconds   int
	WSPongTimeoutSeconds    int
	ChatMessageMaxLength    int
	ChatMessageRatePerMin   int
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
		AuthJWTAlg:            getenv("AUTH_JWT_ALG", "HS256"),
		AuthJWTSecret:         getenv("AUTH_JWT_SECRET", ""),
		AuthJWTPublicKeyPath:  getenv("AUTH_JWT_PUBLIC_KEY_PATH", ""),
		AuthJWTIssuer:         getenv("AUTH_JWT_ISSUER", ""),
		AuthJWTAudience:       getenv("AUTH_JWT_AUDIENCE", ""),
		WSMaxMessageBytes:     getenvInt64("WS_MAX_MESSAGE_BYTES", 65536),
		WSPingIntervalSeconds:  getenvInt("WS_PING_INTERVAL_SECONDS", 25),
		WSPongTimeoutSeconds:   getenvInt("WS_PONG_TIMEOUT_SECONDS", 60),
		ChatMessageMaxLength:   getenvInt("CHAT_MESSAGE_MAX_LENGTH", 4000),
		ChatMessageRatePerMin:  getenvInt("CHAT_MESSAGE_RATE_LIMIT_PER_MINUTE", 60),
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
	if cfg.ChatMessageMaxLength <= 0 {
		return Config{}, fmt.Errorf("CHAT_MESSAGE_MAX_LENGTH must be positive")
	}
	if cfg.ChatMessageRatePerMin <= 0 {
		return Config{}, fmt.Errorf("CHAT_MESSAGE_RATE_LIMIT_PER_MINUTE must be positive")
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

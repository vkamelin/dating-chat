package auth

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"meet-you-chat/internal/config"
)

type Identity struct {
	UserID    uint64
	SessionID string
}

type Claims struct {
	SID string `json:"sid"`
	jwt.RegisteredClaims
}

type Authenticator struct {
	db        *sql.DB
	alg       string
	secret    []byte
	publicKey any
	issuer    string
	audience  string
}

func New(cfg config.Config, db *sql.DB) (*Authenticator, error) {
	a := &Authenticator{
		db:       db,
		alg:      strings.ToUpper(cfg.AuthJWTAlg),
		secret:   []byte(cfg.AuthJWTSecret),
		issuer:   cfg.AuthJWTIssuer,
		audience: cfg.AuthJWTAudience,
	}

	switch a.alg {
	case "HS256", "HS384", "HS512":
		if len(a.secret) == 0 {
			return nil, fmt.Errorf("AUTH_JWT_SECRET is required for %s", a.alg)
		}
	case "RS256", "RS384", "RS512", "ES256", "ES384", "ES512":
		if cfg.AuthJWTPublicKeyPath == "" {
			return nil, fmt.Errorf("AUTH_JWT_PUBLIC_KEY_PATH is required for %s", a.alg)
		}
		pub, err := loadPublicKey(cfg.AuthJWTPublicKeyPath)
		if err != nil {
			return nil, err
		}
		a.publicKey = pub
	default:
		return nil, fmt.Errorf("unsupported AUTH_JWT_ALG: %s", cfg.AuthJWTAlg)
	}

	return a, nil
}

func (a *Authenticator) Authenticate(ctx context.Context, tokenString string) (Identity, error) {
	if strings.TrimSpace(tokenString) == "" {
		return Identity{}, NewError("AUTH_TOKEN_REQUIRED", "Access token is required.", nil)
	}

	claims := &Claims{}
	parser := jwt.NewParser(jwt.WithValidMethods([]string{a.alg}))
	keyfunc := func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != a.alg {
			return nil, fmt.Errorf("unexpected signing method")
		}
		switch a.alg {
		case "HS256", "HS384", "HS512":
			return a.secret, nil
		default:
			return a.publicKey, nil
		}
	}

	_, err := parser.ParseWithClaims(tokenString, claims, keyfunc)
	if err != nil {
		switch {
		case errors.Is(err, jwt.ErrTokenExpired):
			return Identity{}, NewError("AUTH_TOKEN_EXPIRED", "Access token expired.", err)
		default:
			return Identity{}, NewError("AUTH_TOKEN_INVALID", "Access token is invalid.", err)
		}
	}

	now := time.Now().UTC()
	if claims.ExpiresAt == nil || !claims.ExpiresAt.After(now) {
		return Identity{}, NewError("AUTH_TOKEN_EXPIRED", "Access token expired.", nil)
	}
	if a.issuer != "" && claims.Issuer != a.issuer {
		return Identity{}, NewError("AUTH_TOKEN_INVALID", "Access token is invalid.", nil)
	}
	if a.audience != "" && !contains(claims.Audience, a.audience) {
		return Identity{}, NewError("AUTH_TOKEN_INVALID", "Access token is invalid.", nil)
	}

	userID, err := parseUint(claims.Subject)
	if err != nil || userID == 0 {
		return Identity{}, NewError("AUTH_TOKEN_INVALID", "Access token is invalid.", err)
	}
	if strings.TrimSpace(claims.SID) == "" {
		return Identity{}, NewError("AUTH_TOKEN_INVALID", "Access token is invalid.", nil)
	}

	var sessionUserID uint64
	var revokedAt sql.NullTime
	var expiresAt time.Time
	err = a.db.QueryRowContext(ctx, `
		SELECT user_id, expires_at, revoked_at
		FROM user_sessions
		WHERE id = ?
		LIMIT 1
	`, claims.SID).Scan(&sessionUserID, &expiresAt, &revokedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Identity{}, NewError("AUTH_SESSION_NOT_FOUND", "Session not found.", err)
		}
		return Identity{}, NewError("AUTH_TOKEN_INVALID", "Access token is invalid.", err)
	}
	if revokedAt.Valid {
		return Identity{}, NewError("AUTH_SESSION_REVOKED", "Session revoked.", nil)
	}
	if !expiresAt.After(now) {
		return Identity{}, NewError("AUTH_TOKEN_EXPIRED", "Access token expired.", nil)
	}
	if sessionUserID != userID {
		return Identity{}, NewError("AUTH_TOKEN_INVALID", "Access token is invalid.", nil)
	}

	var userStatus string
	err = a.db.QueryRowContext(ctx, `
		SELECT status
		FROM users
		WHERE id = ?
		LIMIT 1
	`, userID).Scan(&userStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Identity{}, NewError("AUTH_USER_NOT_FOUND", "User not found.", err)
		}
		return Identity{}, NewError("AUTH_TOKEN_INVALID", "Access token is invalid.", err)
	}
	if strings.ToLower(userStatus) != "active" {
		return Identity{}, NewError("AUTH_USER_BLOCKED", "User is blocked.", nil)
	}

	return Identity{UserID: userID, SessionID: claims.SID}, nil
}

func loadPublicKey(path string) (any, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("invalid public key pem")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err == nil {
		switch k := pub.(type) {
		case *rsa.PublicKey:
			return k, nil
		default:
			return pub, nil
		}
	}
	rsaPub, rsaErr := x509.ParsePKCS1PublicKey(block.Bytes)
	if rsaErr == nil {
		return rsaPub, nil
	}
	return nil, err
}

func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

func parseUint(v string) (uint64, error) {
	if strings.TrimSpace(v) == "" {
		return 0, fmt.Errorf("empty")
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

type Error struct {
	Code    string
	Message string
	Err     error
}

func (e *Error) Error() string { return e.Message }
func (e *Error) Unwrap() error { return e.Err }

func NewError(code, message string, err error) *Error {
	return &Error{Code: code, Message: message, Err: err}
}

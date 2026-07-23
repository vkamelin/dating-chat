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
	publicKey any
	issuer    string
	audience  string
	clockSkew time.Duration
}

func New(cfg config.Config, db *sql.DB) (*Authenticator, error) {
	alg := strings.ToUpper(strings.TrimSpace(cfg.AuthJWTAlgorithm))
	if alg != "RS256" {
		return nil, fmt.Errorf("AUTH_JWT_ALGORITHM must be RS256")
	}

	pub, err := loadPublicKey(cfg.AuthJWTPublicKeyPath)
	if err != nil {
		return nil, err
	}

	a := &Authenticator{
		db:        db,
		alg:       alg,
		publicKey: pub,
		issuer:    cfg.AuthJWTIssuer,
		audience:  cfg.AuthJWTAudience,
		clockSkew: time.Duration(cfg.AuthJWTClockSkew) * time.Second,
	}

	return a, nil
}

func (a *Authenticator) Authenticate(ctx context.Context, tokenString string) (Identity, error) {
	claims := &Claims{}
	parser := jwt.NewParser(jwt.WithValidMethods([]string{a.alg}))
	keyfunc := func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != a.alg {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return a.publicKey, nil
	}

	_, err := parser.ParseWithClaims(tokenString, claims, keyfunc)
	if err != nil {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", err)
	}

	now := time.Now().UTC()
	if claims.Subject == "" {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
	}
	if claims.ExpiresAt == nil || claims.IssuedAt == nil {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
	}
	if claims.Issuer != a.issuer {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
	}
	if len(claims.Audience) != 1 || claims.Audience[0] != a.audience {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
	}
	issuedAt := claims.IssuedAt.Time.UTC().Unix()
	tokenExpiresAt := claims.ExpiresAt.Time.UTC().Unix()
	nowUnix := now.Unix()
	skew := int64(a.clockSkew / time.Second)
	if issuedAt > nowUnix+skew {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
	}
	if tokenExpiresAt <= nowUnix-skew {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
	}
	if tokenExpiresAt <= issuedAt {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
	}
	if strings.TrimSpace(claims.SID) == "" {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
	}

	userID, err := parseUint(claims.Subject)
	if err != nil || userID == 0 {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", err)
	}

	var sessionUserID uint64
	var revokedAt sql.NullTime
	var sessionExpiresAt time.Time
	err = a.db.QueryRowContext(ctx, `
		SELECT user_id, expires_at, revoked_at
		FROM users_sessions
		WHERE id = ?
		LIMIT 1
	`, claims.SID).Scan(&sessionUserID, &sessionExpiresAt, &revokedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", err)
		}
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", err)
	}
	if revokedAt.Valid {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
	}
	if !sessionExpiresAt.After(now) {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
	}
	if sessionUserID != userID {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
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
			return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", err)
		}
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", err)
	}
	if strings.ToLower(userStatus) != "active" {
		return Identity{}, NewError("AUTH_ACCESS_TOKEN_INVALID", "Access token is invalid.", nil)
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

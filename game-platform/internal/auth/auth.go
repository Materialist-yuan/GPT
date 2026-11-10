// Package auth provides deliberately narrow HS256 access tokens and password hashing.
package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const AccessTTL = 15 * time.Minute
const RefreshTTL = 7 * 24 * time.Hour
const TicketTTL = 30 * time.Second

var ErrToken = errors.New("invalid token")

type Claims struct {
	Issuer    string `json:"iss"`
	Audience  string `json:"aud"`
	Subject   string `json:"sub"`
	SessionID string `json:"sid"`
	ID        string `json:"jti"`
	IssuedAt  int64  `json:"iat"`
	NotBefore int64  `json:"nbf"`
	ExpiresAt int64  `json:"exp"`
}

type Signer struct{ secret []byte }

func NewSigner(secret string) (*Signer, error) {
	if len(secret) < 32 {
		return nil, errors.New("JWT_SECRET must contain at least 32 bytes")
	}
	return &Signer{secret: []byte(secret)}, nil
}

func RandomToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (s *Signer) Sign(accountID int64, sid string, now time.Time) (string, error) {
	if accountID <= 0 || sid == "" {
		return "", ErrToken
	}
	id, err := RandomToken()
	if err != nil {
		return "", err
	}
	claims := Claims{Issuer: "gameplatform-login", Audience: "gameplatform-api", Subject: strconv.FormatInt(accountID, 10), SessionID: sid, ID: id, IssuedAt: now.Unix(), NotBefore: now.Unix(), ExpiresAt: now.Add(AccessTTL).Unix()}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(unsigned))
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (s *Signer) Verify(token string, now time.Time) (Claims, error) {
	var claims Claims
	if len(token) > 4096 {
		return claims, ErrToken
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return claims, ErrToken
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return claims, ErrToken
	}
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return claims, ErrToken
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return claims, ErrToken
	}
	var h struct {
		Alg  string `json:"alg"`
		Type string `json:"typ"`
	}
	if json.Unmarshal(header, &h) != nil || h.Alg != "HS256" || h.Type != "JWT" {
		return claims, ErrToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(payload, &claims) != nil {
		return Claims{}, ErrToken
	}
	accountID, err := strconv.ParseInt(claims.Subject, 10, 64)
	unix := now.Unix()
	if err != nil || accountID <= 0 || claims.Issuer != "gameplatform-login" || claims.Audience != "gameplatform-api" || claims.SessionID == "" || claims.ID == "" || claims.ExpiresAt <= unix || claims.NotBefore > unix || claims.IssuedAt > unix || claims.IssuedAt <= 0 || claims.ExpiresAt <= claims.IssuedAt || claims.ExpiresAt-claims.IssuedAt > int64(AccessTTL/time.Second) {
		return Claims{}, ErrToken
	}
	return claims, nil
}

func HashPassword(password string, cost int) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	return string(b), err
}

func CheckPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

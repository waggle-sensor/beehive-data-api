package main

import (
	"crypto/ecdsa"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	pelicanIssuer = "https://sagecontinuum.org"
	pelicanKeyID  = "3cfa"
)

// sciClaims is a SciToken. `aud` is a string, matching the tokens
// pelican_token.py mints; a JSON array is a different token.
type sciClaims struct {
	Subject   string `json:"sub"`
	Audience  string `json:"aud"`
	Version   string `json:"ver"`
	Scope     string `json:"scope"`
	Issuer    string `json:"iss"`
	IssuedAt  int64  `json:"iat"`
	NotBefore int64  `json:"nbf"`
	ExpiresAt int64  `json:"exp"`
	ID        string `json:"jti"`
}

// GetExpirationTime returns the expiration time of the token.
func (c sciClaims) GetExpirationTime() (*jwt.NumericDate, error) {
	return jwt.NewNumericDate(time.Unix(c.ExpiresAt, 0)), nil
}

// GetIssuedAt returns the issued at time of the token.
func (c sciClaims) GetIssuedAt() (*jwt.NumericDate, error) {
	return jwt.NewNumericDate(time.Unix(c.IssuedAt, 0)), nil
}

// GetNotBefore returns the not before time of the token.
func (c sciClaims) GetNotBefore() (*jwt.NumericDate, error) {
	return jwt.NewNumericDate(time.Unix(c.NotBefore, 0)), nil
}

// GetIssuer returns the issuer of the token.
func (c sciClaims) GetIssuer() (string, error)  { return c.Issuer, nil }

// GetSubject returns the subject of the token.
func (c sciClaims) GetSubject() (string, error) { return c.Subject, nil }

// GetAudience returns the audience of the token.
func (c sciClaims) GetAudience() (jwt.ClaimStrings, error) {
	return jwt.ClaimStrings{c.Audience}, nil
}

// mintSciToken signs a short-lived read token the way.
// `nbf` and `iat` are backdated by 60s because Pelican can see a token before our clock agrees it is valid.
func mintSciToken(key *ecdsa.PrivateKey, scope string, now time.Time, lifetime time.Duration) (string, error) {
	if lifetime <= 0 {
		return "", fmt.Errorf("token lifetime must be positive")
	}
	sec := now.Unix()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, sciClaims{
		Subject:   "beehive-data-api",
		Audience:  "ANY",
		Version:   "scitoken:2.0",
		Scope:     scope,
		Issuer:    pelicanIssuer,
		IssuedAt:  sec - 60,
		NotBefore: sec - 60,
		ExpiresAt: sec + int64(lifetime.Seconds()),
		ID:        fmt.Sprintf("bda-%d", sec),
	})
	tok.Header["kid"] = pelicanKeyID
	tok.Header["typ"] = "JWT"
	return tok.SignedString(key)
}

// loadPelicanKey loads a Pelican key from a file.
func loadPelicanKey(path string) (*ecdsa.PrivateKey, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return jwt.ParseECPrivateKeyFromPEM(pem)
}

// readScope is the SciTokens scope that covers the demo prefix and nothing beside it. The warehouse URL is https://host/sage/<prefix>/warehouse; staged Parquet lives under <prefix> as well, and the token has to cover both.
func readScope(warehouse string) (string, error) {
	u, err := url.Parse(warehouse)
	if err != nil {
		return "", err
	}
	path := strings.TrimPrefix(u.Path, "/sage/")
	seg := strings.SplitN(path, "/", 2)[0]
	if seg == "" || strings.Contains(seg, "..") {
		return "", fmt.Errorf("warehouse %q has no prefix to scope a token to", warehouse)
	}
	return "read:/" + seg, nil
}

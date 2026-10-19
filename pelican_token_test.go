package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestMintSciTokenClaims(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	signed, err := mintSciToken(key, "read:/cold-storage-demo-1", now, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(signed, ".")
	if len(parts) != 3 {
		t.Fatalf("token parts: %d", len(parts))
	}
	hdrJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	var hdr map[string]any
	if err := json.Unmarshal(hdrJSON, &hdr); err != nil {
		t.Fatal(err)
	}
	if hdr["alg"] != "ES256" || hdr["kid"] != pelicanKeyID || hdr["typ"] != "JWT" {
		t.Fatalf("header: %#v", hdr)
	}
	bodyJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(bodyJSON, &body); err != nil {
		t.Fatal(err)
	}
	if body["aud"] != "ANY" || body["iss"] != pelicanIssuer || body["ver"] != "scitoken:2.0" {
		t.Fatalf("claims: %#v", body)
	}
	if body["scope"] != "read:/cold-storage-demo-1" {
		t.Fatalf("scope: %#v", body["scope"])
	}
	if int64(body["nbf"].(float64)) != now.Unix()-60 {
		t.Fatalf("nbf not backdated: %#v", body["nbf"])
	}
	parsed, err := jwt.Parse(signed, func(tok *jwt.Token) (any, error) {
		return &key.PublicKey, nil
	}, jwt.WithoutClaimsValidation())
	if err != nil || !parsed.Valid {
		t.Fatalf("parse: %v valid=%v", err, parsed != nil && parsed.Valid)
	}
}

func TestReadScope(t *testing.T) {
	got, err := readScope("https://nrdstor.example:8443/sage/cold-storage-demo-1/warehouse")
	if err != nil {
		t.Fatal(err)
	}
	if got != "read:/cold-storage-demo-1" {
		t.Fatalf("scope %q", got)
	}
	if _, err := readScope("https://nrdstor.example:8443/sage"); err == nil {
		t.Fatal("warehouse without a prefix was accepted")
	}
}

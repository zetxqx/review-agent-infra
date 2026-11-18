package token

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

func TestMintAndVerifyRoundTrip(t *testing.T) {
	key := []byte("super-secret-hmac-key")
	parent := "run-parent"
	exp := time.Now().Add(10 * time.Minute).Unix()
	in := &reviewv1.TaskClaims{
		RunId:                   "run-123",
		SessionId:               "run-123-s0",
		ParentSessionId:         &parent,
		Repo:                    "llm-d/llm-d-router",
		HarnessProfile:          "openhands",
		MaxRequests:             60,
		MaxTotalTokens:          1500000,
		NormalizerReserveTokens: 20000,
		MaxOutputTokens:         4096,
		Exp:                     exp,
		Jti:                     "jti-abc",
	}

	signed, err := Mint(key, in)
	if err != nil {
		t.Fatalf("mint failed: %v", err)
	}
	got, err := Verify(key, "Bearer "+signed)
	if err != nil {
		t.Fatalf("verify failed: %v", err)
	}
	if got.RunId != in.RunId || got.SessionId != in.SessionId || got.GetParentSessionId() != parent ||
		got.MaxTotalTokens != in.MaxTotalTokens || got.Exp != exp || got.Jti != in.Jti {
		t.Fatalf("claims mismatch: got %+v want %+v", got, in)
	}
}

func TestVerifyRejectsInvalidTokens(t *testing.T) {
	key := []byte("secret-a")
	otherKey := []byte("secret-b")

	valid, err := Mint(key, &reviewv1.TaskClaims{
		RunId: "r1", SessionId: "s1", Repo: "o/r", HarnessProfile: "p",
		Exp: time.Now().Add(time.Minute).Unix(), Jti: "jti-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	expired, err := Mint(key, &reviewv1.TaskClaims{
		RunId: "r1", SessionId: "s1", Repo: "o/r", HarnessProfile: "p",
		Exp: time.Now().Add(-time.Minute).Unix(), Jti: "jti-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	noExp, err := jwt.NewWithClaims(jwt.SigningMethodHS256, taskJWTClaims{
		RunID: "r1", RegisteredClaims: jwt.RegisteredClaims{ID: "jti-3"},
	}).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	noJTI, err := jwt.NewWithClaims(jwt.SigningMethodHS256, taskJWTClaims{
		RunID: "r1", RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute))},
	}).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	hs512, err := jwt.NewWithClaims(jwt.SigningMethodHS512, taskJWTClaims{
		RunID: "r1", RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)), ID: "jti-4"},
	}).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		hdr   string
		vKey  []byte
	}{
		{"missing_header", "", key},
		{"non_bearer", "Basic " + valid, key},
		{"wrong_key", "Bearer " + valid, otherKey},
		{"expired", "Bearer " + expired, key},
		{"missing_exp", "Bearer " + noExp, key},
		{"missing_jti", "Bearer " + noJTI, key},
		{"wrong_alg", "Bearer " + hs512, key},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Verify(tc.vKey, tc.hdr); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

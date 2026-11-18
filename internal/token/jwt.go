package token

import (
	"errors"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

type taskJWTClaims struct {
	RunID                   string  `json:"run_id"`
	SessionID               string  `json:"session_id"`
	ParentSessionID         *string `json:"parent_session_id,omitempty"`
	Repo                    string  `json:"repo"`
	HarnessProfile          string  `json:"harness_profile"`
	MaxRequests             int32   `json:"max_requests"`
	MaxTotalTokens          int64   `json:"max_total_tokens"`
	NormalizerReserveTokens int64   `json:"normalizer_reserve_tokens"`
	MaxOutputTokens         int32   `json:"max_output_tokens"`
	jwt.RegisteredClaims
}

func Mint(key []byte, c *reviewv1.TaskClaims) (string, error) {
	if len(key) == 0 || c == nil || c.Jti == "" {
		return "", errors.New("invalid key or claims")
	}
	claims := taskJWTClaims{
		RunID:                   c.RunId,
		SessionID:               c.SessionId,
		ParentSessionID:         c.ParentSessionId,
		Repo:                    c.Repo,
		HarnessProfile:          c.HarnessProfile,
		MaxRequests:             c.MaxRequests,
		MaxTotalTokens:          c.MaxTotalTokens,
		NormalizerReserveTokens: c.NormalizerReserveTokens,
		MaxOutputTokens:         c.MaxOutputTokens,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Unix(c.Exp, 0)),
			ID:        c.Jti,
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
}

func Verify(key []byte, authHeader string) (*reviewv1.TaskClaims, error) {
	if len(key) == 0 {
		return nil, errors.New("empty verification key")
	}
	scheme, tok, ok := strings.Cut(authHeader, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(tok) == "" {
		return nil, errors.New("missing bearer token")
	}
	var parsed taskJWTClaims
	tokObj, err := jwt.ParseWithClaims(
		strings.TrimSpace(tok),
		&parsed,
		func(t *jwt.Token) (any, error) {
			return key, nil
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
	)
	if err != nil || !tokObj.Valid || parsed.ID == "" || parsed.ExpiresAt == nil {
		return nil, errors.New("invalid task token")
	}
	return &reviewv1.TaskClaims{
		RunId:                   parsed.RunID,
		SessionId:               parsed.SessionID,
		ParentSessionId:         parsed.ParentSessionID,
		Repo:                    parsed.Repo,
		HarnessProfile:          parsed.HarnessProfile,
		MaxRequests:             parsed.MaxRequests,
		MaxTotalTokens:          parsed.MaxTotalTokens,
		NormalizerReserveTokens: parsed.NormalizerReserveTokens,
		MaxOutputTokens:         parsed.MaxOutputTokens,
		Exp:                     parsed.ExpiresAt.Unix(),
		Jti:                     parsed.ID,
	}, nil
}

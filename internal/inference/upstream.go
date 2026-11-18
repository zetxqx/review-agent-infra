package inference

import (
	"encoding/json"
	"net/http"
	"strings"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

func upstreamPath(api string) string {
	if api == "responses" {
		return "/v1/responses"
	}
	return "/v1/chat/completions"
}

type VllmDirectUpstream struct {
	BaseURL string
	APIKey  string
}

// payload key: allowlisted OpenAI request JSON field name; value: sanitized JSON value.
func (u *VllmDirectUpstream) Build(_ *reviewv1.TaskClaims, api string, payload map[string]any) (contract.UpstreamRequest, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return contract.UpstreamRequest{}, err
	}
	hdr := make(http.Header)
	hdr.Set("Content-Type", "application/json")
	if u.APIKey != "" {
		hdr.Set("Authorization", "Bearer "+u.APIKey)
	}
	return contract.UpstreamRequest{
		URL:     strings.TrimRight(u.BaseURL, "/") + upstreamPath(api),
		Headers: hdr,
		Body:    body,
	}, nil
}

type LlmdGatewayUpstream struct {
	BaseURL   string
	APIKey    string
	Objective string
}

// payload key: allowlisted OpenAI request JSON field name; value: sanitized JSON value.
func (u *LlmdGatewayUpstream) Build(claims *reviewv1.TaskClaims, api string, payload map[string]any) (contract.UpstreamRequest, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return contract.UpstreamRequest{}, err
	}
	objective := u.Objective
	if objective == "" {
		objective = "code-review-batch"
	}
	hdr := make(http.Header)
	hdr.Set("Content-Type", "application/json")
	if u.APIKey != "" {
		hdr.Set("Authorization", "Bearer "+u.APIKey)
	}
	hdr.Set("x-session-id", claims.SessionId)
	hdr.Set("x-llm-d-inference-fairness-id", claims.Repo)
	hdr.Set("x-llm-d-inference-objective", objective)
	if claims.ParentSessionId != nil && *claims.ParentSessionId != "" {
		hdr.Set("x-parent-session-id", *claims.ParentSessionId)
	}
	return contract.UpstreamRequest{
		URL:     strings.TrimRight(u.BaseURL, "/") + upstreamPath(api),
		Headers: hdr,
		Body:    body,
	}, nil
}

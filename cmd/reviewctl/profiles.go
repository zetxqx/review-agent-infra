package main

import (
	"fmt"
	"strings"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

type ProfileSpec struct {
	Name              string
	Adapter           string
	Image             string
	DefaultConfidence float64
	// key: harness adapter option key; value: option string value
	Options map[string]string
	Budget  *reviewv1.TaskBudget
}

// Return map key: harness profile name; value: configured profile specification.
func builtinProfiles() map[string]ProfileSpec {
	// key: harness profile name; value: configured profile specification
	return map[string]ProfileSpec{
		"pr-agent": {
			Name:              "pr-agent",
			Adapter:           "pr-agent",
			Image:             "review-agent-infra/poc-sandbox:local",
			DefaultConfidence: 0.80,
			Budget: &reviewv1.TaskBudget{
				MaxRequests:             8,
				MaxContextTokens:        32768,
				MaxTotalTokens:          150000,
				NormalizerReserveTokens: 20000,
				WallClockSeconds:        300,
			},
		},
		"openhands": {
			Name:              "openhands",
			Adapter:           "openhands",
			Image:             "review-agent-infra/poc-sandbox:local",
			DefaultConfidence: 0.80,
			Budget: &reviewv1.TaskBudget{
				MaxRequests:             120,
				MaxContextTokens:        32768,
				MaxTotalTokens:          5000000,
				NormalizerReserveTokens: 50000,
				WallClockSeconds:        600,
			},
		},
		"codex": {
			Name:              "codex",
			Adapter:           "codex",
			Image:             "review-agent-infra/poc-sandbox:local",
			DefaultConfidence: 0.80,
			Budget: &reviewv1.TaskBudget{
				MaxRequests:             60,
				MaxContextTokens:        32768,
				MaxTotalTokens:          5000000,
				NormalizerReserveTokens: 50000,
				WallClockSeconds:        600,
			},
		},
		"mini-swe-agent": {
			Name:              "mini-swe-agent",
			Adapter:           "mini-swe-agent",
			Image:             "review-agent-infra/poc-sandbox:local",
			DefaultConfidence: 0.80,
			Budget: &reviewv1.TaskBudget{
				MaxRequests:             60,
				MaxContextTokens:        32768,
				MaxTotalTokens:          5000000,
				NormalizerReserveTokens: 50000,
				WallClockSeconds:        600,
			},
		},
		"opencode": {
			Name:              "opencode",
			Adapter:           "opencode",
			Image:             "review-agent-infra/poc-sandbox:local",
			DefaultConfidence: 0.80,
			Budget: &reviewv1.TaskBudget{
				MaxRequests:             60,
				MaxContextTokens:        32768,
				MaxTotalTokens:          5000000,
				NormalizerReserveTokens: 50000,
				WallClockSeconds:        600,
			},
		},
		"goose": {
			Name:              "goose",
			Adapter:           "goose",
			Image:             "review-agent-infra/poc-sandbox:local",
			DefaultConfidence: 0.80,
			Budget: &reviewv1.TaskBudget{
				MaxRequests:             60,
				MaxContextTokens:        32768,
				MaxTotalTokens:          5000000,
				NormalizerReserveTokens: 50000,
				WallClockSeconds:        600,
			},
		},
		"aider": {
			Name:              "aider",
			Adapter:           "aider",
			Image:             "review-agent-infra/poc-sandbox:local",
			DefaultConfidence: 0.80,
			Budget: &reviewv1.TaskBudget{
				MaxRequests:             12,
				MaxContextTokens:        32768,
				MaxTotalTokens:          300000,
				NormalizerReserveTokens: 20000,
				WallClockSeconds:        300,
			},
		},
		"single-shot": {
			Name:              "single-shot",
			Adapter:           "single-shot",
			Image:             "review-agent-infra/poc-sandbox:local",
			DefaultConfidence: 0.80,
			Budget: &reviewv1.TaskBudget{
				MaxRequests:             8,
				MaxContextTokens:        32768,
				MaxTotalTokens:          150000,
				NormalizerReserveTokens: 20000,
				WallClockSeconds:        180,
			},
		},
		"pr-agent-sim": {
			Name:              "pr-agent-sim",
			Adapter:           "command",
			Image:             "review-agent-infra/poc-sandbox:local",
			DefaultConfidence: 0.80,
			// key: harness adapter option key; value: option string value
			Options: map[string]string{"mode": "pr-agent-yaml"},
			Budget: &reviewv1.TaskBudget{
				MaxRequests:             8,
				MaxContextTokens:        32768,
				MaxTotalTokens:          150000,
				NormalizerReserveTokens: 20000,
				WallClockSeconds:        180,
			},
		},
		"mock-llm": {
			Name:              "mock-llm",
			Adapter:           "mock-llm",
			Image:             "review-agent-infra/poc-sandbox:local",
			DefaultConfidence: 0.80,
			Budget: &reviewv1.TaskBudget{
				MaxRequests:             20,
				MaxContextTokens:        32768,
				MaxTotalTokens:          300000,
				NormalizerReserveTokens: 20000,
				WallClockSeconds:        180,
			},
		},
	}
}

func resolveProfile(name string) (ProfileSpec, error) {
	trimmed := strings.TrimSpace(name)
	profiles := builtinProfiles()
	if p, ok := profiles[trimmed]; ok {
		return p, nil
	}
	return ProfileSpec{}, fmt.Errorf("unknown harness profile %q", trimmed)
}

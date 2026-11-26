package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/orchestrator"
	"github.com/zetxqx/review-agent-infra/internal/publisher"
	"github.com/zetxqx/review-agent-infra/internal/sandbox"
	"google.golang.org/protobuf/encoding/protojson"
)

type harnessConfigFile struct {
	Default string
	// key: harness profile name; value: raw JSON profile specification
	Profiles map[string]json.RawMessage
	Routing  []orchestrator.RoutingRule
}

type profileConfigFile struct {
	Name              string
	Adapter           string
	Image             string
	DefaultConfidence float64
	// key: harness adapter option key; value: option string value
	Options map[string]string
	Budget  json.RawMessage
}

func loadHarnessConfigJSON(path string) (orchestrator.HarnessConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return orchestrator.HarnessConfig{}, err
	}
	cfg, err := parseHarnessConfigJSON(raw)
	if err != nil {
		return orchestrator.HarnessConfig{}, fmt.Errorf("harness config %s: %w", path, err)
	}
	return cfg, nil
}

func parseHarnessConfigJSON(raw []byte) (orchestrator.HarnessConfig, error) {
	var file harnessConfigFile
	if err := decodeStrictJSON(raw, &file); err != nil {
		return orchestrator.HarnessConfig{}, fmt.Errorf("decode: %w", err)
	}
	cfg := orchestrator.DefaultHarnessConfig()
	builtins := orchestrator.DefaultHarnessConfig().Profiles
	if file.Default != "" {
		cfg.Default = file.Default
	}
	if file.Routing != nil {
		cfg.Routing = file.Routing
	}
	for name, rawProfile := range file.Profiles {
		profile, err := parseProfileConfig(name, rawProfile, builtins[name])
		if err != nil {
			return orchestrator.HarnessConfig{}, err
		}
		cfg.Profiles[name] = profile
	}
	if _, ok := cfg.Profiles[cfg.Default]; !ok {
		return orchestrator.HarnessConfig{}, fmt.Errorf("default harness profile %q is not defined", cfg.Default)
	}
	return cfg, nil
}

func parseProfileConfig(name string, raw json.RawMessage, builtin orchestrator.ProfileConfig) (orchestrator.ProfileConfig, error) {
	var file profileConfigFile
	if err := decodeStrictJSON(raw, &file); err != nil {
		return orchestrator.ProfileConfig{}, fmt.Errorf("harness profile %q: %w", name, err)
	}
	if file.Name != "" && file.Name != name {
		return orchestrator.ProfileConfig{}, fmt.Errorf("harness profile %q declares mismatched Name %q", name, file.Name)
	}
	profile := orchestrator.ProfileConfig{
		Name:              name,
		Adapter:           firstNonEmpty(file.Adapter, builtin.Adapter),
		Image:             firstNonEmpty(file.Image, builtin.Image, sandbox.DefaultSandboxImage),
		DefaultConfidence: file.DefaultConfidence,
		Options:           file.Options,
	}
	if profile.Adapter == "" {
		return orchestrator.ProfileConfig{}, fmt.Errorf("harness profile %q has no adapter", name)
	}
	if profile.DefaultConfidence == 0 {
		profile.DefaultConfidence = builtin.DefaultConfidence
	}
	if profile.DefaultConfidence == 0 {
		profile.DefaultConfidence = publisher.DefaultProfileConfidence
	}
	if profile.DefaultConfidence < 0 || profile.DefaultConfidence > 1 {
		return orchestrator.ProfileConfig{}, fmt.Errorf("harness profile %q DefaultConfidence %v is outside [0, 1]", name, profile.DefaultConfidence)
	}
	trimmedBudget := bytes.TrimSpace(file.Budget)
	if len(trimmedBudget) == 0 || bytes.Equal(trimmedBudget, []byte("null")) {
		return orchestrator.ProfileConfig{}, fmt.Errorf("harness profile %q has no budget", name)
	}
	budget := &reviewv1.TaskBudget{}
	if err := protojson.Unmarshal(trimmedBudget, budget); err != nil {
		return orchestrator.ProfileConfig{}, fmt.Errorf("harness profile %q budget: %w", name, err)
	}
	if err := validateTaskBudget(budget); err != nil {
		return orchestrator.ProfileConfig{}, fmt.Errorf("harness profile %q budget: %w", name, err)
	}
	profile.Budget = budget
	return profile, nil
}

func validateTaskBudget(b *reviewv1.TaskBudget) error {
	switch {
	case b.GetMaxRequests() <= 0:
		return fmt.Errorf("max_requests must be positive, got %d", b.GetMaxRequests())
	case b.GetMaxTotalTokens() <= 0:
		return fmt.Errorf("max_total_tokens must be positive, got %d", b.GetMaxTotalTokens())
	case b.GetNormalizerReserveTokens() < 0 || b.GetNormalizerReserveTokens() >= b.GetMaxTotalTokens():
		return fmt.Errorf("normalizer_reserve_tokens must be in [0, max_total_tokens), got %d of %d", b.GetNormalizerReserveTokens(), b.GetMaxTotalTokens())
	case b.GetWallClockSeconds() <= 0:
		return fmt.Errorf("wall_clock_seconds must be positive, got %d", b.GetWallClockSeconds())
	case b.GetMaxContextTokens() < 0:
		return fmt.Errorf("max_context_tokens must not be negative, got %d", b.GetMaxContextTokens())
	}
	return nil
}

func decodeStrictJSON(raw []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after JSON value")
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

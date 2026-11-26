package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeHarnessConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "harness.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadHarnessConfigJSONRejectsProfileWithoutBudget(t *testing.T) {
	path := writeHarnessConfig(t, `{"Profiles":{"openhands":{"Name":"openhands","Adapter":"openhands","Image":"registry.example/sandbox:1"}}}`)
	_, err := loadHarnessConfigJSON(path)
	if err == nil || !strings.Contains(err.Error(), `"openhands"`) || !strings.Contains(err.Error(), "budget") {
		t.Fatalf("expected an error naming the openhands profile and its missing budget, got %v", err)
	}
}

func TestLoadHarnessConfigJSONRejectsUnknownDefaultProfile(t *testing.T) {
	path := writeHarnessConfig(t, `{"Default":"missing"}`)
	_, err := loadHarnessConfigJSON(path)
	if err == nil || !strings.Contains(err.Error(), `"missing"`) {
		t.Fatalf("expected an error naming the unknown default profile, got %v", err)
	}
}

func TestLoadHarnessConfigJSONAcceptsFullProfile(t *testing.T) {
	path := writeHarnessConfig(t, `{"Default":"openhands","Profiles":{"openhands":{"Name":"openhands","Adapter":"openhands","Image":"registry.example/sandbox:1","DefaultConfidence":0.8,"Budget":{"max_requests":60,"max_context_tokens":32768,"max_total_tokens":5000000,"normalizer_reserve_tokens":50000,"wall_clock_seconds":1800}}}}`)
	cfg, err := loadHarnessConfigJSON(path)
	if err != nil {
		t.Fatalf("loadHarnessConfigJSON: %v", err)
	}
	openhands := cfg.Profiles["openhands"]
	if openhands.Image != "registry.example/sandbox:1" || openhands.Budget.GetWallClockSeconds() != 1800 || openhands.Budget.GetMaxRequests() != 60 {
		t.Fatalf("unexpected openhands profile: %+v", openhands)
	}
	if _, ok := cfg.Profiles["single-shot"]; !ok {
		t.Fatalf("expected built-in profiles that the file does not name to stay available")
	}
}

func TestParseControllerFlagsReadsGitHubAppIDsFromEnv(t *testing.T) {
	t.Setenv("GITHUB_APP_ID", "123456")
	t.Setenv("GITHUB_INSTALLATION_ID", "7890")
	f, err := parseControllerFlags(nil)
	if err != nil {
		t.Fatalf("parseControllerFlags: %v", err)
	}
	if f.githubAppID != 123456 || f.githubInstallID != 7890 {
		t.Fatalf("expected App ID 123456 and installation ID 7890 from env, got %d and %d", f.githubAppID, f.githubInstallID)
	}

	flagged, err := parseControllerFlags([]string{"--github-app-id", "42"})
	if err != nil {
		t.Fatalf("parseControllerFlags with flag: %v", err)
	}
	if flagged.githubAppID != 42 {
		t.Fatalf("expected the flag to override the env App ID, got %d", flagged.githubAppID)
	}

	t.Setenv("GITHUB_APP_ID", "not-a-number")
	if _, err := parseControllerFlags(nil); err == nil || !strings.Contains(err.Error(), "GITHUB_APP_ID") {
		t.Fatalf("expected an error naming GITHUB_APP_ID, got %v", err)
	}
}

func TestParseControllerFlagsStatelessAndEmbeddedProxyDefaults(t *testing.T) {
	t.Setenv("ADAPTER_UPSTREAM_URL", "http://llmd.internal:8000")
	t.Setenv("ADAPTER_UPSTREAM_MODEL", "qwen3-coder")
	f, err := parseControllerFlags(nil)
	if err != nil {
		t.Fatalf("parseControllerFlags: %v", err)
	}
	if f.sandboxListenAddr != ":8081" || f.feedbackPollSec != 0 {
		t.Fatalf("unexpected defaults: sandboxListenAddr=%q feedbackPollSec=%d", f.sandboxListenAddr, f.feedbackPollSec)
	}
	if f.upstreamURL != "http://llmd.internal:8000" || f.upstreamModel != "qwen3-coder" {
		t.Fatalf("unexpected upstream env values: url=%q model=%q", f.upstreamURL, f.upstreamModel)
	}
}

func TestLoadHarnessConfigJSONBackfillsOmittedProfileFields(t *testing.T) {
	path := writeHarnessConfig(t, `{"Profiles":{"codex":{"Budget":{"maxRequests":30,"maxTotalTokens":"400000","normalizerReserveTokens":"20000","wallClockSeconds":600}},"custom":{"Adapter":"aider","Budget":{"max_requests":5,"max_total_tokens":100000,"wall_clock_seconds":120}}}}`)
	cfg, err := loadHarnessConfigJSON(path)
	if err != nil {
		t.Fatalf("loadHarnessConfigJSON: %v", err)
	}
	codex := cfg.Profiles["codex"]
	if codex.Name != "codex" || codex.Adapter != "codex" || codex.Image == "" || codex.DefaultConfidence != 0.80 {
		t.Fatalf("expected codex identity and confidence backfilled from built-in, got %+v", codex)
	}
	if codex.Budget.GetMaxRequests() != 30 || codex.Budget.GetMaxTotalTokens() != 400000 || codex.Budget.GetNormalizerReserveTokens() != 20000 || codex.Budget.GetWallClockSeconds() != 600 {
		t.Fatalf("expected protojson lowerCamelCase budget decoded, got %+v", codex.Budget)
	}
	custom := cfg.Profiles["custom"]
	if custom.Name != "custom" || custom.Adapter != "aider" || custom.Image == "" || custom.DefaultConfidence <= 0 {
		t.Fatalf("expected custom profile name, image, and confidence backfilled, got %+v", custom)
	}
}

func TestLoadHarnessConfigJSONRejectsInvalidProfiles(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"unknown budget field", `{"Profiles":{"codex":{"Budget":{"maxRequestz":30,"max_total_tokens":1000,"wall_clock_seconds":60}}}}`, "maxRequestz"},
		{"zero requests", `{"Profiles":{"codex":{"Budget":{"max_total_tokens":1000,"wall_clock_seconds":60}}}}`, "max_requests"},
		{"zero total", `{"Profiles":{"codex":{"Budget":{"max_requests":5,"wall_clock_seconds":60}}}}`, "max_total_tokens"},
		{"negative reserve", `{"Profiles":{"codex":{"Budget":{"max_requests":5,"max_total_tokens":1000,"normalizer_reserve_tokens":-1,"wall_clock_seconds":60}}}}`, "normalizer_reserve_tokens"},
		{"reserve not below total", `{"Profiles":{"codex":{"Budget":{"max_requests":5,"max_total_tokens":1000,"normalizer_reserve_tokens":1000,"wall_clock_seconds":60}}}}`, "normalizer_reserve_tokens"},
		{"zero wall clock", `{"Profiles":{"codex":{"Budget":{"max_requests":5,"max_total_tokens":1000}}}}`, "wall_clock_seconds"},
		{"null budget", `{"Profiles":{"codex":{"Budget":null}}}`, "budget"},
		{"custom without adapter", `{"Profiles":{"custom":{"Budget":{"max_requests":5,"max_total_tokens":1000,"wall_clock_seconds":60}}}}`, "adapter"},
		{"mismatched name", `{"Profiles":{"codex":{"Name":"other","Budget":{"max_requests":5,"max_total_tokens":1000,"wall_clock_seconds":60}}}}`, "mismatched"},
		{"confidence above one", `{"Profiles":{"codex":{"DefaultConfidence":1.5,"Budget":{"max_requests":5,"max_total_tokens":1000,"wall_clock_seconds":60}}}}`, "DefaultConfidence"},
		{"unknown profile field", `{"Profiles":{"codex":{"Imgae":"x","Budget":{"max_requests":5,"max_total_tokens":1000,"wall_clock_seconds":60}}}}`, "Imgae"},
		{"unknown top-level field", `{"Defualt":"codex"}`, "Defualt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadHarnessConfigJSON(writeHarnessConfig(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error mentioning %q, got %v", tc.want, err)
			}
		})
	}
}

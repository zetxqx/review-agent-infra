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

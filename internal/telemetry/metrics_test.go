package telemetry

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegistryPrometheusExposition(t *testing.T) {
	reg := NewRegistry()
	reg.IncRun("llm-d/router", "openhands", "done")
	reg.IncRun("llm-d/router", "openhands", "done")
	reg.AddTokens("llm-d/router", "openhands", 1500, 320, 900)
	reg.ObserveRunDuration("llm-d/router", "openhands", 12.5)
	reg.IncRateLimited("llm-d/router", "openhands", "repo_window")

	srv := httptest.NewServer(reg.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET metrics: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	text := string(raw)

	for _, want := range []string{
		`review_runs_total{repo="llm-d/router",harness="openhands",status="done"} 2`,
		`review_tokens_total{repo="llm-d/router",harness="openhands",kind="prompt"} 1500`,
		`review_tokens_total{repo="llm-d/router",harness="openhands",kind="completion"} 320`,
		`review_cached_tokens_total{repo="llm-d/router",harness="openhands"} 900`,
		`review_run_duration_seconds_bucket{repo="llm-d/router",harness="openhands",le="30"} 1`,
		`review_run_duration_seconds_count{repo="llm-d/router",harness="openhands"} 1`,
		`review_rate_limited_total{repo="llm-d/router",harness="openhands",reason="repo_window"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, text)
		}
	}
}

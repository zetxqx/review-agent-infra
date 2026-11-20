package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestReviewWithFailedReportSkipsPublishAndReturnsError(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	gitExec(t, repoDir, "init", "-q", "-b", "main")
	writeFile(t, repoDir, "main.go", "package main\nfunc f() int { return 1 }\n")
	gitExec(t, repoDir, "add", "-A")
	gitExec(t, repoDir, "commit", "-q", "-m", "init")
	writeFile(t, repoDir, "main.go", "package main\nfunc f() int { return 2 }\n")
	gitExec(t, repoDir, "add", "-A")
	gitExec(t, repoDir, "commit", "-q", "-m", "change")
	gitExec(t, repoDir, "remote", "add", "origin", "git@github.com:zetxqx/review-agent-infra.git")

	commits, err := resolveTargetCommits(ctx, commonFlags{repoDir: repoDir, commitRef: "HEAD"})
	if err != nil {
		t.Fatalf("resolveTargetCommits: %v", err)
	}
	headSHA := commits[0].HeadSHA

	fakeDockerDir := t.TempDir()
	script := strings.Join([]string{
		"#!/usr/bin/env bash",
		"set -euo pipefail",
		"if [[ \"$*\" == *\"read-report\"* ]]; then",
		"  echo '{\"summary\":\"Partial deliberation before the wall clock expired.\",\"findings\":[],\"provenance\":{\"profile\":\"single-shot\",\"normalizer\":\"NORMALIZER_PATH_NONE\",\"status\":\"REPORT_STATUS_HARNESS_FAILED\"}}'",
		"else",
		"  cat > /dev/null",
		"fi",
	}, "\n")
	if err := os.WriteFile(filepath.Join(fakeDockerDir, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", fakeDockerDir+":"+os.Getenv("PATH"))

	var mu sync.Mutex
	var reviewPosts int
	ghMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/zetxqx/review-agent-infra/pulls/19":
			_ = json.NewEncoder(w).Encode(struct {
				Head struct {
					SHA string `json:"sha"`
				} `json:"head"`
			}{
				Head: struct {
					SHA string `json:"sha"`
				}{SHA: headSHA},
			})
		case r.Method == http.MethodPost:
			mu.Lock()
			reviewPosts++
			mu.Unlock()
			_, _ = w.Write([]byte(`{"id":9002}`))
		default:
			_, _ = w.Write([]byte("[]"))
		}
	}))
	defer ghMock.Close()
	t.Setenv("GITHUB_API_URL", ghMock.URL)
	t.Setenv("GITHUB_TOKEN", "ghs_failed_report_test_tok")

	var out bytes.Buffer
	err = runCLI(ctx, []string{
		"review",
		"--repo", repoDir,
		"--commit", "HEAD",
		"--runner", "docker",
		"--harness", "single-shot",
		"--mock-upstream",
		"--publish-pr", "19",
		"--format", "json",
	}, &out)
	if err == nil || !strings.Contains(err.Error(), "harness_failed") {
		t.Fatalf("expected harness_failed error from review, got %v", err)
	}

	mu.Lock()
	posted := reviewPosts
	mu.Unlock()
	if posted != 0 {
		t.Fatalf("failed report must not be published, got %d GitHub write requests", posted)
	}

	var decoded struct {
		Metrics   ExecutionMetrics     `json:"metrics"`
		Published *PublishedReviewInfo `json:"published"`
	}
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, out.String())
	}
	if decoded.Metrics.Status != "harness_failed" || decoded.Published != nil {
		t.Fatalf("expected rendered harness_failed metrics without published info, got %+v published=%+v", decoded.Metrics, decoded.Published)
	}
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

func TestReviewFailsWhenHarnessRequestsAreRejectedWithoutReportFile(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	gitExec(t, repoDir, "init", "-q", "-b", "main")
	writeFile(t, repoDir, "main.go", "package main\nfunc f() int { return 1 }\n")
	gitExec(t, repoDir, "add", "-A")
	gitExec(t, repoDir, "commit", "-q", "-m", "init")
	writeFile(t, repoDir, "main.go", "package main\nfunc f() int { return 2 }\n")
	gitExec(t, repoDir, "add", "-A")
	gitExec(t, repoDir, "commit", "-q", "-m", "change")

	harnessScript := filepath.Join(t.TempDir(), "hit_request_cap.py")
	scriptBody := strings.Join([]string{
		"import json, os, urllib.error, urllib.request",
		"base = os.environ['OPENAI_BASE_URL'].rstrip('/')",
		"body = json.dumps({",
		"    'model': os.environ.get('REVIEW_MODEL', 'default'),",
		"    'max_tokens': 16,",
		"    'messages': [{'role': 'user', 'content': 'review step'}],",
		"}).encode('utf-8')",
		"headers = {",
		"    'Authorization': 'Bearer ' + os.environ['OPENAI_API_KEY'],",
		"    'Content-Type': 'application/json',",
		"}",
		"for _ in range(10):",
		"    req = urllib.request.Request(base + '/chat/completions', data=body, headers=headers)",
		"    try:",
		"        urllib.request.urlopen(req, timeout=20).read()",
		"    except urllib.error.HTTPError as exc:",
		"        print('LLM request rejected with HTTP', exc.code)",
		"        break",
		"print('Still checking candidate A before finalizing.')",
	}, "\n")
	if err := os.WriteFile(harnessScript, []byte(scriptBody), 0o755); err != nil {
		t.Fatalf("write harness script: %v", err)
	}
	argvJSON, err := json.Marshal([]string{"python3", harnessScript})
	if err != nil {
		t.Fatal(err)
	}

	var cfg commonFlags
	fs := flag.NewFlagSet("reviewctl test", flag.ContinueOnError)
	bindCommonFlags(fs, &cfg)
	if err := fs.Parse([]string{"--repo", repoDir, "--commit", "HEAD", "--runner", "local", "--mock-upstream"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	commits, err := resolveTargetCommits(ctx, cfg)
	if err != nil {
		t.Fatalf("resolveTargetCommits: %v", err)
	}
	adapterSess, err := startEphemeralAdapter(cfg)
	if err != nil {
		t.Fatalf("startEphemeralAdapter: %v", err)
	}
	defer adapterSess.closeFn()
	runner, cleanupRunner, err := buildSandboxRunner(cfg)
	if err != nil {
		t.Fatalf("buildSandboxRunner: %v", err)
	}
	defer cleanupRunner()

	profile := ProfileSpec{
		Name:              "cap-probe",
		Adapter:           "command",
		DefaultConfidence: 0.80,
		// key: harness adapter option key; value: option string value
		Options: map[string]string{"argv": string(argvJSON)},
		Budget: &reviewv1.TaskBudget{
			MaxRequests:             2,
			MaxContextTokens:        32768,
			MaxTotalTokens:          300000,
			NormalizerReserveTokens: 20000,
			WallClockSeconds:        60,
		},
	}

	res, err := executeProfileOnCommit(ctx, cfg, adapterSess, runner, commits[0], profile, 0)
	if err != nil {
		t.Fatalf("executeProfileOnCommit: %v", err)
	}
	if res.Requests != 2 || res.RejectedRequests != 1 {
		t.Fatalf("expected 2 admitted and 1 rejected harness requests, got %+v", res)
	}
	if res.NormalizerPath != "llm" || res.NormalizeRequests != 1 {
		t.Fatalf("expected the kit to normalize the partial output once, got %+v", res)
	}
	if res.Status != "harness_failed" || res.Report.GetProvenance().GetStatus() != reviewv1.ReportStatus_REPORT_STATUS_HARNESS_FAILED {
		t.Fatalf("a run cut off by the request cap without a report file must fail, got status=%q provenance=%v", res.Status, res.Report.GetProvenance())
	}
}

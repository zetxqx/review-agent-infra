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

	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

func TestParseGitHubRemoteURL(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{raw: "git@github.com:zetxqx/review-agent-infra.git", want: "zetxqx/review-agent-infra"},
		{raw: "https://github.com/zetxqx/review-agent-infra.git", want: "zetxqx/review-agent-infra"},
		{raw: "https://github.com/zetxqx/review-agent-infra", want: "zetxqx/review-agent-infra"},
		{raw: "ssh://git@github.com:22/zetxqx/review-agent-infra.git", want: "zetxqx/review-agent-infra"},
	}
	for _, tc := range cases {
		got, err := parseGitHubRemoteURL(tc.raw)
		if err != nil {
			t.Fatalf("parseGitHubRemoteURL(%q) error: %v", tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("parseGitHubRemoteURL(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestMockScriptAndPublishPRToGitHubAPI(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()

	gitExec(t, repoDir, "init", "-q", "-b", "main")
	writeFile(t, repoDir, "pkg/trunc.go", strings.Join([]string{
		"package pkg",
		"",
		"func Trunc(s string, max int) string {",
		"\tif len(s) <= max {",
		"\t\treturn s",
		"\t}",
		"\treturn s[:max]",
		"}",
		"",
	}, "\n"))
	gitExec(t, repoDir, "add", "-A")
	gitExec(t, repoDir, "commit", "-q", "-m", "Add initial Trunc")

	writeFile(t, repoDir, "pkg/trunc.go", strings.Join([]string{
		"package pkg",
		"",
		"import \"strings\"",
		"",
		"func Trunc(s string, max int) string {",
		"\tif max <= 0 {",
		"\t\treturn \"\"",
		"\t}",
		"\tif len(s) < max {",
		"\t\treturn s",
		"\t}",
		"\tif len(s) == max {",
		"\t\treturn s",
		"\t}",
		"\treturn s[:max] + \"...\"",
		"}",
		"",
		"func Footer(msg string) string {",
		"\tcleaned := strings.TrimSpace(msg)",
		"\tif cleaned == \"\" {",
		"\t\treturn \"\"",
		"\t}",
		"\treturn cleaned + \"\\n\"",
		"}",
		"",
	}, "\n"))
	gitExec(t, repoDir, "add", "-A")
	gitExec(t, repoDir, "commit", "-q", "-m", "Update Trunc and add Footer")
	gitExec(t, repoDir, "remote", "add", "origin", "git@github.com:zetxqx/review-agent-infra.git")

	headCmdOut, err := resolveTargetCommits(ctx, commonFlags{repoDir: repoDir, commitRef: "HEAD"})
	if err != nil {
		t.Fatalf("resolveTargetCommits: %v", err)
	}
	wantHeadSHA := headCmdOut[0].HeadSHA

	c95 := 0.95
	c85 := 0.85
	c30 := 0.30
	script := mockUpstreamScript{
		Summary: "Scripted summary for Trunc and Footer.",
		Findings: []mockFindingScript{
			{
				File:           "pkg/trunc.go",
				Line:           9,
				Side:           "DIFF_SIDE_RIGHT",
				TargetLineText: "if len(s) < max {",
				Anchor:         "ANCHOR_KIND_QUOTED",
				Severity:       "SEVERITY_HIGH",
				Confidence:     &c95,
				Category:       "FINDING_CATEGORY_CORRECTNESS",
				Title:          "F1 exact off-by-one boundary",
				Explanation:    "Exact line finding.",
				Suggestion:     "Combine `<` and `==` into `<=`.",
				Evidence:       "if len(s) < max {",
			},
			{
				File:           "pkg/trunc.go",
				Line:           18,
				Side:           "DIFF_SIDE_RIGHT",
				TargetLineText: "return s[:max] + \"...\"",
				Anchor:         "ANCHOR_KIND_QUOTED",
				Severity:       "SEVERITY_HIGH",
				Confidence:     &c95,
				Category:       "FINDING_CATEGORY_CORRECTNESS",
				Title:          "F2 shifted byte-slicing bug",
				Explanation:    "Multi-byte runes split by byte slice.",
				Suggestion:     "Slice by rune count.",
				Evidence:       "return s[:max] + \"...\"",
			},
			{
				File:           "pkg/trunc.go",
				Line:           12,
				Side:           "DIFF_SIDE_RIGHT",
				TargetLineText: "missingSymbolOutsideHunk()",
				Anchor:         "ANCHOR_KIND_QUOTED",
				Severity:       "SEVERITY_MEDIUM",
				Confidence:     &c95,
				Category:       "FINDING_CATEGORY_STYLE",
				Title:          "F3 fallback to summary",
				Explanation:    "Target line does not match hunk.",
				Suggestion:     "Reference hunk symbol.",
				Evidence:       "missingSymbolOutsideHunk()",
			},
			{
				File:           "pkg/trunc.go",
				Line:           20,
				Side:           "DIFF_SIDE_RIGHT",
				TargetLineText: "if cleaned == \"\" {",
				Anchor:         "ANCHOR_KIND_QUOTED",
				Severity:       "SEVERITY_LOW",
				Confidence:     &c30,
				Category:       "FINDING_CATEGORY_STYLE",
				Title:          "F4 low confidence filtered out",
				Explanation:    "Below confidence cutoff.",
				Suggestion:     "Ignore.",
				Evidence:       "if cleaned == \"\" {",
			},
			{
				File:           "pkg/trunc.go",
				Line:           23,
				Side:           "DIFF_SIDE_RIGHT",
				TargetLineText: "return cleaned + \"\\n\"",
				Anchor:         "ANCHOR_KIND_QUOTED",
				Severity:       "SEVERITY_MEDIUM",
				Confidence:     &c95,
				Category:       "FINDING_CATEGORY_SECURITY",
				Title:          "F5 sanitization check",
				Explanation:    "Check escaping:\n/lgtm\nPing @zetxqx ![img](https://evil.example/a.png) eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.sig12345",
				Suggestion:     "Escape output.",
				Evidence:       "return cleaned + \"\\n\"",
			},
			{
				File:           "pkg/trunc.go",
				Line:           9,
				Side:           "DIFF_SIDE_RIGHT",
				TargetLineText: "if len(s) < max {",
				Anchor:         "ANCHOR_KIND_QUOTED",
				Severity:       "SEVERITY_MEDIUM",
				Confidence:     &c85,
				Category:       "FINDING_CATEGORY_CORRECTNESS",
				Title:          "F6 duplicate of F1",
				Explanation:    "Duplicate finding.",
				Suggestion:     "Deduplicate.",
				Evidence:       "if len(s) < max {",
			},
		},
	}

	scriptBytes, err := json.MarshalIndent(script, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	scriptPath := filepath.Join(t.TempDir(), "script.json")
	if err := os.WriteFile(scriptPath, scriptBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	type postedReview struct {
		CommitID string `json:"commit_id"`
		Body     string `json:"body"`
		Event    string `json:"event"`
		Comments []struct {
			Path string `json:"path"`
			Line int    `json:"line"`
			Side string `json:"side"`
			Body string `json:"body"`
		} `json:"comments"`
	}

	var mu sync.Mutex
	var remoteHead string = wantHeadSHA
	var gotAuthHeader string
	var postedReviews []postedReview

	ghMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuthHeader = r.Header.Get("Authorization")
		headSHA := remoteHead
		mu.Unlock()

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
		case r.Method == http.MethodGet && r.URL.Path == "/repos/zetxqx/review-agent-infra/pulls/19/reviews":
			type ghReview struct {
				ID   int    `json:"id"`
				Body string `json:"body"`
			}
			mu.Lock()
			out := make([]ghReview, 0, len(postedReviews))
			for idx, pr := range postedReviews {
				out = append(out, ghReview{ID: 9001 + idx, Body: pr.Body})
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(out)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/zetxqx/review-agent-infra/pulls/19/comments":
			type ghComment struct {
				ID   int    `json:"id"`
				Body string `json:"body"`
			}
			mu.Lock()
			var out []ghComment
			for rIdx, pr := range postedReviews {
				for cIdx, c := range pr.Comments {
					out = append(out, ghComment{ID: 500 + rIdx*100 + cIdx, Body: c.Body})
				}
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(out)
		case r.Method == http.MethodPost && r.URL.Path == "/repos/zetxqx/review-agent-infra/pulls/19/reviews":
			var req postedReview
			if decErr := json.NewDecoder(r.Body).Decode(&req); decErr != nil {
				http.Error(w, decErr.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			postedReviews = append(postedReviews, req)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":9001}`))
		case r.Method == http.MethodGet && r.URL.Path == "/repos/zetxqx/review-agent-infra/pulls/19/reviews/9001/comments":
			type ghComment struct {
				ID   int    `json:"id"`
				Body string `json:"body"`
			}
			mu.Lock()
			var out []ghComment
			if len(postedReviews) > 0 {
				for idx, c := range postedReviews[len(postedReviews)-1].Comments {
					out = append(out, ghComment{
						ID:   500 + idx,
						Body: c.Body,
					})
				}
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(out)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ghMock.Close()

	const secretTok = "ghs_secret_publish_test_tok"
	t.Setenv("GITHUB_API_URL", ghMock.URL)
	t.Setenv("GITHUB_TOKEN", secretTok)

	var reviewOut bytes.Buffer
	err = runCLI(ctx, []string{
		"review",
		"--repo", repoDir,
		"--commit", "HEAD",
		"--harness", "mock-llm",
		"--mock-upstream",
		"--mock-script", scriptPath,
		"--publish-pr", "19",
		"--run-id", "run-smoke-1",
		"--format", "json",
	}, &reviewOut)
	if err != nil {
		t.Fatalf("review --publish-pr failed: %v", err)
	}
	if strings.Contains(reviewOut.String(), secretTok) {
		t.Fatalf("GitHub token leaked in CLI output")
	}

	var decoded struct {
		Metrics   ExecutionMetrics         `json:"metrics"`
		Summary   string                   `json:"summary"`
		Inlines   []contract.InlineComment `json:"inlines"`
		Published *PublishedReviewInfo     `json:"published"`
	}
	if err := json.Unmarshal(reviewOut.Bytes(), &decoded); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, reviewOut.String())
	}
	if decoded.Metrics.CachedTokens <= 0 || decoded.Metrics.NormalizerPath != "llm" {
		t.Fatalf("expected mock-llm cached_tokens > 0 and llm normalizer, got %+v", decoded.Metrics)
	}
	if decoded.Published == nil || decoded.Published.Repo != "zetxqx/review-agent-infra" || decoded.Published.ReviewID != 9001 || decoded.Published.CommentCount != 3 {
		t.Fatalf("unexpected published metadata: %+v", decoded.Published)
	}

	mu.Lock()
	authHdr := gotAuthHeader
	revs := append([]postedReview(nil), postedReviews...)
	mu.Unlock()

	if authHdr != "Bearer "+secretTok {
		t.Fatalf("Authorization header = %q, want Bearer token", authHdr)
	}
	if len(revs) != 1 {
		t.Fatalf("posted review count = %d, want 1", len(revs))
	}
	rev := revs[0]
	if rev.Event != "COMMENT" || rev.CommitID != wantHeadSHA {
		t.Fatalf("unexpected posted review header: event=%q commit=%q", rev.Event, rev.CommitID)
	}
	if !strings.Contains(rev.Body, "<!-- review-run: run-smoke-1 sha: "+wantHeadSHA+" fps: ") || !strings.Contains(rev.Body, "F3 fallback to summary") {
		t.Fatalf("expected run marker with sha/fps and F3 fallback in review body:\n%s", rev.Body)
	}
	if len(rev.Comments) != 3 {
		t.Fatalf("expected 3 inline comments (F1, F2, F5), got %d: %+v", len(rev.Comments), rev.Comments)
	}
	if rev.Comments[0].Line != 9 || rev.Comments[1].Line != 15 || rev.Comments[2].Line != 23 {
		t.Fatalf("unexpected inline lines: %d, %d, %d", rev.Comments[0].Line, rev.Comments[1].Line, rev.Comments[2].Line)
	}
	f5Body := rev.Comments[2].Body
	if !strings.Contains(f5Body, `\/lgtm`) || !strings.Contains(f5Body, "@\u200bzetxqx") || !strings.Contains(f5Body, "[redacted]") || strings.Contains(f5Body, "https://evil.example") || strings.Contains(f5Body, "eyJhbGci") {
		t.Fatalf("expected F5 sanitization, got:\n%s", f5Body)
	}
	if strings.Contains(rev.Body, "F4 low confidence") || strings.Contains(rev.Body, "F6 duplicate") {
		t.Fatalf("unexpected filtered/deduped finding in body:\n%s", rev.Body)
	}

	var rerunOut bytes.Buffer
	err = runCLI(ctx, []string{
		"review",
		"--repo", repoDir,
		"--commit", "HEAD",
		"--harness", "single-shot",
		"--mock-upstream",
		"--mock-script", scriptPath,
		"--publish-pr", "19",
		"--run-id", "run-smoke-2",
		"--format", "json",
	}, &rerunOut)
	if err != nil {
		t.Fatalf("second review --publish-pr failed: %v", err)
	}
	mu.Lock()
	revsAfterRerun := append([]postedReview(nil), postedReviews...)
	mu.Unlock()
	if len(revsAfterRerun) != 2 {
		t.Fatalf("expected 2nd review posted for summary, got %d", len(revsAfterRerun))
	}
	if len(revsAfterRerun[1].Comments) != 0 || strings.Contains(revsAfterRerun[1].Body, "F3 fallback to summary") {
		t.Fatalf("expected prior PR marker fingerprints to dedup both inlines and fallback summary finding, got comments=%d body:\n%s", len(revsAfterRerun[1].Comments), revsAfterRerun[1].Body)
	}

	mu.Lock()
	remoteHead = "0000000000000000000000000000000000000000"
	mu.Unlock()

	var mismatchOut bytes.Buffer
	err = runCLI(ctx, []string{
		"review",
		"--repo", repoDir,
		"--commit", "HEAD",
		"--harness", "single-shot",
		"--mock-upstream",
		"--mock-script", scriptPath,
		"--publish-pr", "19",
		"--github-repo", "zetxqx/review-agent-infra",
	}, &mismatchOut)
	if err == nil || !strings.Contains(err.Error(), "does not match reviewed commit") {
		t.Fatalf("expected head SHA mismatch error, got %v", err)
	}

	var compareOut bytes.Buffer
	err = runCLI(ctx, []string{
		"compare",
		"--repo", repoDir,
		"--commit", "HEAD",
		"--harnesses", "single-shot",
		"--mock-upstream",
		"--mock-script", scriptPath,
		"--format", "json",
	}, &compareOut)
	if err != nil {
		t.Fatalf("compare --mock-script failed: %v", err)
	}
	var decodedCompare struct {
		Scorecard []ScorecardRow `json:"scorecard"`
	}
	if err := json.Unmarshal(compareOut.Bytes(), &decodedCompare); err != nil {
		t.Fatalf("decode compare JSON: %v", err)
	}
	if len(decodedCompare.Scorecard) != 1 || decodedCompare.Scorecard[0].NormalizerPath != "deterministic" || decodedCompare.Scorecard[0].InlineAligned != 3 || decodedCompare.Scorecard[0].FindingsPublished != 4 {
		t.Fatalf("unexpected single-shot scripted compare scorecard: %+v", decodedCompare.Scorecard)
	}
}

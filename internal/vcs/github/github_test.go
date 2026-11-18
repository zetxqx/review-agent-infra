package github

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

func TestWebhookSignatureAndEventParsing(t *testing.T) {
	ctx := context.Background()
	secret := []byte("top-secret-webhook-hmac")

	prSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/llm-d/router/pulls/42" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{
				"number": 42,
				"draft": false,
				"user": {"login": "ext-contributor", "type": "User"},
				"base": {"sha": "base111"},
				"head": {"sha": "head222", "repo": {"full_name": "ext/router", "fork": true}}
			}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer prSrv.Close()

	plugin := NewPlugin(Config{
		WebhookSecret: secret,
		StaticToken:   "ghs_test",
		APIBaseURL:    prSrv.URL,
	})

	prBody := []byte(`{
		"action": "synchronize",
		"number": 17,
		"pull_request": {
			"number": 17,
			"draft": false,
			"author_association": "MEMBER",
			"user": {"login": "alice", "type": "User"},
			"base": {"sha": "aaa111", "repo": {"full_name": "llm-d/router", "fork": false}},
			"head": {"sha": "bbb222", "repo": {"full_name": "llm-d/router", "fork": false}}
		},
		"repository": {"full_name": "llm-d/router"},
		"sender": {"login": "alice", "type": "User"}
	}`)

	badHdr := make(http.Header)
	badHdr.Set("X-GitHub-Event", "pull_request")
	badHdr.Set("X-Hub-Signature-256", "sha256=deadbeef")
	if _, err := plugin.VerifyAndParseWebhook(ctx, badHdr, prBody); err == nil {
		t.Fatal("expected invalid signature error")
	}

	hdr := make(http.Header)
	hdr.Set("X-GitHub-Delivery", "deliv-1")
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", SignWebhookPayload(secret, prBody))

	ev, err := plugin.VerifyAndParseWebhook(ctx, hdr, prBody)
	if err != nil {
		t.Fatalf("VerifyAndParseWebhook PR: %v", err)
	}
	if ev.EventKind != reviewv1.EventKind_EVENT_KIND_REVIEW || ev.TriggerType != reviewv1.TriggerType_TRIGGER_TYPE_AUTO {
		t.Fatalf("unexpected PR event kind/type: %+v", ev)
	}
	if ev.Repo != "llm-d/router" || ev.PrNumber != 17 || ev.BaseSha != "aaa111" || ev.HeadSha != "bbb222" || ev.IsFork || !ev.IsMaintainer {
		t.Fatalf("unexpected PR fields: %+v", ev)
	}

	closeBody := bytes.ReplaceAll(prBody, []byte(`"synchronize"`), []byte(`"closed"`))
	closeHdr := make(http.Header)
	closeHdr.Set("X-GitHub-Delivery", "deliv-2")
	closeHdr.Set("X-GitHub-Event", "pull_request")
	closeHdr.Set("X-Hub-Signature-256", SignWebhookPayload(secret, closeBody))
	closeEv, err := plugin.VerifyAndParseWebhook(ctx, closeHdr, closeBody)
	if err != nil || closeEv.EventKind != reviewv1.EventKind_EVENT_KIND_CLOSE {
		t.Fatalf("expected CLOSE event, got %+v err=%v", closeEv, err)
	}

	cmdBody := []byte(`{
		"action": "created",
		"issue": {
			"number": 42,
			"user": {"login": "ext-contributor", "type": "User"},
			"pull_request": {"url": "https://api.github.com/repos/llm-d/router/pulls/42"}
		},
		"comment": {
			"id": 987654,
			"body": "Checking:\n/review openhands\n",
			"author_association": "OWNER",
			"user": {"login": "maintainer1", "type": "User"}
		},
		"repository": {"full_name": "llm-d/router"},
		"sender": {"login": "maintainer1", "type": "User"}
	}`)
	cmdHdr := make(http.Header)
	cmdHdr.Set("X-GitHub-Delivery", "deliv-3")
	cmdHdr.Set("X-GitHub-Event", "issue_comment")
	cmdHdr.Set("X-Hub-Signature-256", SignWebhookPayload(secret, cmdBody))

	cmdEv, err := plugin.VerifyAndParseWebhook(ctx, cmdHdr, cmdBody)
	if err != nil {
		t.Fatalf("VerifyAndParseWebhook /review: %v", err)
	}
	if cmdEv.EventKind != reviewv1.EventKind_EVENT_KIND_REVIEW ||
		cmdEv.TriggerType != reviewv1.TriggerType_TRIGGER_TYPE_MANUAL_COMMAND ||
		!cmdEv.IsMaintainer ||
		cmdEv.ForceRerunSeq != 987654 ||
		cmdEv.GetRequestedHarness() != "openhands" ||
		cmdEv.BaseSha != "base111" ||
		cmdEv.HeadSha != "head222" ||
		!cmdEv.IsFork {
		t.Fatalf("unexpected manual /review event: %+v", cmdEv)
	}

	fencedBody := bytes.ReplaceAll(cmdBody, []byte("Checking:\\n/review openhands\\n"), []byte("```\\n/review openhands\\n```"))
	fencedHdr := make(http.Header)
	fencedHdr.Set("X-GitHub-Event", "issue_comment")
	fencedHdr.Set("X-Hub-Signature-256", SignWebhookPayload(secret, fencedBody))
	fencedEv, err := plugin.VerifyAndParseWebhook(ctx, fencedHdr, fencedBody)
	if err != nil || fencedEv.EventKind != reviewv1.EventKind_EVENT_KIND_IGNORE {
		t.Fatalf("expected fenced /review to be ignored, got %+v err=%v", fencedEv, err)
	}

	replyBody := []byte(`{
		"action": "created",
		"comment": {
			"id": 5002,
			"in_reply_to_id": 5001,
			"body": "Fixed in latest commit!",
			"user": {"login": "alice", "type": "User"}
		},
		"pull_request": {
			"number": 17,
			"user": {"login": "alice", "type": "User"},
			"base": {"sha": "aaa111"},
			"head": {"sha": "bbb222"}
		},
		"repository": {"full_name": "llm-d/router"},
		"sender": {"login": "alice", "type": "User"}
	}`)
	replyHdr := make(http.Header)
	replyHdr.Set("X-GitHub-Delivery", "deliv-4")
	replyHdr.Set("X-GitHub-Event", "pull_request_review_comment")
	replyHdr.Set("X-Hub-Signature-256", SignWebhookPayload(secret, replyBody))
	replyEv, err := plugin.VerifyAndParseWebhook(ctx, replyHdr, replyBody)
	if err != nil || replyEv.EventKind != reviewv1.EventKind_EVENT_KIND_FEEDBACK || replyEv.ForceRerunSeq != 5001 {
		t.Fatalf("unexpected feedback event: %+v err=%v", replyEv, err)
	}
}

func TestAppJWTAndCachedInstallationTokenExchange(t *testing.T) {
	ctx := context.Background()
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	pkcs8DER, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8DER})
	parsedKey, err := ParseRSAPAPrivateKeyHelper(pemBytes)
	if err != nil {
		t.Fatalf("ParseRSAPrivateKeyPEM: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	var installLookups atomic.Int32
	var tokenExchanges atomic.Int32

	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/llm-d/router/installation":
			installLookups.Add(1)
			auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			tok, parseErr := jwt.Parse(auth, func(_ *jwt.Token) (any, error) {
				return &privKey.PublicKey, nil
			})
			if parseErr != nil || !tok.Valid {
				http.Error(w, "bad app jwt", http.StatusUnauthorized)
				return
			}
			_, _ = io.WriteString(w, `{"id": 7788}`)
		case "/app/installations/7788/access_tokens":
			seq := tokenExchanges.Add(1)
			exp := now.Add(30 * time.Minute).Format(time.RFC3339)
			fmt.Fprintf(w, `{"token": "ghs_install_%d", "expires_at": %q}`, seq, exp)
		case "/repos/llm-d/router/pulls/10":
			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ghs_install_") {
				http.Error(w, "missing install token", http.StatusUnauthorized)
				return
			}
			_, _ = io.WriteString(w, `{"number": 10, "head": {"sha": "sha999"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ghSrv.Close()

	curNow := now
	plugin := NewPlugin(Config{
		AppID:      12345,
		PrivateKey: parsedKey,
		APIBaseURL: ghSrv.URL,
		Now:        func() time.Time { return curNow },
	})

	sha1, err := plugin.GetHeadSHA(ctx, "llm-d/router", 10)
	if err != nil || sha1 != "sha999" {
		t.Fatalf("first GetHeadSHA = %q err=%v", sha1, err)
	}
	sha2, err := plugin.GetHeadSHA(ctx, "llm-d/router", 10)
	if err != nil || sha2 != "sha999" {
		t.Fatalf("second GetHeadSHA = %q err=%v", sha2, err)
	}
	if installLookups.Load() != 1 || tokenExchanges.Load() != 1 {
		t.Fatalf("expected cached install+token (1,1), got (%d,%d)", installLookups.Load(), tokenExchanges.Load())
	}

	curNow = now.Add(29 * time.Minute)
	if _, err := plugin.GetHeadSHA(ctx, "llm-d/router", 10); err != nil {
		t.Fatalf("post-expiry GetHeadSHA: %v", err)
	}
	if tokenExchanges.Load() != 2 {
		t.Fatalf("expected token refresh on near-expiry, got %d exchanges", tokenExchanges.Load())
	}
}

func ParseRSAPAPrivateKeyHelper(pemBytes []byte) (*rsa.PrivateKey, error) {
	return ParseRSAPrivateKeyPEM(pemBytes)
}

func TestFetchDiffTarballRulesCheckLogsPublishIdempotencyAndFeedback(t *testing.T) {
	ctx := context.Background()

	type storedComment struct {
		ID       int64  `json:"id"`
		ReviewID int64  `json:"pull_request_review_id"`
		Body     string `json:"body"`
	}
	type storedReview struct {
		ID   int64  `json:"id"`
		Body string `json:"body"`
	}

	var reviews []storedReview
	var comments []storedComment
	var postReviewCalls atomic.Int32
	var nextReviewID int64 = 100
	var nextCommentID int64 = 500

	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/repos/llm-d/router/compare/base1..."):
			_, _ = io.WriteString(w, strings.Join([]string{
				"diff --git a/pkg/router/scorer.go b/pkg/router/scorer.go",
				"--- a/pkg/router/scorer.go",
				"+++ b/pkg/router/scorer.go",
				"@@ -1,2 +1,3 @@",
				" package router",
				"+func Score() int { return 42 }",
				"diff --git a/logo.png b/logo.png",
				"Binary files a/logo.png and b/logo.png differ",
			}, "\n"))
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/tarball/head2":
			_, _ = io.WriteString(w, "tarball-bytes-within-cap")
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/tarball/huge":
			_, _ = io.WriteString(w, strings.Repeat("x", 1024))
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/git/trees/base1":
			_, _ = io.WriteString(w, `{
				"tree": [
					{"path": "AGENTS.md", "type": "blob"},
					{"path": "pkg/AGENTS.md", "type": "blob"},
					{"path": "unrelated/AGENTS.md", "type": "blob"},
					{"path": "docs/review/concurrency.md", "type": "blob"}
				]
			}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/contents/AGENTS.md":
			_, _ = io.WriteString(w, "Root base rule")
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/contents/pkg/AGENTS.md":
			_, _ = io.WriteString(w, "Pkg scoped rule")
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/contents/docs/review/concurrency.md":
			_, _ = io.WriteString(w, "Concurrency checklist")
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/commits/head2/check-runs":
			_, _ = io.WriteString(w, `{
				"check_runs": [
					{"id": 1, "name": "build", "conclusion": "success", "output": {"title": "green"}},
					{"id": 2, "name": "unit-tests", "conclusion": "failure", "output": {"title": "TestScorer failed", "summary": "race detected", "text": "goroutine 17 data race details"}}
				]
			}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/pulls/12/reviews":
			_ = json.NewEncoder(w).Encode(reviews)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/repos/llm-d/router/pulls/12/comments"):
			_ = json.NewEncoder(w).Encode(comments)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/repos/llm-d/router/pulls/12/reviews/") && strings.HasSuffix(r.URL.Path, "/comments"):
			var filtered []storedComment
			for _, c := range comments {
				if strings.Contains(r.URL.Path, fmt.Sprintf("/reviews/%d/comments", c.ReviewID)) {
					filtered = append(filtered, c)
				}
			}
			_ = json.NewEncoder(w).Encode(filtered)
		case r.Method == http.MethodPost && r.URL.Path == "/repos/llm-d/router/pulls/12/reviews":
			postReviewCalls.Add(1)
			var req struct {
				Body     string `json:"body"`
				Event    string `json:"event"`
				Comments []struct {
					Path string `json:"path"`
					Line int    `json:"line"`
					Body string `json:"body"`
				} `json:"comments"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req.Event != "COMMENT" {
				http.Error(w, "only COMMENT allowed", http.StatusBadRequest)
				return
			}
			nextReviewID++
			rv := storedReview{ID: nextReviewID, Body: req.Body}
			reviews = append(reviews, rv)
			for _, c := range req.Comments {
				nextCommentID++
				comments = append(comments, storedComment{ID: nextCommentID, ReviewID: rv.ID, Body: c.Body})
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(rv)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/pulls/comments/501/reactions":
			_, _ = io.WriteString(w, `[{"content": "+1"}, {"content": "+1"}, {"content": "-1"}]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ghSrv.Close()

	plugin := NewPlugin(Config{
		StaticToken: "ghs_test",
		APIBaseURL:  ghSrv.URL,
		MaxTarBytes: 128,
	})

	diffPatch, tarRC, err := plugin.FetchDiffAndTarball(ctx, "llm-d/router", 12, "base1", "head2")
	if err != nil {
		t.Fatalf("FetchDiffAndTarball: %v", err)
	}
	tarData, err := io.ReadAll(tarRC)
	_ = tarRC.Close()
	if err != nil || string(tarData) != "tarball-bytes-within-cap" {
		t.Fatalf("unexpected tarball bytes %q err=%v", string(tarData), err)
	}
	if strings.Contains(string(diffPatch), "Binary files") || !strings.Contains(string(diffPatch), "+func Score()") {
		t.Fatalf("unexpected diffPatch:\n%s", string(diffPatch))
	}

	_, hugeRC, err := plugin.FetchDiffAndTarball(ctx, "llm-d/router", 12, "base1", "huge")
	if err != nil {
		t.Fatalf("FetchDiffAndTarball huge open: %v", err)
	}
	_, hugeReadErr := io.ReadAll(hugeRC)
	_ = hugeRC.Close()
	if hugeReadErr == nil {
		t.Fatal("expected size cap error reading oversize tarball")
	}

	rules, err := plugin.FetchBaseRules(ctx, "llm-d/router", "base1", []string{"pkg/router/scorer.go"})
	if err != nil {
		t.Fatalf("FetchBaseRules: %v", err)
	}
	if string(rules["AGENTS.md"]) != "Root base rule" ||
		string(rules["pkg/AGENTS.md"]) != "Pkg scoped rule" ||
		string(rules["docs/review/concurrency.md"]) != "Concurrency checklist" {
		t.Fatalf("unexpected base rules: %v", rules)
	}
	if _, hasUnrelated := rules["unrelated/AGENTS.md"]; hasUnrelated {
		t.Fatalf("unexpected unrelated/AGENTS.md in rules")
	}

	logs, err := plugin.FetchCheckLogs(ctx, "llm-d/router", 12, "head2", 60)
	if err != nil {
		t.Fatalf("FetchCheckLogs: %v", err)
	}
	if len(logs) != 60 || !strings.Contains(string(logs), "unit-tests") {
		t.Fatalf("unexpected truncated check logs (%d bytes): %q", len(logs), string(logs))
	}

	pubReq := contract.PublishRequest{
		Repo:     "llm-d/router",
		PRNumber: 12,
		CommitID: "head2",
		RunID:    "run-abc",
		Event:    "APPROVE",
		Summary:  "Found 1 concurrency issue.",
		Inlines: []contract.InlineComment{
			{
				Fingerprint: "fp111",
				Category:    "FINDING_CATEGORY_CONCURRENCY",
				File:        "pkg/router/scorer.go",
				Line:        2,
				Side:        "RIGHT",
				Body:        "**[HIGH]** Race on Score",
			},
		},
	}

	res1, err := plugin.PublishReview(ctx, pubReq)
	if err != nil {
		t.Fatalf("first PublishReview: %v", err)
	}
	if res1.ReviewID != 101 || len(res1.Comments) != 1 || res1.Comments[0].CommentID != 501 || res1.Comments[0].Fingerprint != "fp111" {
		t.Fatalf("unexpected first PublishResult: %+v", res1)
	}

	resReplay, err := plugin.PublishReview(ctx, pubReq)
	if err != nil {
		t.Fatalf("idempotent replay PublishReview: %v", err)
	}
	if postReviewCalls.Load() != 1 || resReplay.ReviewID != 101 {
		t.Fatalf("expected idempotent skip (calls=1, id=101), got calls=%d res=%+v", postReviewCalls.Load(), resReplay)
	}

	resRound2, err := plugin.PublishReview(ctx, contract.PublishRequest{
		Repo:     "llm-d/router",
		PRNumber: 12,
		CommitID: "head3",
		RunID:    "run-def",
		Summary:  "Round 2 review.",
		Inlines: []contract.InlineComment{
			{Fingerprint: "fp111", Category: "FINDING_CATEGORY_CONCURRENCY", File: "pkg/router/scorer.go", Line: 2, Side: "RIGHT", Body: "dup"},
			{Fingerprint: "fp222", Category: "FINDING_CATEGORY_CORRECTNESS", File: "pkg/router/scorer.go", Line: 3, Side: "RIGHT", Body: "new"},
		},
	})
	if err != nil {
		t.Fatalf("round 2 PublishReview: %v", err)
	}
	if len(comments) != 2 || len(resRound2.Comments) != 1 || resRound2.Comments[0].Fingerprint != "fp222" {
		t.Fatalf("expected fp111 skipped and only fp222 added (total 2 PR comments, 1 new in round 2), got comments=%d res=%+v", len(comments), resRound2)
	}

	fb, err := plugin.PollFeedback(ctx, "llm-d/router", 12, []int64{501})
	if err != nil {
		t.Fatalf("PollFeedback: %v", err)
	}
	if len(fb) != 1 || fb[0].ThumbsUp != 2 || fb[0].ThumbsDown != 1 {
		t.Fatalf("unexpected feedback samples: %+v", fb)
	}
}

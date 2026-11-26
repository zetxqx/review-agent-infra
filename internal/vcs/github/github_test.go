package github

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

func verifyRS256JWT(raw string, pub *rsa.PublicKey) error {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return errors.New("malformed jwt")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	return rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig)
}

func TestWebhookSignatureAndEventParsing(t *testing.T) {
	ctx := context.Background()
	secret := []byte("top-secret-webhook-hmac")

	prSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/llm-d/router/pulls/42" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{
				"number": 42,
				"title": "Fork PR title from API",
				"body": "Fork PR body from API",
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
			"title": "Fix scorer race",
			"body": "Uses mutex for shared map",
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
	if ev.Repo != "llm-d/router" || ev.PrNumber != 17 || ev.BaseSha != "aaa111" || ev.HeadSha != "bbb222" || ev.IsFork || !ev.IsMaintainer || ev.PrTitle != "Fix scorer race" || ev.PrBody != "Uses mutex for shared map" {
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
		cmdEv.PrTitle != "Fork PR title from API" ||
		cmdEv.PrBody != "Fork PR body from API" ||
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
			if err := verifyRS256JWT(auth, &privKey.PublicKey); err != nil {
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

	listedFPs, err := plugin.ListPublishedFingerprints(ctx, "llm-d/router", 12)
	if err != nil || !slices.Equal(listedFPs, []string{"fp111", "fp222"}) {
		t.Fatalf("expected ListPublishedFingerprints=[fp111 fp222], got %v err=%v", listedFPs, err)
	}

	fb, err := plugin.PollFeedback(ctx, "llm-d/router", 12, []int64{501})
	if err != nil {
		t.Fatalf("PollFeedback: %v", err)
	}
	if len(fb) != 1 || fb[0].ThumbsUp != 2 || fb[0].ThumbsDown != 1 {
		t.Fatalf("unexpected feedback samples: %+v", fb)
	}
}

func TestCheckCredentialsVerifiesAppAndMintsInstallationToken(t *testing.T) {
	ctx := context.Background()
	appKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	var tokenExchanges atomic.Int32
	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if err := verifyRS256JWT(bearer, &appKey.PublicKey); err != nil {
			http.Error(w, `{"message":"A JSON web token could not be decoded"}`, http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/app":
			_, _ = w.Write([]byte(`{"id":123,"slug":"review-bot"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/app/installations/77/access_tokens":
			tokenExchanges.Add(1)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"ghs_check_token","expires_at":"2099-01-01T00:00:00Z"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ghSrv.Close()

	good := NewPlugin(Config{AppID: 123, InstallationID: 77, PrivateKey: appKey, APIBaseURL: ghSrv.URL})
	if err := good.CheckCredentials(ctx); err != nil {
		t.Fatalf("CheckCredentials with valid App credentials: %v", err)
	}
	if tokenExchanges.Load() != 1 {
		t.Fatalf("expected one installation token exchange, got %d", tokenExchanges.Load())
	}

	wrongKey := NewPlugin(Config{AppID: 123, InstallationID: 77, PrivateKey: otherKey, APIBaseURL: ghSrv.URL})
	if err := wrongKey.CheckCredentials(ctx); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected a 401 error for a key GitHub does not accept, got %v", err)
	}

	missing := NewPlugin(Config{APIBaseURL: ghSrv.URL})
	if err := missing.CheckCredentials(ctx); err == nil {
		t.Fatal("expected an error when no GitHub credentials are configured")
	}

	static := NewPlugin(Config{StaticToken: "ghs_static", APIBaseURL: ghSrv.URL})
	if err := static.CheckCredentials(ctx); err != nil {
		t.Fatalf("CheckCredentials with a static token: %v", err)
	}
}

func TestPaginationMarkerAuthorTrustAndEmptyPublishSkip(t *testing.T) {
	ctx := context.Background()

	var postCalls atomic.Int32
	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/pulls/99/reviews":
			if page == "" || page == "1" {
				batch := make([]ghReviewRecord, listPageSize)
				batch[0] = ghReviewRecord{
					ID:   1,
					Body: "<!-- review-run: run-spoof sha: h1 fps: fp_spoofed -->",
					User: ghUser{Login: "attacker", Type: "User"},
				}
				batch[1] = ghReviewRecord{
					ID:                    2,
					Body:                  "<!-- review-run: run-other sha: h1 fps: fp_other_app -->",
					User:                  ghUser{Login: "other-bot[bot]", Type: "Bot"},
					PerformedViaGitHubApp: &ghAppMeta{ID: 999},
				}
				batch[2] = ghReviewRecord{
					ID:                    3,
					Body:                  "<!-- review-run: run-1 sha: h1 fps: fp_page1_app -->",
					User:                  ghUser{Login: "review-bot[bot]", Type: "Bot"},
					PerformedViaGitHubApp: &ghAppMeta{ID: 123},
				}
				batch[3] = ghReviewRecord{
					ID:   4,
					Body: "<!-- review-run: run-2 sha: h1 fps: fp_page1_bot -->",
					User: ghUser{Login: "review-bot[bot]", Type: "Bot"},
				}
				_ = json.NewEncoder(w).Encode(batch)
				return
			}
			if page == "2" {
				_ = json.NewEncoder(w).Encode([]ghReviewRecord{
					{
						ID:                    101,
						Body:                  "<!-- review-run: run-3 sha: h2 fps: fp_page2 -->",
						User:                  ghUser{Login: "review-bot[bot]", Type: "Bot"},
						PerformedViaGitHubApp: &ghAppMeta{ID: 123},
					},
				})
				return
			}
			_ = json.NewEncoder(w).Encode([]ghReviewRecord{})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/pulls/99/comments":
			if page == "" || page == "1" {
				batch := make([]ghCommentRecord, listPageSize)
				batch[0] = ghCommentRecord{
					ID:   10,
					Body: "spoofed\n<!-- fp:fp_spoof_inline -->",
					User: ghUser{Login: "attacker", Type: "User"},
				}
				_ = json.NewEncoder(w).Encode(batch)
				return
			}
			if page == "2" {
				_ = json.NewEncoder(w).Encode([]ghCommentRecord{
					{
						ID:                    110,
						Body:                  "real\n<!-- fp:fp_comment_page2 -->",
						User:                  ghUser{Login: "review-bot[bot]", Type: "Bot"},
						PerformedViaGitHubApp: &ghAppMeta{ID: 123},
					},
				})
				return
			}
			_ = json.NewEncoder(w).Encode([]ghCommentRecord{})
		case r.Method == http.MethodPost && r.URL.Path == "/repos/llm-d/router/pulls/99/reviews":
			postCalls.Add(1)
			_, _ = io.WriteString(w, `{"id":555}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/pulls/99/reviews/555/comments":
			_ = json.NewEncoder(w).Encode([]ghCommentRecord{
				{ID: 777, Body: "posted\n<!-- fp:fp_spoof_inline -->"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ghSrv.Close()

	plugin := NewPlugin(Config{
		AppID:       123,
		StaticToken: "ghs_test",
		APIBaseURL:  ghSrv.URL,
	})

	fps, err := plugin.ListPublishedFingerprints(ctx, "llm-d/router", 99)
	if err != nil {
		t.Fatalf("ListPublishedFingerprints: %v", err)
	}
	wantFPs := []string{"fp_page1_app", "fp_page1_bot", "fp_page2"}
	if !slices.Equal(fps, wantFPs) {
		t.Fatalf("ListPublishedFingerprints = %v, want %v", fps, wantFPs)
	}

	skipRes, err := plugin.PublishReview(ctx, contract.PublishRequest{
		Repo:     "llm-d/router",
		PRNumber: 99,
		CommitID: "h3",
		RunID:    "run-dedup-empty",
		Summary:  "",
		Inlines: []contract.InlineComment{
			{Fingerprint: "fp_page2", File: "a.go", Line: 1, Body: "dup from review marker"},
			{Fingerprint: "fp_comment_page2", File: "b.go", Line: 2, Body: "dup from page 2 comment"},
		},
	})
	if err != nil {
		t.Fatalf("PublishReview all-deduped: %v", err)
	}
	if postCalls.Load() != 0 || skipRes.ReviewID != 0 {
		t.Fatalf("expected empty deduped PublishReview to skip POST, got calls=%d res=%+v", postCalls.Load(), skipRes)
	}

	spoofRes, err := plugin.PublishReview(ctx, contract.PublishRequest{
		Repo:     "llm-d/router",
		PRNumber: 99,
		CommitID: "h3",
		RunID:    "run-not-spoofed",
		Summary:  "",
		Inlines: []contract.InlineComment{
			{Fingerprint: "fp_spoof_inline", File: "c.go", Line: 3, Body: "real finding"},
		},
	})
	if err != nil {
		t.Fatalf("PublishReview spoofed comment must not suppress: %v", err)
	}
	if postCalls.Load() != 1 || spoofRes.ReviewID != 555 {
		t.Fatalf("expected spoofed user marker ignored and review posted, got calls=%d res=%+v", postCalls.Load(), spoofRes)
	}
}

func TestCheckCapturesBotLoginAndRejectsForeignBotMarkers(t *testing.T) {
	ctx := context.Background()
	appKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/app":
			_, _ = io.WriteString(w, `{"id":123,"slug":"review-bot"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/app/installations/77/access_tokens":
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"token":"ghs_tok","expires_at":"2099-01-01T00:00:00Z"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/pulls/15/reviews":
			_ = json.NewEncoder(w).Encode([]ghReviewRecord{
				{
					ID:   1,
					Body: "<!-- review-run: r1 sha: h1 fps: fp_foreign_bot -->",
					User: ghUser{Login: "other-bot[bot]", Type: "Bot"},
				},
				{
					ID:   2,
					Body: "<!-- review-run: r2 sha: h1 fps: fp_own_bot -->",
					User: ghUser{Login: "review-bot[bot]", Type: "Bot"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ghSrv.Close()

	plugin := NewPlugin(Config{
		AppID:          123,
		InstallationID: 77,
		PrivateKey:     appKey,
		APIBaseURL:     ghSrv.URL,
	})
	if err := plugin.CheckCredentials(ctx); err != nil {
		t.Fatalf("CheckCredentials: %v", err)
	}
	if got := plugin.auth.BotLogin(); got != "review-bot[bot]" {
		t.Fatalf("BotLogin() = %q, want review-bot[bot]", got)
	}

	fps, err := plugin.ListPublishedFingerprints(ctx, "llm-d/router", 15)
	if err != nil {
		t.Fatalf("ListPublishedFingerprints: %v", err)
	}
	if !slices.Equal(fps, []string{"fp_own_bot"}) {
		t.Fatalf("ListPublishedFingerprints = %v, want [fp_own_bot]", fps)
	}
}

func TestStaticTokenMarkerTrustRequiresTokenOwnerLogin(t *testing.T) {
	ctx := context.Background()

	var userCalls atomic.Int32
	var postCalls atomic.Int32
	var postedBody atomic.Value
	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/user":
			userCalls.Add(1)
			if r.Header.Get("Authorization") != "Bearer ghp_owner" {
				http.Error(w, "bad token", http.StatusUnauthorized)
				return
			}
			_, _ = io.WriteString(w, `{"login":"Review-Owner","type":"User"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/pulls/7/reviews":
			_ = json.NewEncoder(w).Encode([]ghReviewRecord{
				{ID: 1, Body: "<!-- review-run: run-spoof sha: h1 fps: fp_spoofed --> attacker", User: ghUser{Login: "attacker", Type: "User"}},
				{ID: 2, Body: "<!-- review-run: run-own sha: h1 fps: fp_own --> owner", User: ghUser{Login: "review-owner", Type: "User"}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/pulls/7/comments":
			_ = json.NewEncoder(w).Encode([]ghCommentRecord{
				{ID: 10, Body: "spoof\n<!-- fp:fp_spoof_inline -->", User: ghUser{Login: "attacker", Type: "User"}},
				{ID: 11, Body: "own\n<!-- fp:fp_own_inline -->", User: ghUser{Login: "review-owner", Type: "User"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/repos/llm-d/router/pulls/7/reviews":
			postCalls.Add(1)
			raw, _ := io.ReadAll(r.Body)
			postedBody.Store(string(raw))
			_, _ = io.WriteString(w, `{"id":321}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/pulls/7/reviews/321/comments":
			http.Error(w, "unavailable", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ghSrv.Close()

	plugin := NewPlugin(Config{StaticToken: "ghp_owner", APIBaseURL: ghSrv.URL})

	fps, err := plugin.ListPublishedFingerprints(ctx, "llm-d/router", 7)
	if err != nil {
		t.Fatalf("ListPublishedFingerprints: %v", err)
	}
	if !slices.Equal(fps, []string{"fp_own"}) {
		t.Fatalf("ListPublishedFingerprints = %v, want [fp_own]", fps)
	}

	res, err := plugin.PublishReview(ctx, contract.PublishRequest{
		Repo:     "llm-d/router",
		PRNumber: 7,
		CommitID: "h2",
		RunID:    "run-spoof",
		Summary:  "Static token review.",
		Inlines: []contract.InlineComment{
			{Fingerprint: "fp_spoof_inline", File: "a.go", Line: 1, Body: "real finding a"},
			{Fingerprint: "fp_spoofed", File: "b.go", Line: 2, Body: "real finding b"},
			{Fingerprint: "fp_own_inline", File: "c.go", Line: 3, Body: "already posted"},
		},
	})
	if err != nil {
		t.Fatalf("PublishReview: %v", err)
	}
	if postCalls.Load() != 1 || res.ReviewID != 321 {
		t.Fatalf("expected spoofed run marker ignored and review posted, got calls=%d res=%+v", postCalls.Load(), res)
	}
	body, _ := postedBody.Load().(string)
	if !strings.Contains(body, "fp:fp_spoof_inline") || !strings.Contains(body, "fp:fp_spoofed") || strings.Contains(body, "fp:fp_own_inline") {
		t.Fatalf("unexpected posted review payload: %s", body)
	}
	for _, c := range res.Comments {
		if c.CommentID == 10 {
			t.Fatalf("fallback comment collection returned untrusted author comment: %+v", res.Comments)
		}
	}
	if len(res.Comments) != 1 || res.Comments[0].CommentID != 11 || res.Comments[0].Fingerprint != "fp_own_inline" {
		t.Fatalf("expected only the token owner comment from fallback collection, got %+v", res.Comments)
	}
	if userCalls.Load() != 1 {
		t.Fatalf("expected token owner login resolved once and cached, got %d /user calls", userCalls.Load())
	}
}

func TestStaticTokenMarkerTrustFallbacks(t *testing.T) {
	ctx := context.Background()
	reviews := []ghReviewRecord{
		{ID: 1, Body: "<!-- review-run: r1 sha: h1 fps: fp_user -->", User: ghUser{Login: "someone", Type: "User"}},
		{ID: 2, Body: "<!-- review-run: r2 sha: h1 fps: fp_bot -->", User: ghUser{Login: "github-actions[bot]", Type: "Bot"}},
		{ID: 3, Body: "<!-- review-run: r3 sha: h1 fps: fp_owner -->", User: ghUser{Login: "owner", Type: "User"}},
	}

	newServer := func(userHandler func(w http.ResponseWriter, call int32)) (*httptest.Server, *atomic.Int32) {
		var userCalls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/user":
				userHandler(w, userCalls.Add(1))
			case r.Method == http.MethodGet && r.URL.Path == "/repos/llm-d/router/pulls/5/reviews":
				_ = json.NewEncoder(w).Encode(reviews)
			default:
				http.NotFound(w, r)
			}
		}))
		return srv, &userCalls
	}

	listTwice := func(t *testing.T, plugin *Plugin) ([]string, []string) {
		t.Helper()
		first, err := plugin.ListPublishedFingerprints(ctx, "llm-d/router", 5)
		if err != nil {
			t.Fatalf("first ListPublishedFingerprints: %v", err)
		}
		second, err := plugin.ListPublishedFingerprints(ctx, "llm-d/router", 5)
		if err != nil {
			t.Fatalf("second ListPublishedFingerprints: %v", err)
		}
		return first, second
	}

	t.Run("user endpoint missing keeps legacy trust", func(t *testing.T) {
		srv, userCalls := newServer(func(w http.ResponseWriter, _ int32) {
			http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
		})
		defer srv.Close()
		first, second := listTwice(t, NewPlugin(Config{StaticToken: "ghp_mock", APIBaseURL: srv.URL}))
		want := []string{"fp_bot", "fp_owner", "fp_user"}
		if !slices.Equal(first, want) || !slices.Equal(second, want) {
			t.Fatalf("got %v and %v, want %v", first, second, want)
		}
		if userCalls.Load() != 1 {
			t.Fatalf("expected 404 outcome cached, got %d /user calls", userCalls.Load())
		}
	})

	t.Run("empty login keeps legacy trust", func(t *testing.T) {
		srv, _ := newServer(func(w http.ResponseWriter, _ int32) {
			_, _ = io.WriteString(w, `{}`)
		})
		defer srv.Close()
		first, _ := listTwice(t, NewPlugin(Config{StaticToken: "ghp_mock", APIBaseURL: srv.URL}))
		if want := []string{"fp_bot", "fp_owner", "fp_user"}; !slices.Equal(first, want) {
			t.Fatalf("got %v, want %v", first, want)
		}
	})

	t.Run("integration token forbidden trusts only bots", func(t *testing.T) {
		srv, userCalls := newServer(func(w http.ResponseWriter, _ int32) {
			http.Error(w, `{"message":"Resource not accessible by integration"}`, http.StatusForbidden)
		})
		defer srv.Close()
		first, second := listTwice(t, NewPlugin(Config{StaticToken: "ghs_actions", APIBaseURL: srv.URL}))
		want := []string{"fp_bot"}
		if !slices.Equal(first, want) || !slices.Equal(second, want) {
			t.Fatalf("got %v and %v, want %v", first, second, want)
		}
		if userCalls.Load() != 1 {
			t.Fatalf("expected 403 outcome cached, got %d /user calls", userCalls.Load())
		}
	})

	t.Run("transient failure is not cached", func(t *testing.T) {
		srv, userCalls := newServer(func(w http.ResponseWriter, call int32) {
			if call == 1 {
				http.Error(w, "upstream down", http.StatusBadGateway)
				return
			}
			_, _ = io.WriteString(w, `{"login":"owner"}`)
		})
		defer srv.Close()
		first, second := listTwice(t, NewPlugin(Config{StaticToken: "ghp_owner", APIBaseURL: srv.URL}))
		if !slices.Equal(first, []string{"fp_bot"}) || !slices.Equal(second, []string{"fp_owner"}) {
			t.Fatalf("got %v then %v, want [fp_bot] then [fp_owner]", first, second)
		}
		if userCalls.Load() != 2 {
			t.Fatalf("expected retry after transient failure, got %d /user calls", userCalls.Load())
		}
	})

	t.Run("concurrent resolution is race free", func(t *testing.T) {
		srv, _ := newServer(func(w http.ResponseWriter, _ int32) {
			_, _ = io.WriteString(w, `{"login":"OWNER"}`)
		})
		defer srv.Close()
		plugin := NewPlugin(Config{StaticToken: "ghp_owner", APIBaseURL: srv.URL})
		results := make(chan []string, 8)
		errs := make(chan error, 8)
		for range 8 {
			go func() {
				fps, err := plugin.ListPublishedFingerprints(ctx, "llm-d/router", 5)
				errs <- err
				results <- fps
			}()
		}
		for range 8 {
			if err := <-errs; err != nil {
				t.Fatalf("ListPublishedFingerprints: %v", err)
			}
			if fps := <-results; !slices.Equal(fps, []string{"fp_owner"}) {
				t.Fatalf("got %v, want [fp_owner]", fps)
			}
		}
	})
}

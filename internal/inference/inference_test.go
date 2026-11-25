package inference

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

func testClaims(jti string, maxReqs int32, maxTotal, reserve int64) *reviewv1.TaskClaims {
	parent := "run-root"
	return &reviewv1.TaskClaims{
		RunId:                   "run-1",
		SessionId:               "run-1-s0",
		ParentSessionId:         &parent,
		Repo:                    "llm-d/llm-d-router",
		HarnessProfile:          "openhands",
		MaxRequests:             maxReqs,
		MaxTotalTokens:          maxTotal,
		NormalizerReserveTokens: reserve,
		MaxOutputTokens:         512,
		Exp:                     time.Now().Add(10 * time.Minute).Unix(),
		Jti:                     jti,
	}
}

func TestSessionLedgerCASConcurrencyAndNormalizeCap(t *testing.T) {
	ledger := NewSessionLedger(testClaims("jti-cas", 5, 1000, 200))

	if code := ledger.TryReserve(801, "harness"); code != "budget_exhausted" {
		t.Fatalf("expected harness budget_exhausted above max-reserve, got %q", code)
	}
	if got := ledger.requests.Load(); got != 0 {
		t.Fatalf("expected request counter rolled back to 0 on budget rejection, got %d", got)
	}

	if code := ledger.TryReserve(100, "normalize"); code != "" {
		t.Fatalf("first normalize call failed: %q", code)
	}
	if code := ledger.TryReserve(50, "normalize"); code != "" {
		t.Fatalf("second normalize call failed: %q", code)
	}
	if code := ledger.TryReserve(10, "normalize"); code != "request_limit_exhausted" {
		t.Fatalf("third spoofed normalize call must hit cap, got %q", code)
	}

	concurrent := NewSessionLedger(testClaims("jti-par", 10, 500, 0))
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if concurrent.TryReserve(100, "harness") == "" {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 5 {
		t.Fatalf("expected exactly 5 concurrent reservations within 500 budget, got %d", admitted.Load())
	}
}

func TestSessionLedgerCountsRejectedHarnessRequests(t *testing.T) {
	capped := NewSessionLedger(testClaims("jti-rejected-cap", 2, 1000, 200))
	for i := range 2 {
		if code := capped.TryReserve(100, "harness"); code != "" {
			t.Fatalf("harness request %d within cap rejected: %q", i+1, code)
		}
	}
	if got := capped.Snapshot().GetRejectedRequests(); got != 0 {
		t.Fatalf("expected 0 rejected requests within cap, got %d", got)
	}
	if code := capped.TryReserve(100, "harness"); code != "request_limit_exhausted" {
		t.Fatalf("expected request_limit_exhausted beyond cap, got %q", code)
	}
	if code := capped.TryReserve(100, "harness"); code != "request_limit_exhausted" {
		t.Fatalf("expected request_limit_exhausted beyond cap, got %q", code)
	}
	if got := capped.Snapshot().GetRejectedRequests(); got != 2 {
		t.Fatalf("expected 2 rejected harness requests after request cap, got %d", got)
	}

	overBudget := NewSessionLedger(testClaims("jti-rejected-budget", 5, 1000, 200))
	if code := overBudget.TryReserve(801, "harness"); code != "budget_exhausted" {
		t.Fatalf("expected budget_exhausted above max-reserve, got %q", code)
	}
	if got := overBudget.Snapshot().GetRejectedRequests(); got != 1 {
		t.Fatalf("expected 1 rejected harness request after budget cutoff, got %d", got)
	}
	if code := overBudget.TryReserve(1001, "normalize"); code != "budget_exhausted" {
		t.Fatalf("expected normalize budget_exhausted above max total, got %q", code)
	}
	if got := overBudget.Snapshot().GetRejectedRequests(); got != 1 {
		t.Fatalf("normalize rejections must not count as harness rejections, got %d", got)
	}
}

func TestProxyBundleOneTimeReadReportCapExpiryAndUnregister(t *testing.T) {
	bundlePath := filepath.Join(t.TempDir(), "bundle.tar.gz")
	if err := os.WriteFile(bundlePath, []byte("bundle-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	handler := NewProxyHandler(Config{
		Upstream:       &VllmDirectUpstream{BaseURL: "http://127.0.0.1:1"},
		MaxReportBytes: 64,
		Now:            func() time.Time { return now },
	})
	srv := httptest.NewServer(handler.SandboxMux())
	defer srv.Close()

	claims := testClaims("jti-transfer", 5, 10000, 1000)
	claims.Exp = now.Add(time.Minute).Unix()
	tok, unregister, err := handler.RegisterSession(claims, bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(tok) != 64 {
		t.Fatalf("expected 64-char hex token, got %d", len(tok))
	}

	unauthReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/sandbox/bundle", nil)
	unauthResp, err := http.DefaultClient.Do(unauthReq)
	if err != nil || unauthResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without bearer token, got %v %v", err, unauthResp)
	}
	unauthResp.Body.Close()

	bReq1, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/sandbox/bundle", nil)
	bReq1.Header.Set("Authorization", "Bearer "+tok)
	bResp1, err := http.DefaultClient.Do(bReq1)
	if err != nil || bResp1.StatusCode != http.StatusOK {
		t.Fatalf("first bundle fetch failed: %v %v", err, bResp1)
	}
	gotBundle, _ := io.ReadAll(bResp1.Body)
	bResp1.Body.Close()
	if string(gotBundle) != "bundle-bytes" {
		t.Fatalf("unexpected bundle bytes: %q", gotBundle)
	}

	bReq2, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/sandbox/bundle", nil)
	bReq2.Header.Set("Authorization", "Bearer "+tok)
	bResp2, err := http.DefaultClient.Do(bReq2)
	if err != nil || bResp2.StatusCode != http.StatusGone {
		t.Fatalf("second bundle fetch must return 410 Gone, got %v %v", err, bResp2)
	}
	bResp2.Body.Close()

	bigReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/sandbox/report", strings.NewReader(strings.Repeat("x", 65)))
	bigReq.Header.Set("Authorization", "Bearer "+tok)
	bigResp, err := http.DefaultClient.Do(bigReq)
	if err != nil || bigResp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize report must return 413, got %v %v", err, bigResp)
	}
	bigResp.Body.Close()

	repReq1, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/sandbox/report", strings.NewReader(`{"summary":"first"}`))
	repReq1.Header.Set("Authorization", "Bearer "+tok)
	repResp1, err := http.DefaultClient.Do(repReq1)
	if err != nil || repResp1.StatusCode != http.StatusOK {
		t.Fatalf("first report upload failed: %v %v", err, repResp1)
	}
	repResp1.Body.Close()

	repReq2, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/sandbox/report", strings.NewReader(`{"summary":"latest"}`))
	repReq2.Header.Set("Authorization", "Bearer "+tok)
	repResp2, err := http.DefaultClient.Do(repReq2)
	if err != nil || repResp2.StatusCode != http.StatusOK {
		t.Fatalf("second report upload failed: %v %v", err, repResp2)
	}
	repResp2.Body.Close()

	now = now.Add(2 * time.Minute)
	expReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/models", nil)
	expReq.Header.Set("Authorization", "Bearer "+tok)
	expResp, err := http.DefaultClient.Do(expReq)
	if err != nil || expResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired token must return 401, got %v %v", err, expResp)
	}
	expResp.Body.Close()

	reportBytes, usage := handler.FinishSession(tok)
	if string(reportBytes) != `{"summary":"latest"}` || usage == nil || usage.Jti != "jti-transfer" {
		t.Fatalf("unexpected FinishSession result: report=%q usage=%+v", reportBytes, usage)
	}

	unregister()
	if rep, u := handler.FinishSession(tok); rep != nil || u != nil {
		t.Fatalf("expected nil after unregister, got rep=%q usage=%+v", rep, u)
	}

	staleClaims := testClaims("jti-stale", 5, 10000, 1000)
	staleClaims.Exp = now.Add(-time.Second).Unix()
	staleTok, _, err := handler.RegisterSession(staleClaims, "")
	if err != nil {
		t.Fatal(err)
	}
	freshClaims := testClaims("jti-fresh", 5, 10000, 1000)
	freshClaims.Exp = now.Add(time.Minute).Unix()
	_, freshUnreg, err := handler.RegisterSession(freshClaims, "")
	if err != nil {
		t.Fatal(err)
	}
	defer freshUnreg()
	if handler.lookupSession(staleTok) != nil {
		t.Fatal("expected RegisterSession to opportunistically sweep expired sessions")
	}
}

func TestProxyReportsRejectedHarnessRequestsInUsage(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}`)
	}))
	defer upstream.Close()

	handler := NewProxyHandler(Config{
		Upstream:      &VllmDirectUpstream{BaseURL: upstream.URL},
		UpstreamModel: "m",
	})
	sandboxSrv := httptest.NewServer(handler.SandboxMux())
	defer sandboxSrv.Close()

	claims := testClaims("jti-rejected-usage", 1, 100000, 1000)
	tok, unregister, err := handler.RegisterSession(claims, "")
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()

	wantStatuses := []int{http.StatusOK, http.StatusForbidden, http.StatusForbidden}
	for i, want := range wantStatuses {
		req, _ := http.NewRequest(http.MethodPost, sandboxSrv.URL+"/v1/chat/completions", strings.NewReader(`{"max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("harness request %d: status=%d want=%d body=%s", i+1, resp.StatusCode, want, body)
		}
	}

	_, usage := handler.FinishSession(tok)
	if usage.GetRequests() != 1 || usage.GetRejectedRequests() != 2 {
		t.Fatalf("expected requests=1 rejected_requests=2, got %+v", usage)
	}
}

func TestProxyChatNonStreamAndStreamHidesUnrequestedUsageChunk(t *testing.T) {
	var mu sync.Mutex
	var seenHeaders []http.Header
	// key: parsed upstream request JSON field name; value: decoded JSON value
	var seenBodies []map[string]any

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		// key: parsed upstream request JSON field name; value: decoded JSON value
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		mu.Lock()
		seenHeaders = append(seenHeaders, r.Header.Clone())
		seenBodies = append(seenBodies, body)
		mu.Unlock()

		if r.URL.Path == "/v1/responses" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n")
			fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":40,\"output_tokens\":10,\"input_tokens_details\":{\"cached_tokens\":30}}}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}

		if stream, _ := body["stream"].(bool); stream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("x-llm-d-flow-queue-duration-ms", "17")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
			fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":120,\"completion_tokens\":30,\"prompt_tokens_details\":{\"cached_tokens\":90}}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"cmpl-1","choices":[{"message":{"content":"hello"}}],"usage":{"prompt_tokens":50,"completion_tokens":15,"prompt_tokens_details":{"cached_tokens":40}}}`)
	}))
	defer upstream.Close()

	handler := NewProxyHandler(Config{
		Upstream:      &LlmdGatewayUpstream{BaseURL: upstream.URL, Objective: "code-review-batch"},
		UpstreamModel: "qwen3-coder-30b",
		ModelAlias:    "review-coder",
		BaseRetryWait: time.Millisecond,
		MaxRetryWait:  2 * time.Millisecond,
	})
	sandboxSrv := httptest.NewServer(handler.SandboxMux())
	defer sandboxSrv.Close()

	claims := testClaims("jti-stream-1", 10, 50000, 5000)
	tok, unregister, err := handler.RegisterSession(claims, "")
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()

	modelReq, _ := http.NewRequest(http.MethodGet, sandboxSrv.URL+"/v1/models", nil)
	modelReq.Header.Set("Authorization", "Bearer "+tok)
	modelResp, err := http.DefaultClient.Do(modelReq)
	if err != nil || modelResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/models failed: %v status=%v", err, modelResp)
	}
	modelResp.Body.Close()

	req1, _ := http.NewRequest(http.MethodPost, sandboxSrv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"evil-model","stream":true,"extra_field":"drop-me","messages":[{"role":"user","content":"hi"}]}`))
	req1.Header.Set("Authorization", "Bearer "+tok)
	req1.Header.Set("x-session-id", "spoofed-session")
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	sseBytes, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("stream chat failed status=%d body=%s", resp1.StatusCode, sseBytes)
	}
	if resp1.Header.Get("x-llm-d-flow-queue-duration-ms") != "17" {
		t.Fatalf("expected queue header relayed, got %q", resp1.Header.Get("x-llm-d-flow-queue-duration-ms"))
	}
	if strings.Contains(string(sseBytes), `"prompt_tokens":120`) {
		t.Fatalf("usage-only chunk must be hidden when client omitted stream_options.include_usage, got:\n%s", sseBytes)
	}

	req2, _ := http.NewRequest(http.MethodPost, sandboxSrv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"x","stream":false,"max_tokens":99999,"messages":[{"role":"user","content":"hi"}]}`))
	req2.Header.Set("Authorization", "Bearer "+tok)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("non-stream chat failed status=%d", resp2.StatusCode)
	}

	req3, _ := http.NewRequest(http.MethodPost, sandboxSrv.URL+"/v1/responses", strings.NewReader(`{"model":"x","stream":true,"store":true,"previous_response_id":"resp_old","conversation":"conv_1","input":"hi"}`))
	req3.Header.Set("Authorization", "Bearer "+tok)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp3.Body)
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("responses stream failed status=%d", resp3.StatusCode)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seenHeaders) != 3 {
		t.Fatalf("expected 3 upstream requests, got %d", len(seenHeaders))
	}
	if seenHeaders[0].Get("x-session-id") != "run-1-s0" ||
		seenHeaders[0].Get("x-parent-session-id") != "run-root" ||
		seenHeaders[0].Get("x-llm-d-inference-fairness-id") != "llm-d/llm-d-router" ||
		seenHeaders[0].Get("x-llm-d-inference-objective") != "code-review-batch" {
		t.Fatalf("unexpected llm-d headers: %+v", seenHeaders[0])
	}
	if seenBodies[0]["model"] != "qwen3-coder-30b" || seenBodies[0]["extra_field"] != nil {
		t.Fatalf("chat sanitization failed: %+v", seenBodies[0])
	}
	if maxTok, _ := seenBodies[1]["max_tokens"].(float64); int64(maxTok) != 512 {
		t.Fatalf("expected max_tokens capped at 512, got %v", seenBodies[1]["max_tokens"])
	}
	if seenBodies[2]["store"] != false || seenBodies[2]["previous_response_id"] != nil || seenBodies[2]["conversation"] != nil {
		t.Fatalf("responses sanitization failed: %+v", seenBodies[2])
	}

	_, usage := handler.FinishSession(tok)
	if usage.Requests != 3 || usage.PromptTokens != 210 || usage.CachedTokens != 160 || usage.CompletionTokens != 55 {
		t.Fatalf("unexpected usage summary: %+v", usage)
	}
}

func TestProxyRetries429And503AndEnforces403BudgetCutoff(t *testing.T) {
	var attempts atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if n == 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[],"usage":{"prompt_tokens":300,"completion_tokens":100}}`)
	}))
	defer upstream.Close()

	handler := NewProxyHandler(Config{
		Upstream:      &VllmDirectUpstream{BaseURL: upstream.URL},
		UpstreamModel: "m",
		BaseRetryWait: time.Millisecond,
		MaxRetryWait:  2 * time.Millisecond,
	})
	srv := httptest.NewServer(handler.SandboxMux())
	defer srv.Close()

	claims := testClaims("jti-budget", 10, 900, 200)
	tok, unregister, err := handler.RegisterSession(claims, "")
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()

	req1, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"hi"}]}`))
	req1.Header.Set("Authorization", "Bearer "+tok)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK || attempts.Load() != 3 {
		t.Fatalf("expected 3 upstream attempts with 200 OK, got status=%d attempts=%d", resp1.StatusCode, attempts.Load())
	}

	req2, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"max_tokens":450,"messages":[{"role":"user","content":"hi"}]}`))
	req2.Header.Set("Authorization", "Bearer "+tok)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden || !bytes.Contains(body2, []byte(`"budget_exhausted"`)) {
		t.Fatalf("expected 403 budget_exhausted, got status=%d body=%s", resp2.StatusCode, body2)
	}

	normBody := `{"max_tokens":450,"messages":[{"role":"user","content":"norm"}]}`
	normReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(normBody))
	normReq.Header.Set("Authorization", "Bearer "+tok)
	normReq.Header.Set("x-review-phase", "normalize")
	normResp, err := http.DefaultClient.Do(normReq)
	if err != nil {
		t.Fatal(err)
	}
	normResp.Body.Close()
	if normResp.StatusCode != http.StatusOK {
		t.Fatalf("expected normalize reserve accessible for normalizer call, got %d", normResp.StatusCode)
	}

	normClaims := testClaims("jti-norm-cap", 50, 50000, 1000)
	normTok, normUnreg, err := handler.RegisterSession(normClaims, "")
	if err != nil {
		t.Fatal(err)
	}
	defer normUnreg()
	for i := range 3 {
		r, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`))
		r.Header.Set("Authorization", "Bearer "+normTok)
		r.Header.Set("x-review-phase", "normalize")
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if i < 2 && res.StatusCode != http.StatusOK {
			t.Fatalf("expected normalize call %d to pass, got %d %s", i+1, res.StatusCode, b)
		}
		if i == 2 && (res.StatusCode != http.StatusForbidden || !bytes.Contains(b, []byte(`"request_limit_exhausted"`))) {
			t.Fatalf("expected 3rd spoofed normalize request blocked with request_limit_exhausted, got %d %s", res.StatusCode, b)
		}
	}
}

func TestProxySSEIncludeUsageCutStreamAndSweep(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if bytes.Contains(raw, []byte("cut_me")) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					conn.Close()
				}
			}
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":60,\"completion_tokens\":20}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()

	handler := NewProxyHandler(Config{
		Upstream:      &VllmDirectUpstream{BaseURL: upstream.URL},
		UpstreamModel: "m",
	})
	sandboxSrv := httptest.NewServer(handler.SandboxMux())
	defer sandboxSrv.Close()

	claims := testClaims("jti-cut", 10, 50000, 1000)
	tok, _, err := handler.RegisterSession(claims, "")
	if err != nil {
		t.Fatal(err)
	}

	reqKeep, _ := http.NewRequest(http.MethodPost, sandboxSrv.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`))
	reqKeep.Header.Set("Authorization", "Bearer "+tok)
	respKeep, err := http.DefaultClient.Do(reqKeep)
	if err != nil {
		t.Fatal(err)
	}
	keepBody, _ := io.ReadAll(respKeep.Body)
	respKeep.Body.Close()
	if !strings.Contains(string(keepBody), `"prompt_tokens":60`) {
		t.Fatalf("expected usage chunk preserved when client requested include_usage, got:\n%s", keepBody)
	}

	reqCut, _ := http.NewRequest(http.MethodPost, sandboxSrv.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true,"max_tokens":300,"messages":[{"role":"user","content":"cut_me"}]}`))
	reqCut.Header.Set("Authorization", "Bearer "+tok)
	respCut, err := http.DefaultClient.Do(reqCut)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(respCut.Body)
	respCut.Body.Close()

	sess := handler.lookupSession(tok)
	if sess == nil {
		t.Fatal("expected active session")
	}
	if sess.ledger.committed.Load() <= 80 {
		t.Fatalf("expected cut stream to retain conservative estimate charge above 80, got %d", sess.ledger.committed.Load())
	}

	if swept := handler.SweepExpired(claims.Exp + 1); swept != 1 {
		t.Fatalf("expected 1 swept session, got %d", swept)
	}
}

func TestSessionLedgerCachedTokensExcludedFromBudgetAndNonNegative(t *testing.T) {
	ledger := NewSessionLedger(testClaims("jti-cached", 10, 1000, 200))

	if code := ledger.TryReserve(300, "harness"); code != "" {
		t.Fatalf("first reserve failed: %q", code)
	}
	ledger.Settle(300, &Usage{Prompt: 1000, Cached: 900, Completion: 50})
	if got := ledger.committed.Load(); got != 150 {
		t.Fatalf("expected uncached charge 150 ((1000-900)+50), got %d", got)
	}

	if code := ledger.TryReserve(100, "harness"); code != "" {
		t.Fatalf("second reserve failed: %q", code)
	}
	ledger.Settle(100, &Usage{Prompt: 100, Cached: 250, Completion: 20})
	if got := ledger.committed.Load(); got != 170 {
		t.Fatalf("expected clamped uncached charge 20 (total 170), got %d", got)
	}

	if code := ledger.TryReserve(50, "harness"); code != "" {
		t.Fatalf("third reserve failed: %q", code)
	}
	ledger.Settle(50, &Usage{Prompt: -10, Cached: -5, Completion: -3})
	if got := ledger.committed.Load(); got != 170 {
		t.Fatalf("expected negative usage clamped to 0 charge (total 170), got %d", got)
	}

	snap := ledger.Snapshot()
	if snap.PromptTokens != 1090 || snap.CachedTokens != 1145 || snap.CompletionTokens != 67 {
		t.Fatalf("expected raw usage counters preserved in snapshot, got %+v", snap)
	}
}

func TestProxyUncachedBudgetForChatAndResponses(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		isStream := bytes.Contains(raw, []byte(`"stream":true`))
		if r.URL.Path == "/v1/responses" {
			if isStream {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n")
				fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":500,\"output_tokens\":50,\"input_tokens_details\":{\"cached_tokens\":450}}}}\n\n")
				fmt.Fprint(w, "data: [DONE]\n\n")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"id":"resp-1","usage":{"input_tokens":500,"output_tokens":50,"input_tokens_details":{"cached_tokens":450}}}`)
			return
		}
		if isStream {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
			fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":500,\"completion_tokens\":50,\"prompt_tokens_details\":{\"cached_tokens\":450}}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"cmpl-1","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":500,"completion_tokens":50,"prompt_tokens_details":{"cached_tokens":450}}}`)
	}))
	defer upstream.Close()

	handler := NewProxyHandler(Config{
		Upstream:      &VllmDirectUpstream{BaseURL: upstream.URL},
		UpstreamModel: "m",
	})
	sandboxSrv := httptest.NewServer(handler.SandboxMux())
	defer sandboxSrv.Close()

	claims := testClaims("jti-mixed-cache", 20, 1000, 200)
	claims.MaxOutputTokens = 100
	tok, unregister, err := handler.RegisterSession(claims, "")
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()

	calls := []struct {
		path   string
		stream bool
	}{
		{"/v1/chat/completions", false},
		{"/v1/chat/completions", true},
		{"/v1/responses", false},
		{"/v1/responses", true},
		{"/v1/chat/completions", false},
		{"/v1/responses", true},
	}
	for i, c := range calls {
		payload := fmt.Sprintf(`{"stream":%t,"max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`, c.stream)
		if c.path == "/v1/responses" {
			payload = fmt.Sprintf(`{"stream":%t,"max_output_tokens":100,"input":"hi"}`, c.stream)
		}
		req, _ := http.NewRequest(http.MethodPost, sandboxSrv.URL+c.path, strings.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d (%s stream=%v) expected 200 with cached usage excluded from budget, got %d: %s", i+1, c.path, c.stream, resp.StatusCode, body)
		}
	}

	for i := range 2 {
		req, _ := http.NewRequest(http.MethodPost, sandboxSrv.URL+"/v1/chat/completions", strings.NewReader(`{"max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if i == 0 && resp.StatusCode != http.StatusOK {
			t.Fatalf("7th request expected 200 at 600+130<=800, got %d: %s", resp.StatusCode, body)
		}
		if i == 1 && (resp.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte(`"budget_exhausted"`))) {
			t.Fatalf("8th request expected 403 budget_exhausted at 700+130>800, got %d: %s", resp.StatusCode, body)
		}
	}

	_, usagePB := handler.FinishSession(tok)
	if usagePB.PromptTokens != 3500 || usagePB.CachedTokens != 3150 || usagePB.CompletionTokens != 350 {
		t.Fatalf("expected raw prompt/cached/completion totals (3500/3150/350), got %+v", usagePB)
	}
}

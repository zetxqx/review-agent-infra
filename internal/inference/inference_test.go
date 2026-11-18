package inference

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/token"
	"google.golang.org/protobuf/encoding/protojson"
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

func TestProxyChatNonStreamAndStreamHidesUnrequestedUsageChunk(t *testing.T) {
	key := []byte("proxy-secret")
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
		TokenKey:      key,
		Upstream:      &LlmdGatewayUpstream{BaseURL: upstream.URL, Objective: "code-review-batch"},
		UpstreamModel: "qwen3-coder-30b",
		ModelAlias:    "review-coder",
		BaseRetryWait: time.Millisecond,
		MaxRetryWait:  2 * time.Millisecond,
	})
	sandboxSrv := httptest.NewServer(handler.SandboxMux())
	defer sandboxSrv.Close()
	internalSrv := httptest.NewServer(handler.InternalMux())
	defer internalSrv.Close()

	claims := testClaims("jti-stream-1", 10, 50000, 5000)
	tok, err := token.Mint(key, claims)
	if err != nil {
		t.Fatal(err)
	}

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

	uResp, err := http.Get(internalSrv.URL + "/internal/usage/" + claims.Jti)
	if err != nil || uResp.StatusCode != http.StatusOK {
		t.Fatalf("usage endpoint failed: %v %v", err, uResp)
	}
	uBytes, _ := io.ReadAll(uResp.Body)
	uResp.Body.Close()
	var usage reviewv1.SandboxUsage
	if err := protojson.Unmarshal(uBytes, &usage); err != nil {
		t.Fatalf("decode SandboxUsage failed: %v", err)
	}
	if usage.Requests != 3 || usage.PromptTokens != 210 || usage.CachedTokens != 160 || usage.CompletionTokens != 55 {
		t.Fatalf("unexpected usage summary: %+v", &usage)
	}
}

func TestProxyRetries429And503AndEnforces403BudgetCutoff(t *testing.T) {
	key := []byte("retry-secret")
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
		TokenKey:      key,
		Upstream:      &VllmDirectUpstream{BaseURL: upstream.URL},
		UpstreamModel: "m",
		BaseRetryWait: time.Millisecond,
		MaxRetryWait:  2 * time.Millisecond,
	})
	srv := httptest.NewServer(handler.SandboxMux())
	defer srv.Close()

	claims := testClaims("jti-budget", 10, 900, 200)
	tok, err := token.Mint(key, claims)
	if err != nil {
		t.Fatal(err)
	}

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
	normTok, _ := token.Mint(key, normClaims)
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

func TestProxySSEIncludeUsageCutStreamSweepAndMetrics(t *testing.T) {
	key := []byte("stream-cut-key")
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
		TokenKey:      key,
		Upstream:      &VllmDirectUpstream{BaseURL: upstream.URL},
		UpstreamModel: "m",
	})
	sandboxSrv := httptest.NewServer(handler.SandboxMux())
	defer sandboxSrv.Close()
	internalSrv := httptest.NewServer(handler.InternalMux())
	defer internalSrv.Close()

	claims := testClaims("jti-cut", 10, 50000, 1000)
	tok, err := token.Mint(key, claims)
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

	ledger, ok := handler.Ledgers().Get(claims.Jti)
	if !ok {
		t.Fatal("expected ledger entry")
	}
	if ledger.committed.Load() <= 80 {
		t.Fatalf("expected cut stream to retain conservative estimate charge above 80, got %d", ledger.committed.Load())
	}

	mResp, err := http.Get(internalSrv.URL + "/metrics")
	if err != nil || mResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics failed: %v %v", err, mResp)
	}
	metricsBody, _ := io.ReadAll(mResp.Body)
	mResp.Body.Close()
	if !strings.Contains(string(metricsBody), "review_adapter_requests_total 2") {
		t.Fatalf("unexpected /metrics output:\n%s", metricsBody)
	}

	if swept := handler.Ledgers().SweepExpired(claims.Exp + 1); swept != 1 {
		t.Fatalf("expected 1 swept ledger, got %d", swept)
	}
}

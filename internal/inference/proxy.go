package inference

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/telemetry"
	"github.com/zetxqx/review-agent-infra/internal/token"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultMaxBodyBytes    int64 = 16 << 20
	defaultMaxOutputTokens int64 = 4096
	maxRetries                   = 4
	maxSSELineBytes              = 4 << 20
)

// key: allowlisted Chat Completions JSON field name; value: true when permitted
var chatAllowedFields = map[string]bool{
	"messages":              true,
	"tools":                 true,
	"tool_choice":           true,
	"parallel_tool_calls":   true,
	"response_format":       true,
	"temperature":           true,
	"top_p":                 true,
	"stop":                  true,
	"seed":                  true,
	"max_tokens":            true,
	"max_completion_tokens": true,
	"stream":                true,
	"stream_options":        true,
}

// key: allowlisted Responses API JSON field name; value: true when permitted
var responsesAllowedFields = map[string]bool{
	"input":               true,
	"instructions":        true,
	"tools":               true,
	"tool_choice":         true,
	"parallel_tool_calls": true,
	"text":                true,
	"reasoning":           true,
	"temperature":         true,
	"top_p":               true,
	"max_output_tokens":   true,
	"stream":              true,
}

type Config struct {
	TokenKey      []byte
	Upstream      contract.UpstreamProvider
	UpstreamModel string
	ModelAlias    string
	MaxBodyBytes  int64
	HTTPClient    *http.Client
	BaseRetryWait time.Duration
	MaxRetryWait  time.Duration
	Telemetry     *telemetry.Registry
}

type StreamPlan struct {
	API              string
	Stream           bool
	ClientWantsUsage bool
	Estimate         int64
}

type ProxyHandler struct {
	tokenKey      []byte
	upstream      contract.UpstreamProvider
	upstreamModel string
	modelAlias    string
	maxBodyBytes  int64
	httpClient    *http.Client
	baseRetryWait time.Duration
	maxRetryWait  time.Duration
	ledgers       *LedgerStore
	telemetry     *telemetry.Registry

	totalRequests    atomic.Int64
	budgetRejections atomic.Int64
	promptTokens     atomic.Int64
	cachedTokens     atomic.Int64
	completionTokens atomic.Int64
}

func NewProxyHandler(cfg Config) *ProxyHandler {
	maxBody := cfg.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxBodyBytes
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 420 * time.Second}
	}
	baseWait := cfg.BaseRetryWait
	if baseWait <= 0 {
		baseWait = 5 * time.Second
	}
	maxWait := cfg.MaxRetryWait
	if maxWait <= 0 {
		maxWait = 40 * time.Second
	}
	alias := cfg.ModelAlias
	if alias == "" {
		alias = "review-coder"
	}
	return &ProxyHandler{
		tokenKey:      cfg.TokenKey,
		upstream:      cfg.Upstream,
		upstreamModel: cfg.UpstreamModel,
		modelAlias:    alias,
		maxBodyBytes:  maxBody,
		httpClient:    client,
		baseRetryWait: baseWait,
		maxRetryWait:  maxWait,
		ledgers:       NewLedgerStore(),
		telemetry:     cfg.Telemetry,
	}
}

func (p *ProxyHandler) SandboxMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		p.handleProxy(w, r, "chat")
	})
	mux.HandleFunc("POST /v1/responses", func(w http.ResponseWriter, r *http.Request) {
		p.handleProxy(w, r, "responses")
	})
	mux.HandleFunc("GET /v1/models", p.handleModels)
	return mux
}

func (p *ProxyHandler) InternalMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/usage/{jti}", p.handleUsage)
	mux.HandleFunc("GET /metrics", p.handleMetrics)
	return mux
}

func (p *ProxyHandler) Ledgers() *LedgerStore {
	return p.ledgers
}

func (p *ProxyHandler) handleModels(w http.ResponseWriter, r *http.Request) {
	if _, err := token.Verify(p.tokenKey, r.Header.Get("Authorization")); err != nil {
		writeOpenAIError(w, http.StatusUnauthorized, "unauthorized", "invalid task token")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Object string `json:"object"`
		Data   []struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			OwnedBy string `json:"owned_by"`
		} `json:"data"`
	}{
		Object: "list",
		Data: []struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			OwnedBy string `json:"owned_by"`
		}{
			{ID: p.modelAlias, Object: "model", OwnedBy: "review-agent-infra"},
		},
	})
}

func (p *ProxyHandler) handleUsage(w http.ResponseWriter, r *http.Request) {
	jti := r.PathValue("jti")
	if jti == "" {
		http.NotFound(w, r)
		return
	}
	l, ok := p.ledgers.Get(jti)
	if !ok {
		http.NotFound(w, r)
		return
	}
	raw, err := (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(l.Snapshot())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func (p *ProxyHandler) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(
		w,
		"# TYPE review_adapter_requests_total counter\nreview_adapter_requests_total %d\n"+
			"# TYPE review_adapter_budget_rejections_total counter\nreview_adapter_budget_rejections_total %d\n"+
			"# TYPE review_adapter_tokens_total counter\n"+
			"review_adapter_tokens_total{kind=\"prompt\"} %d\n"+
			"review_adapter_tokens_total{kind=\"cached\"} %d\n"+
			"review_adapter_tokens_total{kind=\"completion\"} %d\n",
		p.totalRequests.Load(),
		p.budgetRejections.Load(),
		p.promptTokens.Load(),
		p.cachedTokens.Load(),
		p.completionTokens.Load(),
	)
	if p.telemetry != nil {
		_, _ = w.Write([]byte(p.telemetry.Render()))
	}
}

func (p *ProxyHandler) handleProxy(w http.ResponseWriter, r *http.Request, api string) {
	claims, err := token.Verify(p.tokenKey, r.Header.Get("Authorization"))
	if err != nil {
		writeOpenAIError(w, http.StatusUnauthorized, "unauthorized", "invalid task token")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, p.maxBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeOpenAIError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds cap")
		return
	}
	payload, plan, err := p.sanitizePayload(api, raw, claims)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	p.totalRequests.Add(1)
	ledger := p.ledgers.GetOrCreate(claims)
	phase := "harness"
	if r.Header.Get("x-review-phase") == "normalize" {
		phase = "normalize"
	}
	if denied := ledger.TryReserve(plan.Estimate, phase); denied != "" {
		p.budgetRejections.Add(1)
		writeOpenAIError(w, http.StatusForbidden, denied, "task token budget or request cap exceeded")
		return
	}

	upReqSpec, err := p.upstream.Build(claims, api, payload)
	if err != nil {
		zero := Usage{}
		ledger.Settle(plan.Estimate, &zero)
		writeOpenAIError(w, http.StatusInternalServerError, "upstream_build_failed", err.Error())
		return
	}

	resp, err := p.sendWithRetry(r.Context(), upReqSpec)
	if err != nil {
		zero := Usage{}
		ledger.Settle(plan.Estimate, &zero)
		writeOpenAIError(w, http.StatusServiceUnavailable, "upstream_unavailable", "inference upstream unavailable")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		zero := Usage{}
		ledger.Settle(plan.Estimate, &zero)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(errBody)
		return
	}

	copySelectedHeaders(w.Header(), resp.Header, "Content-Type", "x-llm-d-flow-queue-duration-ms")
	if !plan.Stream {
		fullBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			ledger.Settle(plan.Estimate, nil)
			return
		}
		u := parseUsageFromBody(api, fullBody)
		ledger.Settle(plan.Estimate, u)
		p.recordTokenMetrics(claims, u)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fullBody)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	u := relaySSE(w, resp.Body, plan, ledger)
	p.recordTokenMetrics(claims, u)
}

func (p *ProxyHandler) recordTokenMetrics(claims *reviewv1.TaskClaims, u *Usage) {
	if u == nil {
		return
	}
	p.promptTokens.Add(u.Prompt)
	p.cachedTokens.Add(u.Cached)
	p.completionTokens.Add(u.Completion)
	if p.telemetry != nil && claims != nil {
		p.telemetry.AddTokens(claims.Repo, claims.HarnessProfile, u.Prompt, u.Completion, u.Cached)
	}
}

// Return map key: allowlisted OpenAI request JSON field name; value: sanitized JSON value.
func (p *ProxyHandler) sanitizePayload(api string, raw []byte, claims *reviewv1.TaskClaims) (map[string]any, StreamPlan, error) {
	// key: raw client JSON field name; value: decoded JSON value
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, StreamPlan{}, err
	}
	if body == nil {
		return nil, StreamPlan{}, errors.New("request body must be a JSON object")
	}

	allowed := responsesAllowedFields
	if api == "chat" {
		allowed = chatAllowedFields
	}
	// key: allowlisted JSON field name; value: sanitized JSON value
	out := make(map[string]any, len(allowed)+2)
	for k, v := range body {
		if allowed[k] {
			out[k] = v
		}
	}
	if p.upstreamModel != "" {
		out["model"] = p.upstreamModel
	} else {
		out["model"] = p.modelAlias
	}

	stream, _ := out["stream"].(bool)
	clientWantsUsage := true
	maxOut := int64(claims.MaxOutputTokens)
	if maxOut <= 0 {
		maxOut = defaultMaxOutputTokens
	}

	if api == "chat" {
		tokenKey := "max_tokens"
		if _, has := out["max_completion_tokens"]; has {
			tokenKey = "max_completion_tokens"
		}
		if reqOut, ok := asInt64(out[tokenKey]); ok && reqOut > 0 && reqOut < maxOut {
			maxOut = reqOut
		}
		out[tokenKey] = maxOut
		if stream {
			// key: stream_options field name; value: decoded client option value
			if so, ok := out["stream_options"].(map[string]any); ok {
				clientWantsUsage, _ = so["include_usage"].(bool)
			} else {
				clientWantsUsage = false
			}
			// key: stream_options field name; value: boolean forcing upstream tail usage chunk
			out["stream_options"] = map[string]any{"include_usage": true}
		} else {
			delete(out, "stream_options")
		}
	} else {
		out["store"] = false
		if reqOut, ok := asInt64(out["max_output_tokens"]); ok && reqOut > 0 && reqOut < maxOut {
			maxOut = reqOut
		}
		out["max_output_tokens"] = maxOut
	}

	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, StreamPlan{}, err
	}
	return out, StreamPlan{
		API:              api,
		Stream:           stream,
		ClientWantsUsage: clientWantsUsage,
		Estimate:         int64(len(encoded))/3 + maxOut,
	}, nil
}

func (p *ProxyHandler) sendWithRetry(ctx context.Context, spec contract.UpstreamRequest) (*http.Response, error) {
	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, spec.URL, bytes.NewReader(spec.Body))
		if err != nil {
			return nil, err
		}
		req.Header = spec.Headers.Clone()

		resp, err := p.httpClient.Do(req)
		if err == nil && resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable {
			return resp, nil
		}
		var retryAfter string
		if resp != nil {
			retryAfter = resp.Header.Get("Retry-After")
			resp.Body.Close()
		}
		if attempt == maxRetries {
			break
		}
		delay := p.backoffDelay(attempt, retryAfter)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, errors.New("upstream exhausted retries")
}

func (p *ProxyHandler) backoffDelay(attempt int, retryAfter string) time.Duration {
	if secs, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	base := min(p.maxRetryWait, p.baseRetryWait<<attempt)
	return time.Duration(float64(base) * (0.5 + 0.5*rand.Float64()))
}

func relaySSE(w http.ResponseWriter, body io.Reader, plan StreamPlan, ledger *SessionLedger) *Usage {
	flusher, canFlush := w.(http.Flusher)
	var seenUsage *Usage
	defer func() {
		ledger.Settle(plan.Estimate, seenUsage)
	}()

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSSELineBytes)
	skipNextBlank := false
	for scanner.Scan() {
		line := scanner.Text()
		if skipNextBlank && line == "" {
			skipNextBlank = false
			continue
		}
		skipNextBlank = false
		if strings.HasPrefix(line, "data: ") && line != "data: [DONE]" {
			u, usageOnly := sniffSSEUsage(plan.API, line[6:])
			if u != nil {
				seenUsage = u
			}
			if usageOnly && !plan.ClientWantsUsage {
				skipNextBlank = true
				continue
			}
		}
		if _, err := io.WriteString(w, line+"\n"); err != nil {
			return seenUsage
		}
		if canFlush && line == "" {
			flusher.Flush()
		}
	}
	return seenUsage
}

type chatUsageJSON struct {
	PromptTokens        int64 `json:"prompt_tokens"`
	CompletionTokens    int64 `json:"completion_tokens"`
	PromptTokensDetails struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
}

type responsesUsageJSON struct {
	InputTokens        int64 `json:"input_tokens"`
	OutputTokens       int64 `json:"output_tokens"`
	InputTokensDetails struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"input_tokens_details"`
}

func parseUsageFromBody(api string, body []byte) *Usage {
	if api == "responses" {
		var payload struct {
			Usage    *responsesUsageJSON `json:"usage"`
			Response *struct {
				Usage *responsesUsageJSON `json:"usage"`
			} `json:"response"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil
		}
		u := payload.Usage
		if u == nil && payload.Response != nil {
			u = payload.Response.Usage
		}
		if u == nil {
			return nil
		}
		return &Usage{
			Prompt:     u.InputTokens,
			Cached:     u.InputTokensDetails.CachedTokens,
			Completion: u.OutputTokens,
		}
	}

	var payload struct {
		Usage *chatUsageJSON `json:"usage"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Usage == nil {
		return nil
	}
	return &Usage{
		Prompt:     payload.Usage.PromptTokens,
		Cached:     payload.Usage.PromptTokensDetails.CachedTokens,
		Completion: payload.Usage.CompletionTokens,
	}
}

func sniffSSEUsage(api, data string) (*Usage, bool) {
	if api == "responses" {
		var event struct {
			Type     string `json:"type"`
			Response *struct {
				Usage *responsesUsageJSON `json:"usage"`
			} `json:"response"`
		}
		if err := json.Unmarshal([]byte(data), &event); err != nil || event.Type != "response.completed" {
			return nil, false
		}
		if event.Response == nil || event.Response.Usage == nil {
			return nil, false
		}
		u := event.Response.Usage
		return &Usage{
			Prompt:     u.InputTokens,
			Cached:     u.InputTokensDetails.CachedTokens,
			Completion: u.OutputTokens,
		}, false
	}

	var event struct {
		Choices []json.RawMessage `json:"choices"`
		Usage   *chatUsageJSON    `json:"usage"`
	}
	if err := json.Unmarshal([]byte(data), &event); err != nil || event.Usage == nil {
		return nil, false
	}
	return &Usage{
		Prompt:     event.Usage.PromptTokens,
		Cached:     event.Usage.PromptTokensDetails.CachedTokens,
		Completion: event.Usage.CompletionTokens,
	}, len(event.Choices) == 0
}

func asInt64(v any) (int64, bool) {
	n, ok := v.(float64)
	if !ok {
		return 0, false
	}
	return int64(n), true
}

func copySelectedHeaders(dst, src http.Header, names ...string) {
	for _, name := range names {
		if val := src.Get(name); val != "" {
			dst.Set(name, val)
		}
	}
}

func writeOpenAIError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}{
		Error: struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		}{
			Message: msg,
			Type:    "invalid_request_error",
			Code:    code,
		},
	})
}

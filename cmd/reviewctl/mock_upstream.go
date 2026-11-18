package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/publisher"
)

type mockUpstreamRequestLog struct {
	SessionID   string
	FairnessID  string
	Objective   string
	Stream      bool
	IncludeUse  bool
	JSONSchema  bool
	PromptText  string
}

type diffAwareMockUpstream struct {
	mu sync.Mutex
	// key: x-session-id header value; value: number of upstream calls observed in that session
	sessionTurns map[string]int
	requests     []mockUpstreamRequestLog
}

func startMockUpstreamServer() (*httptest.Server, *diffAwareMockUpstream) {
	m := &diffAwareMockUpstream{
		// key: x-session-id header value; value: number of upstream calls observed in that session
		sessionTurns: make(map[string]int),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", m.handleChatCompletions)
	return httptest.NewServer(mux), m
}

func (m *diffAwareMockUpstream) Requests() []mockUpstreamRequestLog {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.requests)
}

func (m *diffAwareMockUpstream) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// key: OpenAI request JSON field name; value: decoded JSON value
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	stream, _ := payload["stream"].(bool)
	includeUsage := false
	// key: stream_options field name; value: option value
	if so, ok := payload["stream_options"].(map[string]any); ok {
		includeUsage, _ = so["include_usage"].(bool)
	}
	isSchema := false
	// key: response_format field name; value: format config value
	if rf, ok := payload["response_format"].(map[string]any); ok {
		isSchema = rf["type"] == "json_schema"
	}

	promptText := extractMessagesText(payload["messages"])
	sessionID := r.Header.Get("x-session-id")

	m.mu.Lock()
	m.sessionTurns[sessionID]++
	turn := m.sessionTurns[sessionID]
	m.requests = append(m.requests, mockUpstreamRequestLog{
		SessionID:  sessionID,
		FairnessID: r.Header.Get("x-llm-d-inference-fairness-id"),
		Objective:  r.Header.Get("x-llm-d-inference-objective"),
		Stream:     stream,
		IncludeUse: includeUsage,
		JSONSchema: isSchema,
		PromptText: promptText,
	})
	m.mu.Unlock()

	promptToks := int64(640 + turn*120)
	cachedToks := int64(0)
	if turn > 1 {
		cachedToks = int64(520 + (turn-2)*80)
	}
	compToks := int64(110)

	targetFile, targetLine, targetText := pickAnchorFromPrompt(promptText)
	content := buildMockAssistantContent(isSchema, promptText, targetFile, targetLine, targetText)

	if !stream {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			Choices []struct {
				Index   int `json:"index"`
				Message struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				} `json:"message"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage struct {
				PromptTokens        int64 `json:"prompt_tokens"`
				CompletionTokens    int64 `json:"completion_tokens"`
				TotalTokens         int64 `json:"total_tokens"`
				PromptTokensDetails struct {
					CachedTokens int64 `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
			} `json:"usage"`
		}{
			ID:     "chatcmpl-mock",
			Object: "chat.completion",
			Choices: []struct {
				Index   int `json:"index"`
				Message struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				} `json:"message"`
				FinishReason string `json:"finish_reason"`
			}{
				{
					Index: 0,
					Message: struct {
						Role    string `json:"role"`
						Content string `json:"content"`
					}{Role: "assistant", Content: content},
					FinishReason: "stop",
				},
			},
			Usage: struct {
				PromptTokens        int64 `json:"prompt_tokens"`
				CompletionTokens    int64 `json:"completion_tokens"`
				TotalTokens         int64 `json:"total_tokens"`
				PromptTokensDetails struct {
					CachedTokens int64 `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
			}{
				PromptTokens:     promptToks,
				CompletionTokens: compToks,
				TotalTokens:      promptToks + compToks,
				PromptTokensDetails: struct {
					CachedTokens int64 `json:"cached_tokens"`
				}{CachedTokens: cachedToks},
			},
		})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, _ := w.(http.Flusher)

	mid := len(content) / 2
	for _, piece := range []string{content[:mid], content[mid:]} {
		chunkBytes, _ := json.Marshal(struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			Choices []struct {
				Index int `json:"index"`
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}{
			ID:     "chatcmpl-stream",
			Object: "chat.completion.chunk",
			Choices: []struct {
				Index int `json:"index"`
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			}{
				{
					Index: 0,
					Delta: struct {
						Content string `json:"content"`
					}{Content: piece},
				},
			},
		})
		fmt.Fprintf(w, "data: %s\n\n", chunkBytes)
		if flusher != nil {
			flusher.Flush()
		}
	}

	if includeUsage {
		usageChunk, _ := json.Marshal(struct {
			ID      string            `json:"id"`
			Object  string            `json:"object"`
			Choices []json.RawMessage `json:"choices"`
			Usage   struct {
				PromptTokens        int64 `json:"prompt_tokens"`
				CompletionTokens    int64 `json:"completion_tokens"`
				TotalTokens         int64 `json:"total_tokens"`
				PromptTokensDetails struct {
					CachedTokens int64 `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
			} `json:"usage"`
		}{
			ID:      "chatcmpl-stream",
			Object:  "chat.completion.chunk",
			Choices: []json.RawMessage{},
			Usage: struct {
				PromptTokens        int64 `json:"prompt_tokens"`
				CompletionTokens    int64 `json:"completion_tokens"`
				TotalTokens         int64 `json:"total_tokens"`
				PromptTokensDetails struct {
					CachedTokens int64 `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
			}{
				PromptTokens:     promptToks,
				CompletionTokens: compToks,
				TotalTokens:      promptToks + compToks,
				PromptTokensDetails: struct {
					CachedTokens int64 `json:"cached_tokens"`
				}{CachedTokens: cachedToks},
			},
		})
		fmt.Fprintf(w, "data: %s\n\n", usageChunk)
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
}

func extractMessagesText(rawMessages any) string {
	arr, ok := rawMessages.([]any)
	if !ok {
		return ""
	}
	var b strings.Builder
	for _, item := range arr {
		// key: chat message field name; value: message role or content
		if m, isMap := item.(map[string]any); isMap {
			if c, isStr := m["content"].(string); isStr {
				b.WriteString(c)
				b.WriteByte('\n')
			}
		}
	}
	return b.String()
}

func pickAnchorFromPrompt(promptText string) (string, int32, string) {
	const marker = "MOCK_ANCHOR|"
	if idx := strings.Index(promptText, marker); idx >= 0 {
		lineEnd := strings.IndexByte(promptText[idx:], '\n')
		rawLine := promptText[idx:]
		if lineEnd >= 0 {
			rawLine = rawLine[:lineEnd]
		}
		parts := strings.SplitN(strings.TrimPrefix(rawLine, marker), "|", 3)
		if len(parts) == 3 {
			var parsedLine int32
			_, _ = fmt.Sscanf(parts[1], "%d", &parsedLine)
			return parts[0], parsedLine, parts[2]
		}
	}

	diffIdx := strings.LastIndex(promptText, "\n## Diff\n")
	searchArea := promptText
	if diffIdx >= 0 {
		searchArea = promptText[diffIdx:]
	}
	if gitIdx := strings.Index(searchArea, "diff --git "); gitIdx >= 0 {
		parsed := publisher.ParseUnifiedDiff([]byte(searchArea[gitIdx:]))
		files := parsed.Files()
		slices.SortStableFunc(files, func(a, b string) int {
			return cmp.Compare(fileReviewPriority(a), fileReviewPriority(b))
		})
		for _, f := range files {
			fd := parsed.File(f)
			if fd == nil {
				continue
			}
			rightLines := parsed.Lines(f, reviewv1.DiffSide_DIFF_SIDE_RIGHT)
			for _, h := range fd.Hunks {
				for _, hl := range h.Lines {
					trimmed := strings.TrimSpace(hl.Text)
					if hl.Prefix == '+' && isCandidateCodeLine(trimmed) && countTrimmedMatches(rightLines, trimmed) == 1 {
						return f, hl.NewLine, trimmed
					}
				}
			}
			for _, h := range fd.Hunks {
				for _, hl := range h.Lines {
					trimmed := strings.TrimSpace(hl.Text)
					if hl.Prefix == '+' && isCandidateCodeLine(trimmed) {
						return f, hl.NewLine, trimmed
					}
				}
			}
			var lineNos []int32
			for ln := range rightLines {
				lineNos = append(lineNos, ln)
			}
			slices.SortFunc(lineNos, cmp.Compare[int32])
			for _, ln := range lineNos {
				trimmed := strings.TrimSpace(rightLines[ln])
				if trimmed != "" {
					return f, ln, trimmed
				}
			}
		}
	}
	return "unknown.go", 1, "return nil"
}

func isCandidateCodeLine(trimmed string) bool {
	return len(trimmed) >= 6 &&
		len([]rune(trimmed)) <= 450 &&
		!strings.HasPrefix(trimmed, "//") &&
		!strings.HasPrefix(trimmed, "package ") &&
		!strings.HasPrefix(trimmed, "import")
}

func fileReviewPriority(f string) int {
	if strings.HasSuffix(f, "_test.go") || strings.HasSuffix(f, ".md") {
		return 2
	}
	return 1
}

func countTrimmedMatches(lines map[int32]string, want string) int {
	n := 0
	for _, txt := range lines {
		if strings.TrimSpace(txt) == want {
			n++
		}
	}
	return n
}

func buildMockAssistantContent(isSchema bool, promptText, file string, line int32, targetText string) string {
	shiftedLine := line + 5
	if isSchema {
		conf := 0.92
		payload, _ := json.Marshal(struct {
			Summary  string `json:"summary"`
			Findings []struct {
				File           string   `json:"file"`
				Line           int32    `json:"line"`
				Side           string   `json:"side"`
				TargetLineText string   `json:"targetLineText"`
				Anchor         string   `json:"anchor"`
				Severity       string   `json:"severity"`
				Confidence     *float64 `json:"confidence"`
				Category       string   `json:"category"`
				Title          string   `json:"title"`
				Explanation    string   `json:"explanation"`
				Suggestion     string   `json:"suggestion"`
				Evidence       string   `json:"evidence"`
			} `json:"findings"`
		}{
			Summary: fmt.Sprintf("Automated review checked `%s` against repository base rules.", file),
			Findings: []struct {
				File           string   `json:"file"`
				Line           int32    `json:"line"`
				Side           string   `json:"side"`
				TargetLineText string   `json:"targetLineText"`
				Anchor         string   `json:"anchor"`
				Severity       string   `json:"severity"`
				Confidence     *float64 `json:"confidence"`
				Category       string   `json:"category"`
				Title          string   `json:"title"`
				Explanation    string   `json:"explanation"`
				Suggestion     string   `json:"suggestion"`
				Evidence       string   `json:"evidence"`
			}{
				{
					File:           file,
					Line:           shiftedLine,
					Side:           "DIFF_SIDE_RIGHT",
					TargetLineText: targetText,
					Anchor:         "ANCHOR_KIND_QUOTED",
					Severity:       "SEVERITY_HIGH",
					Confidence:     &conf,
					Category:       "FINDING_CATEGORY_CORRECTNESS",
					Title:          "Verify error and state handling on changed path",
					Explanation:    fmt.Sprintf("Inspect `%s` to ensure edge cases and invariants hold.", targetText),
					Suggestion:     "Guard nil/error states explicitly before proceeding.",
					Evidence:       targetText,
				},
			},
		})
		return string(payload)
	}

	if strings.Contains(promptText, "key_issues_to_review") {
		return fmt.Sprintf(
			"review:\n  security_concerns: No\n  key_issues_to_review:\n    - relevant_file: %q\n      end_line: %d\n      issue_header: \"Verify error handling on changed path\"\n      issue_content: \"Check %s\"\n",
			file, line, targetText,
		)
	}

	return fmt.Sprintf(
		"MOCK_ANCHOR|%s|%d|%s\n### Finding in `%s` (approx line %d)\nQuoted line: `%s`\nVerify error and state handling on this path.\n",
		file, line, targetText, file, shiftedLine, targetText,
	)
}

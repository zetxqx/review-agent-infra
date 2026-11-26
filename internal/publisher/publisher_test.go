package publisher

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"regexp"
	"strings"
	"testing"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

const sampleUnifiedDiff = `diff --git a/internal/router/cache.go b/internal/router/cache.go
index 1111111..2222222 100644
--- a/internal/router/cache.go
+++ b/internal/router/cache.go
@@ -10,7 +10,9 @@ func (c *Cache) Lookup(key string) (string, bool) {
 	c.mu.RLock()
-	val, ok := c.items[key]
-	c.mu.RUnlock()
+	val, ok := c.items[key]
+	if !ok {
+		return "", false
+	}
+	c.mu.RUnlock()
 	return val, ok
 }
@@ -30,4 +32,6 @@ func (c *Cache) Reset() error {
 	if c == nil {
 		return errNilCache
 	}
+	if c.closed {
+		return errNilCache
+	}
 	return nil
diff --git a/internal/router/newfile.go b/internal/router/newfile.go
new file mode 100644
index 0000000..3333333
--- /dev/null
+++ b/internal/router/newfile.go
@@ -0,0 +1,4 @@
+package router
+
+func NewHelper() string {
+	return "ok"
+}
\ No newline at end of file
diff --git a/migrations/001.sql b/migrations/001.sql
index 4444444..5555555 100644
--- a/migrations/001.sql
+++ b/migrations/001.sql
@@ -1,3 +1,2 @@
--- legacy header
-DROP TABLE old_runs;
 CREATE TABLE runs (id TEXT PRIMARY KEY);
`

func TestParseUnifiedDiffTracksOldNewAndContextLines(t *testing.T) {
	idx := ParseUnifiedDiff([]byte(sampleUnifiedDiff))

	if !idx.ContainsFile("internal/router/cache.go") {
		t.Fatalf("expected cache.go in diff index")
	}
	if !idx.ContainsFile("internal/router/newfile.go") {
		t.Fatalf("expected newfile.go in diff index")
	}
	if !idx.ContainsFile("migrations/001.sql") {
		t.Fatalf("expected migrations/001.sql in diff index")
	}
	if idx.ContainsFile("internal/router/missing.go") {
		t.Fatalf("unexpected missing.go in diff index")
	}

	right := idx.Lines("internal/router/cache.go", reviewv1.DiffSide_DIFF_SIDE_RIGHT)
	if got := strings.TrimSpace(right[10]); got != "c.mu.RLock()" {
		t.Fatalf("line 10 right = %q, want c.mu.RLock()", got)
	}
	if got := strings.TrimSpace(right[12]); got != "if !ok {" {
		t.Fatalf("line 12 right = %q, want if !ok {", got)
	}
	if got := strings.TrimSpace(right[13]); got != `return "", false` {
		t.Fatalf("line 13 right = %q, want return \"\", false", got)
	}
	if got := strings.TrimSpace(idx.PrevLine("internal/router/cache.go", reviewv1.DiffSide_DIFF_SIDE_RIGHT, 13)); got != "if !ok {" {
		t.Fatalf("prev line for 13 = %q, want if !ok {", got)
	}
	if got := idx.PrevLine("internal/router/cache.go", reviewv1.DiffSide_DIFF_SIDE_RIGHT, 10); got != "" {
		t.Fatalf("prev line at hunk start = %q, want empty", got)
	}

	left := idx.Lines("internal/router/cache.go", reviewv1.DiffSide_DIFF_SIDE_LEFT)
	if got := strings.TrimSpace(left[12]); got != "c.mu.RUnlock()" {
		t.Fatalf("line 12 left = %q, want c.mu.RUnlock()", got)
	}
	if got := strings.TrimSpace(idx.PrevLine("internal/router/cache.go", reviewv1.DiffSide_DIFF_SIDE_LEFT, 12)); got != "val, ok := c.items[key]" {
		t.Fatalf("left prev line for 12 = %q", got)
	}

	sqlLeft := idx.Lines("migrations/001.sql", reviewv1.DiffSide_DIFF_SIDE_LEFT)
	if got := sqlLeft[1]; got != "-- legacy header" {
		t.Fatalf("sql left line 1 = %q, want -- legacy header", got)
	}
}

func TestPlaceExactRelocateAmbiguousAndFallback(t *testing.T) {
	idx := ParseUnifiedDiff([]byte(sampleUnifiedDiff))

	exact := &reviewv1.ReviewFinding{
		File:           "internal/router/cache.go",
		Line:           13,
		Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
		TargetLineText: `return "", false`,
		Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_QUOTED,
	}
	if p := Place(exact, idx, false); p.Kind != PlacementInline || p.Line != 13 {
		t.Fatalf("exact placement = %+v, want inline:13", p)
	}

	shifted := &reviewv1.ReviewFinding{
		File:           "internal/router/cache.go",
		Line:           99,
		Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
		TargetLineText: `   return "", false   `,
		Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_QUOTED,
	}
	if p := Place(shifted, idx, false); p.Kind != PlacementInline || p.Line != 13 {
		t.Fatalf("relocated placement = %+v, want inline:13", p)
	}

	ambiguous := &reviewv1.ReviewFinding{
		File:           "internal/router/cache.go",
		Line:           99,
		Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
		TargetLineText: "return errNilCache",
		Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_QUOTED,
	}
	if p := Place(ambiguous, idx, true); p.Kind != PlacementFile {
		t.Fatalf("ambiguous placement with file support = %+v, want file", p)
	}
	if p := Place(ambiguous, idx, false); p.Kind != PlacementSummary {
		t.Fatalf("ambiguous placement without file support = %+v, want summary", p)
	}

	derivedShifted := &reviewv1.ReviewFinding{
		File:           "internal/router/cache.go",
		Line:           99,
		Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
		TargetLineText: `return "", false`,
		Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_DERIVED,
	}
	if p := Place(derivedShifted, idx, false); p.Kind != PlacementSummary {
		t.Fatalf("derived shifted placement = %+v, want summary", p)
	}

	otherFile := &reviewv1.ReviewFinding{
		File:           "pkg/unrelated.go",
		Line:           1,
		Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
		TargetLineText: "package unrelated",
		Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_QUOTED,
	}
	if p := Place(otherFile, idx, true); p.Kind != PlacementSummary {
		t.Fatalf("unrelated file placement = %+v, want summary", p)
	}
}

func TestFingerprintAlignedAndFallbackFormulas(t *testing.T) {
	idx := ParseUnifiedDiff([]byte(sampleUnifiedDiff))

	f := &reviewv1.ReviewFinding{
		File:           "internal/router/cache.go",
		Line:           13,
		Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
		TargetLineText: "\t\treturn \"\", false  \n",
		Title:          "RLock leaked on cache miss",
	}

	inlinePlacement := Place(f, idx, false)
	if inlinePlacement.Kind != PlacementInline {
		t.Fatalf("expected inline placement, got %+v", inlinePlacement)
	}
	gotInline := Fingerprint(f, inlinePlacement, idx)
	wantInlineBytes := sha256.Sum256([]byte("internal/router/cache.go\nif !ok {\nreturn \"\", false"))
	wantInline := hex.EncodeToString(wantInlineBytes[:])
	if gotInline != wantInline {
		t.Fatalf("inline fingerprint = %s, want %s", gotInline, wantInline)
	}

	fallbackPlacement := Placement{Kind: PlacementSummary, Side: reviewv1.DiffSide_DIFF_SIDE_RIGHT}
	gotFallback := Fingerprint(f, fallbackPlacement, idx)
	wantFallbackBytes := sha256.Sum256([]byte("internal/router/cache.go\nreturn \"\", false"))
	wantFallback := hex.EncodeToString(wantFallbackBytes[:])
	if gotFallback != wantFallback {
		t.Fatalf("fallback fingerprint = %s, want %s", gotFallback, wantFallback)
	}
}

func TestConfidenceFilteringClusteringAndTopN(t *testing.T) {
	low := 0.40
	high := 0.95
	med := 0.82
	nanVal := math.NaN()

	findings := []*reviewv1.ReviewFinding{
		{File: "a.go", Title: "low conf", Confidence: &low},
		{File: "a.go", Title: "default conf", Confidence: nil},
		{File: "a.go", Title: "high conf", Confidence: &high},
		{File: "a.go", Title: "nan conf falls back to default", Confidence: &nanVal},
		{File: "a.go", Title: "mid conf", Confidence: &med},
	}

	filtered := FilterByConfidence(findings, 0.80, 0.75)
	if len(filtered) != 4 {
		t.Fatalf("filtered count = %d, want 4", len(filtered))
	}

	items := []AlignedFinding{
		{
			Finding:             &reviewv1.ReviewFinding{File: "a.go", Title: "medium line 12", Severity: reviewv1.Severity_SEVERITY_MEDIUM},
			Placement:           Placement{Kind: PlacementInline, Line: 12, Side: reviewv1.DiffSide_DIFF_SIDE_RIGHT},
			Fingerprint:         "fp-12",
			EffectiveConfidence: 0.85,
		},
		{
			Finding:             &reviewv1.ReviewFinding{File: "a.go", Title: "critical line 14", Severity: reviewv1.Severity_SEVERITY_CRITICAL},
			Placement:           Placement{Kind: PlacementInline, Line: 14, Side: reviewv1.DiffSide_DIFF_SIDE_RIGHT},
			Fingerprint:         "fp-14",
			EffectiveConfidence: 0.90,
		},
		{
			Finding:             &reviewv1.ReviewFinding{File: "a.go", Title: "duplicate fp", Severity: reviewv1.Severity_SEVERITY_LOW},
			Placement:           Placement{Kind: PlacementInline, Line: 40, Side: reviewv1.DiffSide_DIFF_SIDE_RIGHT},
			Fingerprint:         "fp-14",
			EffectiveConfidence: 0.99,
		},
		{
			Finding:             &reviewv1.ReviewFinding{File: "a.go", Title: "high line 30", Severity: reviewv1.Severity_SEVERITY_HIGH},
			Placement:           Placement{Kind: PlacementInline, Line: 30, Side: reviewv1.DiffSide_DIFF_SIDE_RIGHT},
			Fingerprint:         "fp-30",
			EffectiveConfidence: 0.88,
		},
		{
			Finding:             &reviewv1.ReviewFinding{File: "b.go", Title: "medium b.go", Severity: reviewv1.Severity_SEVERITY_MEDIUM},
			Placement:           Placement{Kind: PlacementInline, Line: 14, Side: reviewv1.DiffSide_DIFF_SIDE_RIGHT},
			Fingerprint:         "fp-b14",
			EffectiveConfidence: 0.80,
		},
	}

	clustered := ClusterAndCap(items, 3, 2)
	if len(clustered) != 2 {
		t.Fatalf("clustered top-2 len = %d, want 2", len(clustered))
	}
	if clustered[0].Finding.Title != "critical line 14" {
		t.Fatalf("first item = %q, want critical line 14", clustered[0].Finding.Title)
	}
	if clustered[1].Finding.Title != "high line 30" {
		t.Fatalf("second item = %q, want high line 30", clustered[1].Finding.Title)
	}
}

func TestSanitizeMarkdownEscapesCommandsMentionsImagesAndTokens(t *testing.T) {
	jwtTok := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJydW5faWQiOiJydW4tMSJ9.signature_123"
	customSecret := "custom-task-secret-xyz"
	raw := strings.Join([]string{
		"Great catch @maintainer and @org-bot!",
		"/lgtm",
		"  /approve",
		"> /hold",
		"/close",
		"/retest",
		"![tracker](https://evil.example/pixel.png)",
		"[external](https://evil.example/phish)",
		"<script>alert(1)</script><!-- hidden -->",
		"Token leaked: " + jwtTok + " and " + customSecret,
		"```bash",
		"/lgtm",
		"Authorization: Bearer eyJleakedInsideCodeBlock12345.payload99",
		"keep @decorator_in_code",
		"```",
	}, "\n")

	clean := SanitizeMarkdown(raw, customSecret)

	prowCmdRe := regexp.MustCompile(`(?m)^[ \t>]*/[A-Za-z]`)
	if prowCmdRe.MatchString(clean) {
		t.Fatalf("unescaped Prow slash command remained in:\n%s", clean)
	}
	if strings.Contains(clean, "@maintainer") || strings.Contains(clean, "@org-bot") {
		t.Fatalf("unescaped @mention remained in:\n%s", clean)
	}
	if !strings.Contains(clean, "@decorator_in_code") {
		t.Fatalf("expected @ inside fenced code block to remain untouched:\n%s", clean)
	}
	if strings.Contains(clean, "https://evil.example") {
		t.Fatalf("external URL remained in:\n%s", clean)
	}
	if strings.Contains(clean, "<script>") || strings.Contains(clean, "<!--") {
		t.Fatalf("raw HTML remained in:\n%s", clean)
	}
	if strings.Contains(clean, "eyJ") || strings.Contains(clean, customSecret) {
		t.Fatalf("task token leaked in:\n%s", clean)
	}
}

func TestRunPipelineEndToEnd(t *testing.T) {
	hi := 0.92
	lo := 0.30
	report := &reviewv1.ReviewReport{
		Summary: "Reviewed by @bot\n/approve\nToken eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjF9.sig",
		Findings: []*reviewv1.ReviewFinding{
			{
				File:           "internal/router/cache.go",
				Line:           99,
				Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
				TargetLineText: `return "", false`,
				Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_QUOTED,
				Severity:       reviewv1.Severity_SEVERITY_CRITICAL,
				Confidence:     &hi,
				Category:       reviewv1.FindingCategory_FINDING_CATEGORY_CONCURRENCY,
				Title:          "RLock not released before early return",
				Explanation:    "Returning without `c.mu.RUnlock()` deadlocks writers.\n/hold",
			},
			{
				File:           "internal/router/cache.go",
				Line:           99,
				Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
				TargetLineText: "return errNilCache",
				Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_QUOTED,
				Severity:       reviewv1.Severity_SEVERITY_MEDIUM,
				Confidence:     &hi,
				Category:       reviewv1.FindingCategory_FINDING_CATEGORY_CORRECTNESS,
				Title:          "Duplicate error branch",
				Explanation:    "Ambiguous line falls back to summary.",
			},
			{
				File:           "internal/router/cache.go",
				Line:           10,
				Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
				TargetLineText: "c.mu.RLock()",
				Confidence:     &lo,
				Title:          "Low confidence noise",
			},
			{
				File:           "unrelated.go",
				Line:           1,
				TargetLineText: "package x",
				Confidence:     &hi,
				Title:          "Unchanged file noise",
			},
		},
	}

	opts := DefaultPipelineOptions()
	res := RunPipeline(report, []byte(sampleUnifiedDiff), opts)

	if res.InlineCount != 1 || len(res.Inlines) != 1 {
		t.Fatalf("inline count = %d, want 1", res.InlineCount)
	}
	if res.Inlines[0].Line != 13 {
		t.Fatalf("inline line = %d, want relocated line 13", res.Inlines[0].Line)
	}
	if res.FallbackCount != 1 {
		t.Fatalf("fallback count = %d, want 1", res.FallbackCount)
	}
	if strings.Contains(res.Summary, "eyJ") || strings.Contains(res.Summary, "@bot") {
		t.Fatalf("summary was not sanitized:\n%s", res.Summary)
	}
	if !strings.Contains(res.Summary, "Duplicate error branch") {
		t.Fatalf("expected fallback finding in summary:\n%s", res.Summary)
	}
	if regexp.MustCompile(`(?m)^[ \t>]*/[A-Za-z]`).MatchString(res.Inlines[0].Body) {
		t.Fatalf("inline body has unescaped slash command:\n%s", res.Inlines[0].Body)
	}
}

func TestFormatFindingMarkdownOmitsEvidenceAndFormatsCodeFences(t *testing.T) {
	if got := FormatFindingMarkdown(nil); got != "" {
		t.Fatalf("nil finding = %q, want empty", got)
	}

	prose := &reviewv1.ReviewFinding{
		Severity:    reviewv1.Severity_SEVERITY_HIGH,
		Title:       "Lock leaked on miss",
		Explanation: "Early return skips `RUnlock`.",
		Suggestion:  "Call `c.mu.RUnlock()` before returning.",
		Evidence:    "internal/router/cache.go:24",
	}
	gotProse := FormatFindingMarkdown(prose)
	wantProse := "**[HIGH]** Lock leaked on miss\n\nEarly return skips `RUnlock`.\n\n**Suggestion:** Call `c.mu.RUnlock()` before returning."
	if gotProse != wantProse {
		t.Fatalf("prose markdown =\n%q\nwant:\n%q", gotProse, wantProse)
	}
	if strings.Contains(gotProse, "**Evidence:**") || strings.Contains(gotProse, "cache.go:24") {
		t.Fatalf("evidence leaked into rendered markdown:\n%s", gotProse)
	}

	backtickFence := &reviewv1.ReviewFinding{
		Severity:   reviewv1.Severity_SEVERITY_MEDIUM,
		Title:      "Unclamped upper bound",
		Suggestion: "```go\nif end >= len(lines) {\n\tend = len(lines) - 1\n}\n```",
		Evidence:   "window_sample.go:36",
	}
	gotBacktick := FormatFindingMarkdown(backtickFence)
	wantBacktick := "**[MEDIUM]** Unclamped upper bound\n\n**Suggestion:**\n```go\nif end >= len(lines) {\n\tend = len(lines) - 1\n}\n```"
	if gotBacktick != wantBacktick {
		t.Fatalf("backtick fence markdown =\n%q\nwant:\n%q", gotBacktick, wantBacktick)
	}

	tildeFence := &reviewv1.ReviewFinding{
		Severity:   reviewv1.Severity_SEVERITY_LOW,
		Title:      "Single-line fix",
		Suggestion: "~~~suggestion\nreturn \"\"\n~~~",
	}
	gotTilde := FormatFindingMarkdown(tildeFence)
	wantTilde := "**[LOW]** Single-line fix\n\n**Suggestion:**\n~~~suggestion\nreturn \"\"\n~~~"
	if gotTilde != wantTilde {
		t.Fatalf("tilde fence markdown =\n%q\nwant:\n%q", gotTilde, wantTilde)
	}

	proseThenFence := &reviewv1.ReviewFinding{
		Severity:   reviewv1.Severity_SEVERITY_HIGH,
		Title:      "Bounds check",
		Suggestion: "Guard empty range before slicing:\n```go\nif start > end {\n\treturn \"\"\n}\n```",
	}
	gotProseThenFence := FormatFindingMarkdown(proseThenFence)
	wantProseThenFence := "**[HIGH]** Bounds check\n\n**Suggestion:**\nGuard empty range before slicing:\n```go\nif start > end {\n\treturn \"\"\n}\n```"
	if gotProseThenFence != wantProseThenFence {
		t.Fatalf("prose+fence markdown =\n%q\nwant:\n%q", gotProseThenFence, wantProseThenFence)
	}

	hi := 0.95
	report := &reviewv1.ReviewReport{
		Findings: []*reviewv1.ReviewFinding{
			{
				File:           "internal/router/cache.go",
				Line:           13,
				Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
				TargetLineText: `return "", false`,
				Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_QUOTED,
				Severity:       reviewv1.Severity_SEVERITY_HIGH,
				Confidence:     &hi,
				Title:          "RLock leaked",
				Explanation:    "Returns while holding read lock.",
				Suggestion:     "```go\nc.mu.RUnlock()\nreturn \"\", false\n```",
				Evidence:       "internal/router/cache.go:13 @maintainer secret-tok",
			},
		},
	}
	opts := DefaultPipelineOptions()
	opts.MintedTokens = []string{"secret-tok"}
	res := RunPipeline(report, []byte(sampleUnifiedDiff), opts)
	if len(res.Inlines) != 1 || len(res.Aligned) != 1 {
		t.Fatalf("pipeline counts: inlines=%d aligned=%d, want 1", len(res.Inlines), len(res.Aligned))
	}
	if strings.Contains(res.Inlines[0].Body, "**Evidence:**") {
		t.Fatalf("inline body unexpectedly contains Evidence section:\n%s", res.Inlines[0].Body)
	}
	if !strings.Contains(res.Inlines[0].Body, "**Suggestion:**\n```go\n") {
		t.Fatalf("inline body did not put fenced suggestion on new line:\n%s", res.Inlines[0].Body)
	}
	if strings.Contains(res.Aligned[0].Finding.Evidence, "secret-tok") || strings.Contains(res.Aligned[0].Finding.Evidence, "@maintainer") {
		t.Fatalf("finding evidence was not sanitized on struct: %q", res.Aligned[0].Finding.Evidence)
	}
	if !strings.Contains(res.Aligned[0].Finding.Evidence, RedactedPlaceholder) {
		t.Fatalf("finding evidence missing redaction placeholder: %q", res.Aligned[0].Finding.Evidence)
	}
}

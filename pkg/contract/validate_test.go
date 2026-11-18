package contract

import (
	"math"
	"strings"
	"testing"
	"unicode/utf8"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

func TestValidateReportNil(t *testing.T) {
	ValidateReport(nil)
}

func TestValidateReportBoundsAndDefaults(t *testing.T) {
	nanConf := math.NaN()
	infConf := math.Inf(1)
	overConf := 1.25
	negConf := -0.1
	zeroConf := 0.0
	validConf := 0.85

	report := &reviewv1.ReviewReport{
		Summary: strings.Repeat("é", 8050),
		Findings: []*reviewv1.ReviewFinding{
			nil,
			{File: "   ", Title: "missing file"},
			{File: "a.go", Title: " \t "},
			{
				File:           "b.go",
				Line:           -12,
				TargetLineText: strings.Repeat("a", 600),
				Title:          strings.Repeat("b", 250),
				Explanation:    strings.Repeat("c", 4100),
				Suggestion:     strings.Repeat("d", 4100),
				Evidence:       strings.Repeat("e", 2100),
				Confidence:     &nanConf,
				Side:           reviewv1.DiffSide(99),
				Anchor:         reviewv1.AnchorKind(99),
				Severity:       reviewv1.Severity(99),
				Category:       reviewv1.FindingCategory(99),
			},
			{File: "c.go", Line: 5, Title: "inf", Confidence: &infConf},
			{File: "d.go", Line: 6, Title: "over", Confidence: &overConf},
			{File: "e.go", Line: 7, Title: "neg", Confidence: &negConf},
			{File: "f.go", Line: 8, Title: "zero", Confidence: &zeroConf},
			{
				File:       "g.go",
				Line:       9,
				Title:      "valid",
				Confidence: &validConf,
				Side:       reviewv1.DiffSide_DIFF_SIDE_LEFT,
				Anchor:     reviewv1.AnchorKind_ANCHOR_KIND_DERIVED,
				Severity:   reviewv1.Severity_SEVERITY_CRITICAL,
				Category:   reviewv1.FindingCategory_FINDING_CATEGORY_CONCURRENCY,
			},
		},
	}

	ValidateReport(report)

	if utf8.RuneCountInString(report.Summary) != 8000 {
		t.Fatalf("expected 8000 summary runes, got %d", utf8.RuneCountInString(report.Summary))
	}
	if len(report.Findings) != 6 {
		t.Fatalf("expected 6 kept findings, got %d", len(report.Findings))
	}

	b := report.Findings[0]
	if b.Line != 0 {
		t.Fatalf("expected negative line clamped to 0, got %d", b.Line)
	}
	if utf8.RuneCountInString(b.TargetLineText) != 500 ||
		utf8.RuneCountInString(b.Title) != 200 ||
		utf8.RuneCountInString(b.Explanation) != 4000 ||
		utf8.RuneCountInString(b.Suggestion) != 4000 ||
		utf8.RuneCountInString(b.Evidence) != 2000 {
		t.Fatal("expected finding string fields truncated to contract bounds")
	}
	if b.Confidence != nil {
		t.Fatalf("expected NaN confidence cleared to nil, got %v", *b.Confidence)
	}
	if b.Side != reviewv1.DiffSide_DIFF_SIDE_RIGHT ||
		b.Anchor != reviewv1.AnchorKind_ANCHOR_KIND_QUOTED ||
		b.Severity != reviewv1.Severity_SEVERITY_MEDIUM ||
		b.Category != reviewv1.FindingCategory_FINDING_CATEGORY_CORRECTNESS {
		t.Fatalf("unexpected defaulted enum values: %+v", b)
	}

	if report.Findings[1].Confidence != nil || report.Findings[2].Confidence != nil || report.Findings[3].Confidence != nil {
		t.Fatal("expected out-of-range or infinite confidences cleared to nil")
	}
	if report.Findings[4].Confidence == nil || *report.Findings[4].Confidence != 0.0 {
		t.Fatalf("expected explicit 0.0 confidence preserved, got %v", report.Findings[4].Confidence)
	}
	g := report.Findings[5]
	if g.Side != reviewv1.DiffSide_DIFF_SIDE_LEFT ||
		g.Anchor != reviewv1.AnchorKind_ANCHOR_KIND_DERIVED ||
		g.Severity != reviewv1.Severity_SEVERITY_CRITICAL ||
		g.Category != reviewv1.FindingCategory_FINDING_CATEGORY_CONCURRENCY ||
		*g.Confidence != 0.85 {
		t.Fatalf("unexpected mutation of valid finding: %+v", g)
	}
}

func TestValidateReportFindingsCap(t *testing.T) {
	findings := make([]*reviewv1.ReviewFinding, 120)
	for i := range findings {
		findings[i] = &reviewv1.ReviewFinding{File: "a.go", Title: "issue"}
	}
	report := &reviewv1.ReviewReport{Findings: findings}
	ValidateReport(report)
	if len(report.Findings) != 100 {
		t.Fatalf("expected findings capped at 100, got %d", len(report.Findings))
	}
}

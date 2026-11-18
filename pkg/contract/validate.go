package contract

import (
	"math"
	"strings"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

func clipRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

func ValidateReport(r *reviewv1.ReviewReport) {
	if r == nil {
		return
	}
	r.Summary = clipRunes(r.Summary, 8000)
	if len(r.Findings) > 100 {
		r.Findings = r.Findings[:100]
	}
	kept := r.Findings[:0]
	for _, f := range r.Findings {
		if f == nil || strings.TrimSpace(f.File) == "" || strings.TrimSpace(f.Title) == "" {
			continue
		}
		if f.Line < 0 {
			f.Line = 0
		}
		f.TargetLineText = clipRunes(f.TargetLineText, 500)
		f.Title = clipRunes(f.Title, 200)
		f.Explanation = clipRunes(f.Explanation, 4000)
		f.Suggestion = clipRunes(f.Suggestion, 4000)
		f.Evidence = clipRunes(f.Evidence, 2000)
		if f.Confidence != nil {
			c := *f.Confidence
			if math.IsNaN(c) || math.IsInf(c, 0) || c < 0 || c > 1 {
				f.Confidence = nil
			}
		}
		if f.Side != reviewv1.DiffSide_DIFF_SIDE_RIGHT && f.Side != reviewv1.DiffSide_DIFF_SIDE_LEFT {
			f.Side = reviewv1.DiffSide_DIFF_SIDE_RIGHT
		}
		if f.Anchor != reviewv1.AnchorKind_ANCHOR_KIND_QUOTED && f.Anchor != reviewv1.AnchorKind_ANCHOR_KIND_DERIVED {
			f.Anchor = reviewv1.AnchorKind_ANCHOR_KIND_QUOTED
		}
		switch f.Severity {
		case reviewv1.Severity_SEVERITY_CRITICAL, reviewv1.Severity_SEVERITY_HIGH,
			reviewv1.Severity_SEVERITY_MEDIUM, reviewv1.Severity_SEVERITY_LOW:
		default:
			f.Severity = reviewv1.Severity_SEVERITY_MEDIUM
		}
		switch f.Category {
		case reviewv1.FindingCategory_FINDING_CATEGORY_CORRECTNESS, reviewv1.FindingCategory_FINDING_CATEGORY_CONCURRENCY,
			reviewv1.FindingCategory_FINDING_CATEGORY_SECURITY, reviewv1.FindingCategory_FINDING_CATEGORY_K8S,
			reviewv1.FindingCategory_FINDING_CATEGORY_STYLE:
		default:
			f.Category = reviewv1.FindingCategory_FINDING_CATEGORY_CORRECTNESS
		}
		kept = append(kept, f)
	}
	r.Findings = kept
}

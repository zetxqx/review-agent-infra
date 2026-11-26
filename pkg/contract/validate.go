package contract

import (
	"fmt"
	"math"
	"path"
	"strings"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func clipRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

// DecodeAndValidateReport unmarshals protojson ReviewReport bytes and enforces field and enum bounds.
func DecodeAndValidateReport(raw []byte) (*reviewv1.ReviewReport, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil, fmt.Errorf("sandbox produced empty report")
	}
	var report reviewv1.ReviewReport
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, &report); err != nil {
		return nil, fmt.Errorf("decode ReviewReport protojson: %w", err)
	}
	ValidateReport(&report)
	return &report, nil
}

// ValidateReport clips string lengths, clamps lines, clears invalid confidence values, and normalizes closed enums.
// It rewrites each finding File to a clean repository-relative path (no leading "./" or "/") and drops findings
// whose File is empty or escapes the repository root.
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
		if f == nil || strings.TrimSpace(f.Title) == "" {
			continue
		}
		cleanFile, ok := cleanRepoRelativePath(f.File)
		if !ok {
			continue
		}
		f.File = cleanFile
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

func cleanRepoRelativePath(raw string) (string, bool) {
	p := strings.TrimSpace(raw)
	for strings.HasPrefix(p, "./") || strings.HasPrefix(p, "/") {
		p = strings.TrimPrefix(strings.TrimPrefix(p, "./"), "/")
	}
	if p == "" {
		return "", false
	}
	p = path.Clean(p)
	if p == "." || p == ".." || strings.HasPrefix(p, "../") {
		return "", false
	}
	return p, true
}

// FailReportOnRejectedRequests marks r HARNESS_FAILED and returns true when the controller proxy rejected
// harness-phase requests and the harness left no deterministic report file.
func FailReportOnRejectedRequests(r *reviewv1.ReviewReport, usage *reviewv1.SandboxUsage) bool {
	if r == nil || usage.GetRejectedRequests() == 0 {
		return false
	}
	if r.GetProvenance().GetNormalizer() == reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC {
		return false
	}
	if r.Provenance == nil {
		r.Provenance = &reviewv1.Provenance{}
	}
	r.Provenance.Status = reviewv1.ReportStatus_REPORT_STATUS_HARNESS_FAILED
	return true
}

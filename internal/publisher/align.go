package publisher

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strings"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

type PlacementKind string

const (
	PlacementInline  PlacementKind = "inline"
	PlacementFile    PlacementKind = "file"
	PlacementSummary PlacementKind = "summary"

	DefaultProfileConfidence  = 0.80
	DefaultMinConfidence      = 0.75
	DefaultAdjacentLineWindow = 3
	DefaultTopN               = 8
)

type Placement struct {
	Kind PlacementKind
	Line int32
	Side reviewv1.DiffSide
}

type AlignedFinding struct {
	Finding             *reviewv1.ReviewFinding
	Placement           Placement
	Fingerprint         string
	EffectiveConfidence float64
}

type PipelineOptions struct {
	DefaultConfidence    float64
	MinConfidence        float64
	SupportsFileComments bool
	AdjacentLineWindow   int32
	TopN                 int
	DropUnchangedFiles   bool
	MintedTokens         []string
	// key: SHA-256 finding fingerprint hex; value: true when already published in an earlier round
	SeenFingerprints map[string]bool
}

type PipelineResult struct {
	Summary        string
	Aligned        []AlignedFinding
	Inlines        []contract.InlineComment
	FileComments   []contract.InlineComment
	RawCount       int
	ConfidentCount int
	InlineCount    int
	FallbackCount  int
}

func DefaultPipelineOptions() PipelineOptions {
	return PipelineOptions{
		DefaultConfidence:    DefaultProfileConfidence,
		MinConfidence:        DefaultMinConfidence,
		SupportsFileComments: false,
		AdjacentLineWindow:   DefaultAdjacentLineWindow,
		TopN:                 DefaultTopN,
		DropUnchangedFiles:   true,
	}
}

func Place(f *reviewv1.ReviewFinding, diff *DiffIndex, supportsFileComments bool) Placement {
	side := resolveSide(f)
	if f == nil || diff == nil || !diff.ContainsFile(f.File) {
		return Placement{Kind: PlacementSummary, Side: side}
	}
	visible := diff.Lines(f.File, side)
	if len(visible) == 0 {
		if supportsFileComments {
			return Placement{Kind: PlacementFile, Side: side}
		}
		return Placement{Kind: PlacementSummary, Side: side}
	}

	want := strings.TrimSpace(f.TargetLineText)
	if want != "" {
		if f.Line > 0 {
			if got, ok := visible[f.Line]; ok && strings.TrimSpace(got) == want {
				return Placement{Kind: PlacementInline, Line: f.Line, Side: side}
			}
		}
		if f.Anchor != reviewv1.AnchorKind_ANCHOR_KIND_DERIVED {
			var hits []int32
			for lineNo, text := range visible {
				if strings.TrimSpace(text) == want {
					hits = append(hits, lineNo)
				}
			}
			if len(hits) == 1 {
				return Placement{Kind: PlacementInline, Line: hits[0], Side: side}
			}
		}
	}

	if supportsFileComments {
		return Placement{Kind: PlacementFile, Side: side}
	}
	return Placement{Kind: PlacementSummary, Side: side}
}

func Fingerprint(f *reviewv1.ReviewFinding, p Placement, diff *DiffIndex) string {
	if f == nil {
		sum := sha256.Sum256(nil)
		return hex.EncodeToString(sum[:])
	}
	file := cleanDiffPath(f.File)
	normTarget := normalizeWhitespace(f.TargetLineText)
	if normTarget == "" {
		normTarget = normalizeWhitespace(strings.ToLower(f.Title))
	}
	if p.Kind == PlacementInline {
		var prev string
		if diff != nil {
			prev = diff.PrevLine(file, p.Side, p.Line)
		}
		normPrev := normalizeWhitespace(prev)
		sum := sha256.Sum256([]byte(file + "\n" + normPrev + "\n" + normTarget))
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256([]byte(file + "\n" + normTarget))
	return hex.EncodeToString(sum[:])
}

func EffectiveConfidence(f *reviewv1.ReviewFinding, defaultConfidence float64) float64 {
	if f != nil && f.Confidence != nil {
		c := *f.Confidence
		if !math.IsNaN(c) && !math.IsInf(c, 0) && c >= 0 && c <= 1 {
			return c
		}
	}
	return defaultConfidence
}

func FilterByConfidence(findings []*reviewv1.ReviewFinding, defaultConfidence, minConfidence float64) []*reviewv1.ReviewFinding {
	out := make([]*reviewv1.ReviewFinding, 0, len(findings))
	for _, f := range findings {
		if f == nil {
			continue
		}
		if EffectiveConfidence(f, defaultConfidence) >= minConfidence {
			out = append(out, f)
		}
	}
	return out
}

func ClusterAndCap(items []AlignedFinding, adjacentWindow int32, topN int) []AlignedFinding {
	type indexedFinding struct {
		item AlignedFinding
		idx  int
	}
	ranked := make([]indexedFinding, len(items))
	for i, it := range items {
		ranked[i] = indexedFinding{item: it, idx: i}
	}
	slices.SortFunc(ranked, func(a, b indexedFinding) int {
		if c := cmp.Compare(severityRank(a.item.Finding.GetSeverity()), severityRank(b.item.Finding.GetSeverity())); c != 0 {
			return c
		}
		if c := cmp.Compare(b.item.EffectiveConfidence, a.item.EffectiveConfidence); c != 0 {
			return c
		}
		if c := cmp.Compare(placementRank(a.item.Placement.Kind), placementRank(b.item.Placement.Kind)); c != 0 {
			return c
		}
		return cmp.Compare(a.idx, b.idx)
	})

	// key: SHA-256 finding fingerprint hex; value: true when already retained
	seenFP := make(map[string]bool, len(ranked))
	kept := make([]AlignedFinding, 0, len(ranked))
	for _, cand := range ranked {
		it := cand.item
		if it.Fingerprint != "" && seenFP[it.Fingerprint] {
			continue
		}
		if it.Placement.Kind == PlacementInline && adjacentWindow >= 0 && overlapsInlineCluster(kept, it, adjacentWindow) {
			continue
		}
		if it.Fingerprint != "" {
			seenFP[it.Fingerprint] = true
		}
		kept = append(kept, it)
		if topN > 0 && len(kept) >= topN {
			break
		}
	}
	return kept
}

func RunPipeline(report *reviewv1.ReviewReport, diffPatch []byte, opts PipelineOptions) PipelineResult {
	if report == nil {
		return PipelineResult{}
	}
	rawCount := len(report.Findings)
	contract.ValidateReport(report)
	diff := ParseUnifiedDiff(diffPatch)

	candidates := make([]*reviewv1.ReviewFinding, 0, len(report.Findings))
	for _, f := range report.Findings {
		f.File = cleanDiffPath(f.File)
		if opts.DropUnchangedFiles && len(diff.Files()) > 0 && !diff.ContainsFile(f.File) {
			continue
		}
		candidates = append(candidates, f)
	}

	confident := FilterByConfidence(candidates, opts.DefaultConfidence, opts.MinConfidence)
	aligned := make([]AlignedFinding, 0, len(confident))
	for _, f := range confident {
		p := Place(f, diff, opts.SupportsFileComments)
		fp := Fingerprint(f, p, diff)
		if opts.SeenFingerprints != nil && opts.SeenFingerprints[fp] {
			continue
		}
		aligned = append(aligned, AlignedFinding{
			Finding:             f,
			Placement:           p,
			Fingerprint:         fp,
			EffectiveConfidence: EffectiveConfidence(f, opts.DefaultConfidence),
		})
	}

	finalItems := ClusterAndCap(aligned, opts.AdjacentLineWindow, opts.TopN)
	res := PipelineResult{
		Summary:        SanitizeMarkdown(report.Summary, opts.MintedTokens...),
		Aligned:        finalItems,
		RawCount:       rawCount,
		ConfidentCount: len(confident),
	}

	var fallbackBlocks []string
	for i := range finalItems {
		it := &finalItems[i]
		sanitizeFindingFields(it.Finding, opts.MintedTokens)
		body := FormatFindingMarkdown(it.Finding)
		sideStr := "RIGHT"
		if it.Placement.Side == reviewv1.DiffSide_DIFF_SIDE_LEFT {
			sideStr = "LEFT"
		}
		switch it.Placement.Kind {
		case PlacementInline:
			res.InlineCount++
			res.Inlines = append(res.Inlines, contract.InlineComment{
				Fingerprint: it.Fingerprint,
				Category:    it.Finding.Category.String(),
				File:        it.Finding.File,
				Line:        it.Placement.Line,
				Side:        sideStr,
				Body:        body,
			})
		case PlacementFile:
			res.FallbackCount++
			res.FileComments = append(res.FileComments, contract.InlineComment{
				Fingerprint: it.Fingerprint,
				Category:    it.Finding.Category.String(),
				File:        it.Finding.File,
				Line:        0,
				Side:        sideStr,
				Body:        body,
			})
		default:
			res.FallbackCount++
			loc := SanitizeMarkdown(it.Finding.File, opts.MintedTokens...)
			if it.Finding.Line > 0 {
				loc = fmt.Sprintf("%s:%d", loc, it.Finding.Line)
			}
			fallbackBlocks = append(fallbackBlocks, fmt.Sprintf("### `%s`\n%s", loc, body))
		}
	}

	if len(fallbackBlocks) > 0 {
		joined := strings.Join(fallbackBlocks, "\n\n")
		if strings.TrimSpace(res.Summary) == "" {
			res.Summary = joined
		} else {
			res.Summary = res.Summary + "\n\n" + joined
		}
	}

	return res
}

func FormatFindingMarkdown(f *reviewv1.ReviewFinding) string {
	if f == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "**[%s]** %s", severityLabel(f.Severity), f.Title)
	if strings.TrimSpace(f.Explanation) != "" {
		b.WriteString("\n\n")
		b.WriteString(strings.TrimSpace(f.Explanation))
	}
	if strings.TrimSpace(f.Suggestion) != "" {
		b.WriteString("\n\n**Suggestion:** ")
		b.WriteString(strings.TrimSpace(f.Suggestion))
	}
	if strings.TrimSpace(f.Evidence) != "" {
		b.WriteString("\n\n**Evidence:** ")
		b.WriteString(strings.TrimSpace(f.Evidence))
	}
	return b.String()
}

func sanitizeFindingFields(f *reviewv1.ReviewFinding, mintedTokens []string) {
	if f == nil {
		return
	}
	f.Title = SanitizeMarkdown(f.Title, mintedTokens...)
	f.Explanation = SanitizeMarkdown(f.Explanation, mintedTokens...)
	f.Suggestion = SanitizeMarkdown(f.Suggestion, mintedTokens...)
	f.Evidence = SanitizeMarkdown(f.Evidence, mintedTokens...)
}

func overlapsInlineCluster(kept []AlignedFinding, cand AlignedFinding, window int32) bool {
	candFile := cleanDiffPath(cand.Finding.GetFile())
	for _, existing := range kept {
		if existing.Placement.Kind != PlacementInline {
			continue
		}
		if cleanDiffPath(existing.Finding.GetFile()) != candFile {
			continue
		}
		if existing.Placement.Side != cand.Placement.Side {
			continue
		}
		delta := existing.Placement.Line - cand.Placement.Line
		if delta < 0 {
			delta = -delta
		}
		if delta <= window {
			return true
		}
	}
	return false
}

func resolveSide(f *reviewv1.ReviewFinding) reviewv1.DiffSide {
	if f != nil && f.Side == reviewv1.DiffSide_DIFF_SIDE_LEFT {
		return reviewv1.DiffSide_DIFF_SIDE_LEFT
	}
	return reviewv1.DiffSide_DIFF_SIDE_RIGHT
}

func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func severityRank(s reviewv1.Severity) int {
	switch s {
	case reviewv1.Severity_SEVERITY_CRITICAL:
		return 1
	case reviewv1.Severity_SEVERITY_HIGH:
		return 2
	case reviewv1.Severity_SEVERITY_MEDIUM:
		return 3
	case reviewv1.Severity_SEVERITY_LOW:
		return 4
	default:
		return 5
	}
}

func severityLabel(s reviewv1.Severity) string {
	switch s {
	case reviewv1.Severity_SEVERITY_CRITICAL:
		return "CRITICAL"
	case reviewv1.Severity_SEVERITY_HIGH:
		return "HIGH"
	case reviewv1.Severity_SEVERITY_MEDIUM:
		return "MEDIUM"
	case reviewv1.Severity_SEVERITY_LOW:
		return "LOW"
	default:
		return "MEDIUM"
	}
}

func placementRank(k PlacementKind) int {
	switch k {
	case PlacementInline:
		return 1
	case PlacementFile:
		return 2
	default:
		return 3
	}
}

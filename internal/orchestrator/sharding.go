package orchestrator

import (
	"fmt"
	"path"
	"strings"

	"github.com/zetxqx/review-agent-infra/internal/publisher"
)

type Shard struct {
	Name            string
	Files           []string
	DiffPatch       []byte
	EstimatedTokens int
}

type fileDiffSection struct {
	file   string
	raw    string
	tokens int
}

func SplitDiffIntoShards(diffPatch []byte, maxTokensPerShard int) []Shard {
	text := string(diffPatch)
	if strings.TrimSpace(text) == "" {
		return nil
	}
	if maxTokensPerShard <= 0 {
		maxTokensPerShard = 8192
	}

	sections := splitFileSections(text)
	var filtered []fileDiffSection
	for _, sec := range sections {
		if shouldSkipGeneratedFile(sec.file) {
			continue
		}
		filtered = append(filtered, sec)
	}
	if len(filtered) == 0 {
		return nil
	}

	var shards []Shard
	var curFiles []string
	var curBlocks []string
	curTokens := 0

	flush := func() {
		if len(curBlocks) == 0 {
			return
		}
		idx := len(shards)
		joined := strings.Join(curBlocks, "\n")
		if !strings.HasSuffix(joined, "\n") {
			joined += "\n"
		}
		shards = append(shards, Shard{
			Name:            fmt.Sprintf("shard-%d", idx),
			Files:           curFiles,
			DiffPatch:       []byte(joined),
			EstimatedTokens: curTokens,
		})
		curFiles = nil
		curBlocks = nil
		curTokens = 0
	}

	for _, sec := range filtered {
		if len(curBlocks) > 0 && curTokens+sec.tokens > maxTokensPerShard {
			flush()
		}
		if sec.file != "" {
			curFiles = append(curFiles, sec.file)
		}
		curBlocks = append(curBlocks, sec.raw)
		curTokens += sec.tokens
	}
	flush()
	return shards
}

func splitFileSections(diffText string) []fileDiffSection {
	lines := strings.Split(diffText, "\n")
	var sections []fileDiffSection
	var buf []string

	flush := func() {
		if len(buf) == 0 {
			return
		}
		raw := strings.TrimRight(strings.Join(buf, "\n"), "\n")
		buf = nil
		if strings.TrimSpace(raw) == "" {
			return
		}
		parsed := publisher.ParseUnifiedDiff([]byte(raw + "\n"))
		files := parsed.Files()
		fileName := ""
		if len(files) > 0 {
			fileName = files[0]
		}
		if fileName == "" && len(parsed.Files()) == 0 && !strings.Contains(raw, "@@") {
			return
		}
		est := max(1, len(raw)/4)
		sections = append(sections, fileDiffSection{
			file:   fileName,
			raw:    raw,
			tokens: est,
		})
	}

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if strings.HasPrefix(line, "diff --git ") {
			flush()
		} else if len(buf) > 0 && strings.HasPrefix(line, "--- ") && i+1 < len(lines) && strings.HasPrefix(lines[i+1], "+++ ") && hasHunkLine(buf) {
			flush()
		}
		buf = append(buf, line)
	}
	flush()
	return sections
}

func hasHunkLine(lines []string) bool {
	for _, l := range lines {
		if strings.HasPrefix(l, "@@ ") {
			return true
		}
	}
	return false
}

func shouldSkipGeneratedFile(file string) bool {
	cleaned := path.Clean(strings.TrimLeft(strings.TrimSpace(file), "/"))
	if cleaned == "" || cleaned == "." {
		return false
	}
	base := path.Base(cleaned)
	if base == "go.sum" || strings.HasPrefix(base, "zz_generated.") {
		return true
	}
	if cleaned == "vendor" || strings.HasPrefix(cleaned, "vendor/") || strings.Contains(cleaned, "/vendor/") {
		return true
	}
	return false
}

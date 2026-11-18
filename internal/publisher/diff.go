package publisher

import (
	"regexp"
	"strconv"
	"strings"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

var hunkHeaderRe = regexp.MustCompile(`^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@`)

type HunkLine struct {
	Prefix  byte
	OldLine int32
	NewLine int32
	Text    string
}

type DiffHunk struct {
	OldStart int32
	OldCount int32
	NewStart int32
	NewCount int32
	Lines    []HunkLine
}

type FileDiff struct {
	OldPath    string
	NewPath    string
	Hunks      []DiffHunk
	LeftLines  map[int32]string
	RightLines map[int32]string
	LeftPrev   map[int32]string
	RightPrev  map[int32]string
}

type DiffIndex struct {
	// key: repository-relative file path; value: parsed diff hunks and line maps
	files map[string]*FileDiff
	order []string
}

func ParseUnifiedDiff(patch []byte) *DiffIndex {
	idx := &DiffIndex{
		// key: repository-relative file path; value: parsed diff hunks and line maps
		files: make(map[string]*FileDiff),
	}
	rawLines := strings.Split(string(patch), "\n")
	var curFile *FileDiff
	var curHunk *DiffHunk
	var oldLine, newLine int32
	var oldRem, newRem int32
	var lastLeft, lastRight string
	var hasLastLeft, hasLastRight bool

	flushHunk := func() {
		if curFile != nil && curHunk != nil {
			curFile.Hunks = append(curFile.Hunks, *curHunk)
			curHunk = nil
		}
	}

	flushFile := func() {
		flushHunk()
		if curFile == nil {
			return
		}
		idx.registerFile(curFile)
		curFile = nil
	}

	newFileDiff := func() *FileDiff {
		return &FileDiff{
			LeftLines:  make(map[int32]string),
			RightLines: make(map[int32]string),
			LeftPrev:   make(map[int32]string),
			RightPrev:  make(map[int32]string),
		}
	}

	for i := 0; i < len(rawLines); i++ {
		line := strings.TrimSuffix(rawLines[i], "\r")

		if strings.HasPrefix(line, "diff --git ") {
			flushFile()
			curFile = newFileDiff()
			oldP, newP := parseGitHeaderPaths(line)
			curFile.OldPath = oldP
			curFile.NewPath = newP
			continue
		}

		inActiveHunk := curHunk != nil && (oldRem > 0 || newRem > 0)

		if !inActiveHunk && strings.HasPrefix(line, "--- ") && i+1 < len(rawLines) && strings.HasPrefix(strings.TrimSuffix(rawLines[i+1], "\r"), "+++ ") {
			if curFile == nil || len(curFile.Hunks) > 0 || curHunk != nil {
				flushFile()
				curFile = newFileDiff()
			}
			curFile.OldPath = parseUnifiedFileHeader(strings.TrimPrefix(line, "--- "))
			i++
			plusLine := strings.TrimSuffix(rawLines[i], "\r")
			curFile.NewPath = parseUnifiedFileHeader(strings.TrimPrefix(plusLine, "+++ "))
			continue
		}

		if curFile != nil && curHunk == nil {
			if strings.HasPrefix(line, "rename from ") {
				curFile.OldPath = cleanDiffPath(strings.TrimPrefix(line, "rename from "))
				continue
			}
			if strings.HasPrefix(line, "rename to ") {
				curFile.NewPath = cleanDiffPath(strings.TrimPrefix(line, "rename to "))
				continue
			}
			if strings.HasPrefix(line, "--- ") {
				curFile.OldPath = parseUnifiedFileHeader(strings.TrimPrefix(line, "--- "))
				continue
			}
			if strings.HasPrefix(line, "+++ ") {
				curFile.NewPath = parseUnifiedFileHeader(strings.TrimPrefix(line, "+++ "))
				continue
			}
		}

		if m := hunkHeaderRe.FindStringSubmatch(line); m != nil {
			if curFile == nil {
				curFile = newFileDiff()
			}
			flushHunk()
			oStart, oCount := parseHunkRange(m[1], m[2])
			nStart, nCount := parseHunkRange(m[3], m[4])
			curHunk = &DiffHunk{
				OldStart: oStart,
				OldCount: oCount,
				NewStart: nStart,
				NewCount: nCount,
			}
			oldLine = oStart
			newLine = nStart
			oldRem = oCount
			newRem = nCount
			lastLeft, lastRight = "", ""
			hasLastLeft, hasLastRight = false, false
			continue
		}

		if curHunk == nil || line == "" {
			continue
		}

		switch line[0] {
		case ' ':
			text := line[1:]
			curHunk.Lines = append(curHunk.Lines, HunkLine{Prefix: ' ', OldLine: oldLine, NewLine: newLine, Text: text})
			curFile.LeftLines[oldLine] = text
			if hasLastLeft {
				curFile.LeftPrev[oldLine] = lastLeft
			}
			lastLeft = text
			hasLastLeft = true

			curFile.RightLines[newLine] = text
			if hasLastRight {
				curFile.RightPrev[newLine] = lastRight
			}
			lastRight = text
			hasLastRight = true

			oldLine++
			newLine++
			if oldRem > 0 {
				oldRem--
			}
			if newRem > 0 {
				newRem--
			}
		case '-':
			text := line[1:]
			curHunk.Lines = append(curHunk.Lines, HunkLine{Prefix: '-', OldLine: oldLine, Text: text})
			curFile.LeftLines[oldLine] = text
			if hasLastLeft {
				curFile.LeftPrev[oldLine] = lastLeft
			}
			lastLeft = text
			hasLastLeft = true
			oldLine++
			if oldRem > 0 {
				oldRem--
			}
		case '+':
			text := line[1:]
			curHunk.Lines = append(curHunk.Lines, HunkLine{Prefix: '+', NewLine: newLine, Text: text})
			curFile.RightLines[newLine] = text
			if hasLastRight {
				curFile.RightPrev[newLine] = lastRight
			}
			lastRight = text
			hasLastRight = true
			newLine++
			if newRem > 0 {
				newRem--
			}
		case '\\':
			continue
		default:
			flushHunk()
		}
	}

	flushFile()
	return idx
}

func (d *DiffIndex) registerFile(fd *FileDiff) {
	primary := fd.NewPath
	if primary == "" {
		primary = fd.OldPath
	}
	if primary == "" {
		return
	}
	if _, exists := d.files[primary]; !exists {
		d.order = append(d.order, primary)
	}
	d.files[primary] = fd
	if fd.OldPath != "" && fd.OldPath != primary {
		if _, exists := d.files[fd.OldPath]; !exists {
			d.files[fd.OldPath] = fd
		}
	}
}

func (d *DiffIndex) Files() []string {
	if d == nil {
		return nil
	}
	out := make([]string, len(d.order))
	copy(out, d.order)
	return out
}

func (d *DiffIndex) ContainsFile(file string) bool {
	if d == nil {
		return false
	}
	_, ok := d.files[cleanDiffPath(file)]
	return ok
}

func (d *DiffIndex) File(file string) *FileDiff {
	if d == nil {
		return nil
	}
	return d.files[cleanDiffPath(file)]
}

func (d *DiffIndex) Lines(file string, side reviewv1.DiffSide) map[int32]string {
	fd := d.File(file)
	if fd == nil {
		return nil
	}
	if side == reviewv1.DiffSide_DIFF_SIDE_LEFT {
		return fd.LeftLines
	}
	return fd.RightLines
}

func (d *DiffIndex) PrevLine(file string, side reviewv1.DiffSide, line int32) string {
	fd := d.File(file)
	if fd == nil {
		return ""
	}
	if side == reviewv1.DiffSide_DIFF_SIDE_LEFT {
		return fd.LeftPrev[line]
	}
	return fd.RightPrev[line]
}

func parseHunkRange(startStr, countStr string) (int32, int32) {
	start, _ := strconv.Atoi(startStr)
	count := 1
	if countStr != "" {
		count, _ = strconv.Atoi(countStr)
	}
	return int32(start), int32(count)
}

func parseGitHeaderPaths(line string) (string, string) {
	rest := strings.TrimSpace(strings.TrimPrefix(line, "diff --git "))
	parts := strings.Fields(rest)
	if len(parts) == 2 {
		return stripABPrefix(parts[0], "a/"), stripABPrefix(parts[1], "b/")
	}
	return "", ""
}

func parseUnifiedFileHeader(spec string) string {
	if idx := strings.IndexByte(spec, '\t'); idx >= 0 {
		spec = spec[:idx]
	}
	spec = unquoteGitPath(strings.TrimSpace(spec))
	if spec == "/dev/null" {
		return ""
	}
	if strings.HasPrefix(spec, "a/") {
		return cleanDiffPath(spec[2:])
	}
	if strings.HasPrefix(spec, "b/") {
		return cleanDiffPath(spec[2:])
	}
	return cleanDiffPath(spec)
}

func stripABPrefix(p, prefix string) string {
	p = unquoteGitPath(strings.TrimSpace(p))
	if p == "/dev/null" {
		return ""
	}
	return cleanDiffPath(strings.TrimPrefix(p, prefix))
}

func unquoteGitPath(p string) string {
	if len(p) >= 2 && p[0] == '"' && p[len(p)-1] == '"' {
		if unq, err := strconv.Unquote(p); err == nil {
			return unq
		}
		return p[1 : len(p)-1]
	}
	return p
}

func cleanDiffPath(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "./")
	p = strings.TrimLeft(p, "/")
	return p
}

package loader

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	MaxReportBytes   = 2 << 20
	maxFileBytes     = 64 << 20
	maxTotalBytes    = 2 << 30
	maxSymlinkDepth  = 40
	defaultWorkspace = "/workspace"
)

// key: basename of untrusted repository instruction file; value: true when stripped from bundle src
var maskedNames = map[string]bool{
	"AGENTS.md":          true,
	"AGENTS.override.md": true,
	"CLAUDE.md":          true,
	"GEMINI.md":          true,
	".cursorrules":       true,
	".goosehints":        true,
	".pr_agent.toml":     true,
	"opencode.json":      true,
	"opencode.jsonc":     true,
	".mcp.json":          true,
	".aider.conf.yml":    true,
}

// key: directory name of untrusted agent config directory; value: true when stripped recursively from bundle src
var maskedDirs = map[string]bool{
	".agents":    true,
	".codex":     true,
	".opencode":  true,
	".openhands": true,
	".claude":    true,
	".cursor":    true,
	".goose":     true,
}

// inputs key: file path relative to /workspace/.review/in; value: raw file bytes.
func BuildBundleTGZ(tarballPath string, inputs map[string][]byte, dst io.Writer) error {
	f, err := os.Open(tarballPath)
	if err != nil {
		return fmt.Errorf("open snapshot tarball: %w", err)
	}
	defer f.Close()
	return BuildBundleTGZFromReader(f, inputs, dst)
}

// inputs key: file path relative to /workspace/.review/in; value: raw file bytes.
func BuildBundleTGZFromReader(srcTGZ io.Reader, inputs map[string][]byte, dst io.Writer) error {
	gzIn, err := gzip.NewReader(srcTGZ)
	if err != nil {
		return err
	}
	defer gzIn.Close()

	gw := gzip.NewWriter(dst)
	tw := tar.NewWriter(gw)
	zeroTime := time.Unix(0, 0)

	for _, baseDir := range []string{"src", ".review", ".review/in", ".review/out"} {
		if err := tw.WriteHeader(&tar.Header{
			Name:     baseDir,
			Typeflag: tar.TypeDir,
			Mode:     0o755,
			ModTime:  zeroTime,
		}); err != nil {
			_ = tw.Close()
			_ = gw.Close()
			return err
		}
	}

	tr := tar.NewReader(gzIn)
	var totalWritten int64
	var allPaths []string
	var symlinkOrder []string
	// key: cleaned repo-relative symlink path; value: raw symlink target
	symlinks := make(map[string]string)
	// key: cleaned repo-relative non-symlink path; value: true when emitted as dir, regular file, or hardlink
	nonSymlinkPaths := make(map[string]bool)
	// key: cleaned repo-relative regular file path; value: true when emitted into bundle src
	emittedRegs := make(map[string]bool)
	// key: masked file path or masked dir glob ("<dir>/*"); value: true when stripped
	maskedSet := make(map[string]bool)

	for {
		hdr, nextErr := tr.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			_ = tw.Close()
			_ = gw.Close()
			return nextErr
		}
		if hdr.Typeflag == tar.TypeXGlobalHeader || hdr.Typeflag == tar.TypeXHeader {
			continue
		}
		if strings.HasPrefix(hdr.Name, "/") || strings.Contains(hdr.Name, "\\") {
			return fmt.Errorf("unsafe tar path %q", hdr.Name)
		}
		if hasTraversalSegment(hdr.Name) {
			return fmt.Errorf("path traversal rejected: %q", hdr.Name)
		}

		rel := stripTopDir(path.Clean(hdr.Name))
		if rel == "" || rel == "." {
			continue
		}
		if path.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, "../") {
			return fmt.Errorf("unsafe tar path %q", hdr.Name)
		}

		if maskedKey, isMasked := matchMaskedPath(rel, hdr.Typeflag == tar.TypeDir); isMasked {
			maskedSet[maskedKey] = true
			continue
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			allPaths = append(allPaths, rel)
			nonSymlinkPaths[rel] = true
			if err := tw.WriteHeader(&tar.Header{
				Name:     "src/" + rel,
				Typeflag: tar.TypeDir,
				Mode:     0o755,
				ModTime:  zeroTime,
			}); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if hdr.Size < 0 || hdr.Size > maxFileBytes {
				return fmt.Errorf("file %q exceeds %d byte cap", rel, maxFileBytes)
			}
			if totalWritten+hdr.Size > maxTotalBytes {
				return fmt.Errorf("archive exceeds %d total byte cap", maxTotalBytes)
			}
			mode := (hdr.Mode & 0o777) | 0o600
			if err := tw.WriteHeader(&tar.Header{
				Name:     "src/" + rel,
				Typeflag: tar.TypeReg,
				Mode:     mode,
				Size:     hdr.Size,
				ModTime:  zeroTime,
			}); err != nil {
				return err
			}
			n, copyErr := io.Copy(tw, io.LimitReader(tr, maxFileBytes+1))
			if copyErr != nil {
				return copyErr
			}
			if n > maxFileBytes {
				return fmt.Errorf("file %q exceeds %d byte cap", rel, maxFileBytes)
			}
			if n != hdr.Size {
				return fmt.Errorf("file %q short read: got %d want %d", rel, n, hdr.Size)
			}
			totalWritten += n
			if totalWritten > maxTotalBytes {
				return fmt.Errorf("archive exceeds %d total byte cap", maxTotalBytes)
			}
			allPaths = append(allPaths, rel)
			nonSymlinkPaths[rel] = true
			emittedRegs[rel] = true
		case tar.TypeSymlink:
			if hdr.Linkname == "" || path.IsAbs(hdr.Linkname) || strings.Contains(hdr.Linkname, "\\") {
				return fmt.Errorf("unsafe symlink target %q -> %q", rel, hdr.Linkname)
			}
			resolved := path.Clean(path.Join(path.Dir(rel), hdr.Linkname))
			if resolved == ".." || strings.HasPrefix(resolved, "../") {
				return fmt.Errorf("escaping symlink rejected: %q -> %q", rel, hdr.Linkname)
			}
			if _, exists := symlinks[rel]; exists {
				return fmt.Errorf("symlink destination %q already exists", rel)
			}
			symlinks[rel] = hdr.Linkname
			symlinkOrder = append(symlinkOrder, rel)
			allPaths = append(allPaths, rel)
		case tar.TypeLink:
			if strings.HasPrefix(hdr.Linkname, "/") || strings.Contains(hdr.Linkname, "\\") || hasTraversalSegment(hdr.Linkname) {
				return fmt.Errorf("escaping hardlink rejected: %q -> %q", rel, hdr.Linkname)
			}
			target := stripTopDir(path.Clean(hdr.Linkname))
			if target == "" || target == "." || path.IsAbs(target) || target == ".." || strings.HasPrefix(target, "../") {
				return fmt.Errorf("escaping hardlink rejected: %q -> %q", rel, hdr.Linkname)
			}
			if _, maskedTarget := matchMaskedPath(target, false); maskedTarget {
				continue
			}
			if !emittedRegs[target] {
				return fmt.Errorf("escaping or unresolved hardlink rejected: %q -> %q", rel, hdr.Linkname)
			}
			if err := tw.WriteHeader(&tar.Header{
				Name:     "src/" + rel,
				Typeflag: tar.TypeLink,
				Linkname: "src/" + target,
				Mode:     0o644,
				ModTime:  zeroTime,
			}); err != nil {
				return err
			}
			allPaths = append(allPaths, rel)
			nonSymlinkPaths[rel] = true
			emittedRegs[rel] = true
		default:
			return fmt.Errorf("unsupported tar entry type %q for %q", hdr.Typeflag, rel)
		}
	}

	if err := validateSymlinks(allPaths, symlinkOrder, symlinks, nonSymlinkPaths); err != nil {
		return err
	}

	for _, rel := range symlinkOrder {
		if err := tw.WriteHeader(&tar.Header{
			Name:     "src/" + rel,
			Typeflag: tar.TypeSymlink,
			Linkname: symlinks[rel],
			Mode:     0o777,
			ModTime:  zeroTime,
		}); err != nil {
			return err
		}
	}

	maskedFiles := make([]string, 0, len(maskedSet))
	for k := range maskedSet {
		maskedFiles = append(maskedFiles, k)
	}
	slices.Sort(maskedFiles)

	preparedInputs, err := prepareBundleInputs(inputs, maskedFiles)
	if err != nil {
		return err
	}
	inputKeys := make([]string, 0, len(preparedInputs))
	for k := range preparedInputs {
		inputKeys = append(inputKeys, k)
	}
	slices.Sort(inputKeys)

	for _, k := range inputKeys {
		if strings.HasPrefix(k, "/") || strings.Contains(k, "\\") || hasTraversalSegment(k) {
			return fmt.Errorf("unsafe input path %q", k)
		}
		cleaned := path.Clean(strings.TrimLeft(k, "/"))
		if cleaned == "" || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
			return fmt.Errorf("unsafe input path %q", k)
		}
		data := preparedInputs[k]
		if err := tw.WriteHeader(&tar.Header{
			Name:     ".review/in/" + cleaned,
			Typeflag: tar.TypeReg,
			Mode:     0o644,
			Size:     int64(len(data)),
			ModTime:  zeroTime,
		}); err != nil {
			return err
		}
		if _, err := tw.Write(data); err != nil {
			return err
		}
	}

	if err := tw.Close(); err != nil {
		_ = gw.Close()
		return err
	}
	return gw.Close()
}

// symlinks key: cleaned repo-relative symlink path; value: raw symlink target.
// nonSymlinkPaths key: cleaned repo-relative non-symlink path; value: true when emitted as dir, file, or hardlink.
func validateSymlinks(allPaths, symlinkOrder []string, symlinks map[string]string, nonSymlinkPaths map[string]bool) error {
	for _, rel := range symlinkOrder {
		if nonSymlinkPaths[rel] {
			return fmt.Errorf("symlink destination %q already exists", rel)
		}
	}
	for _, p := range allPaths {
		for dir := path.Dir(p); dir != "." && dir != "/"; dir = path.Dir(dir) {
			if _, isSym := symlinks[dir]; isSym {
				return fmt.Errorf("symlink ancestor %q is not a real directory", dir)
			}
		}
	}
	for _, rel := range symlinkOrder {
		if err := validateSymlinkChain(rel, symlinks); err != nil {
			return err
		}
	}
	return nil
}

// symlinks key: cleaned repo-relative symlink path; value: raw symlink target.
func validateSymlinkChain(start string, symlinks map[string]string) error {
	var stack []string
	if dir := path.Dir(start); dir != "." {
		stack = strings.Split(dir, "/")
	}
	remaining := strings.Split(symlinks[start], "/")
	hops := 0

	for len(remaining) > 0 {
		seg := remaining[0]
		remaining = remaining[1:]
		if seg == "" || seg == "." {
			continue
		}
		if seg == ".." {
			if len(stack) == 0 {
				return fmt.Errorf("chained symlink escape rejected: %q -> %q", start, symlinks[start])
			}
			stack = stack[:len(stack)-1]
			continue
		}
		stack = append(stack, seg)
		cur := strings.Join(stack, "/")
		if target, isSym := symlinks[cur]; isSym {
			hops++
			if hops > maxSymlinkDepth {
				return fmt.Errorf("symlink loop rejected: %q -> %q", start, symlinks[start])
			}
			if target == "" || path.IsAbs(target) || strings.Contains(target, "\\") {
				return fmt.Errorf("chained symlink escape rejected: %q -> %q", start, target)
			}
			stack = stack[:len(stack)-1]
			nextSegs := strings.Split(target, "/")
			remaining = append(nextSegs, remaining...)
		}
	}
	return nil
}

func matchMaskedPath(rel string, isDir bool) (string, bool) {
	parts := strings.Split(rel, "/")
	for i, seg := range parts {
		if maskedDirs[seg] {
			return strings.Join(parts[:i+1], "/") + "/*", true
		}
	}
	if !isDir && maskedNames[parts[len(parts)-1]] {
		return rel, true
	}
	return "", false
}

// inputs key: file path relative to /workspace/.review/in; value: raw file bytes.
// Return map key: file path relative to /workspace/.review/in; value: raw file bytes with updated task.json.
func prepareBundleInputs(inputs map[string][]byte, maskedFiles []string) (map[string][]byte, error) {
	// key: file path relative to /workspace/.review/in; value: raw file bytes
	out := make(map[string][]byte, len(inputs))
	for k, v := range inputs {
		out[k] = v
	}
	rawTask, ok := out["task.json"]
	if !ok {
		return nil, errors.New("bundle inputs missing task.json")
	}
	var task reviewv1.AgentTask
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(rawTask, &task); err != nil {
		return nil, fmt.Errorf("decode task.json: %w", err)
	}
	task.SrcDir = defaultWorkspace + "/src"
	task.DiffFile = defaultWorkspace + "/.review/in/diff.patch"
	if hasRulesPrefix(out) {
		task.RulesDir = defaultWorkspace + "/.review/in/rules"
	} else if _, hasRulesMD := out["rules.md"]; hasRulesMD {
		task.RulesDir = defaultWorkspace + "/.review/in/rules.md"
	} else {
		task.RulesDir = defaultWorkspace + "/.review/in/rules"
	}
	task.OutDir = defaultWorkspace + "/.review/out"
	if _, hasCI := out["ci_failure.log"]; hasCI && task.CiLogsFile == nil {
		ciPath := defaultWorkspace + "/.review/in/ci_failure.log"
		task.CiLogsFile = &ciPath
	}
	task.MaskedFiles = maskedFiles
	baseRef := "base"
	task.Git = &reviewv1.GitInfo{
		BaseRef: &baseRef,
		HeadRef: "pr",
	}

	encoded, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(&task)
	if err != nil {
		return nil, fmt.Errorf("encode task.json: %w", err)
	}
	out["task.json"] = append(encoded, '\n')
	return out, nil
}

// inputs key: file path relative to /workspace/.review/in; value: raw file bytes.
func hasRulesPrefix(inputs map[string][]byte) bool {
	for k := range inputs {
		cleaned := path.Clean(strings.TrimLeft(k, "/"))
		if strings.HasPrefix(cleaned, "rules/") {
			return true
		}
	}
	return false
}

func hasTraversalSegment(raw string) bool {
	for _, seg := range strings.Split(raw, "/") {
		if seg == ".." {
			return true
		}
	}
	return false
}

func stripTopDir(p string) string {
	cleaned := path.Clean(strings.TrimLeft(p, "/"))
	if idx := strings.IndexByte(cleaned, '/'); idx >= 0 {
		return cleaned[idx+1:]
	}
	return ""
}

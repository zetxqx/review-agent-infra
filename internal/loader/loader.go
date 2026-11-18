package loader

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	MaxReportBytes = 2 << 20
	maxFileBytes   = 64 << 20
	maxTotalBytes  = 2 << 30
)

type Paths struct {
	SrcDir string
	InDir  string
	OutDir string
}

func DefaultPaths() Paths {
	return Paths{
		SrcDir: "/workspace/src",
		InDir:  "/workspace/.review/in",
		OutDir: "/workspace/.review/out",
	}
}

// key: basename of untrusted repository instruction file; value: true when deleted from workspace src
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

// key: directory name of untrusted agent config directory; value: true when deleted recursively from workspace src
var maskedDirs = map[string]bool{
	".agents":    true,
	".codex":     true,
	".opencode":  true,
	".openhands": true,
	".claude":    true,
	".cursor":    true,
	".goose":     true,
}

func UnpackSourceTGZ(r io.Reader, srcDir string, stripTop bool) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gz.Close()
	return ExtractSafeTar(gz, srcDir, stripTop)
}

func Prepare(r io.Reader, p Paths) error {
	if err := os.MkdirAll(p.InDir, 0o755); err != nil {
		return err
	}
	if err := ExtractSafeTar(r, p.InDir, false); err != nil {
		return err
	}
	masked, err := MaskInstructionFiles(p.SrcDir)
	if err != nil {
		return err
	}
	baseRef, err := BuildLocalGitRepo(p.SrcDir, filepath.Join(p.InDir, "diff.patch"), masked)
	if err != nil {
		return err
	}
	return updateTaskJSON(filepath.Join(p.InDir, "task.json"), masked, baseRef)
}

func ReadReport(w io.Writer, outDir string) error {
	root, err := os.OpenRoot(outDir)
	if err != nil {
		return err
	}
	defer root.Close()

	f, err := root.OpenFile("report.json", os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("report.json is not a regular file")
	}
	if info.Size() > MaxReportBytes {
		return fmt.Errorf("report.json size %d exceeds %d byte cap", info.Size(), MaxReportBytes)
	}
	n, err := io.Copy(w, io.LimitReader(f, MaxReportBytes+1))
	if err != nil {
		return err
	}
	if n > MaxReportBytes {
		return fmt.Errorf("report.json read exceeds %d byte cap", MaxReportBytes)
	}
	return nil
}

type deferredSymlink struct {
	rel      string
	linkname string
}

func ExtractSafeTar(r io.Reader, dst string, stripTop bool) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	root, err := os.OpenRoot(dst)
	if err != nil {
		return err
	}
	defer root.Close()

	tr := tar.NewReader(r)
	var totalWritten int64
	var symlinks []deferredSymlink
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
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

		rel := path.Clean(hdr.Name)
		if stripTop {
			rel = stripTopDir(rel)
		}
		if rel == "" || rel == "." {
			continue
		}
		if path.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, "../") {
			return fmt.Errorf("unsafe tar path %q", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := root.MkdirAll(rel, 0o755); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if dir := path.Dir(rel); dir != "." {
				if err := root.MkdirAll(dir, 0o755); err != nil {
					return err
				}
			}
			f, err := root.OpenFile(rel, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, hdr.FileInfo().Mode().Perm()|0o600)
			if err != nil {
				return err
			}
			n, copyErr := io.Copy(f, io.LimitReader(tr, maxFileBytes+1))
			closeErr := f.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if n > maxFileBytes {
				return fmt.Errorf("file %q exceeds %d byte cap", rel, maxFileBytes)
			}
			totalWritten += n
			if totalWritten > maxTotalBytes {
				return fmt.Errorf("archive exceeds %d total byte cap", maxTotalBytes)
			}
		case tar.TypeSymlink:
			if hdr.Linkname == "" || path.IsAbs(hdr.Linkname) || strings.Contains(hdr.Linkname, "\\") {
				return fmt.Errorf("unsafe symlink target %q -> %q", rel, hdr.Linkname)
			}
			resolved := path.Clean(path.Join(path.Dir(rel), hdr.Linkname))
			if resolved == ".." || strings.HasPrefix(resolved, "../") {
				return fmt.Errorf("escaping symlink rejected: %q -> %q", rel, hdr.Linkname)
			}
			symlinks = append(symlinks, deferredSymlink{rel: rel, linkname: hdr.Linkname})
		case tar.TypeLink:
			if strings.HasPrefix(hdr.Linkname, "/") || hasTraversalSegment(hdr.Linkname) {
				return fmt.Errorf("escaping hardlink rejected: %q -> %q", rel, hdr.Linkname)
			}
			target := path.Clean(hdr.Linkname)
			if stripTop {
				target = stripTopDir(target)
			}
			if target == "" || target == "." || path.IsAbs(target) || target == ".." || strings.HasPrefix(target, "../") {
				return fmt.Errorf("escaping hardlink rejected: %q -> %q", rel, hdr.Linkname)
			}
			if dir := path.Dir(rel); dir != "." {
				if err := root.MkdirAll(dir, 0o755); err != nil {
					return err
				}
			}
			if err := root.Link(target, rel); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported tar entry type %q for %q", hdr.Typeflag, rel)
		}
	}

	var created []string
	cleanupCreated := func() {
		for i := len(created) - 1; i >= 0; i-- {
			_ = root.Remove(created[i])
		}
	}

	for _, s := range symlinks {
		if err := ensureRealAncestorDirs(root, path.Dir(s.rel)); err != nil {
			cleanupCreated()
			return err
		}
		if _, err := root.Lstat(s.rel); err == nil {
			cleanupCreated()
			return fmt.Errorf("symlink destination %q already exists", s.rel)
		} else if !errors.Is(err, fs.ErrNotExist) {
			cleanupCreated()
			return err
		}
		if err := root.Symlink(s.linkname, s.rel); err != nil {
			cleanupCreated()
			return err
		}
		created = append(created, s.rel)
	}

	for _, s := range symlinks {
		if _, err := root.Stat(s.rel); err != nil && !errors.Is(err, fs.ErrNotExist) {
			cleanupCreated()
			return fmt.Errorf("chained symlink escape rejected: %q -> %q: %w", s.rel, s.linkname, err)
		}
	}
	return nil
}

func ensureRealAncestorDirs(root *os.Root, dir string) error {
	if dir == "" || dir == "." {
		return nil
	}
	parts := strings.Split(dir, "/")
	cur := ""
	for _, seg := range parts {
		if seg == "" || seg == "." {
			continue
		}
		if cur == "" {
			cur = seg
		} else {
			cur = cur + "/" + seg
		}
		info, err := root.Lstat(cur)
		if errors.Is(err, fs.ErrNotExist) {
			if mkErr := root.Mkdir(cur, 0o755); mkErr != nil {
				return mkErr
			}
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("symlink ancestor %q is not a real directory", cur)
		}
	}
	return nil
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

func MaskInstructionFiles(src string) ([]string, error) {
	var masked []string
	err := filepath.WalkDir(src, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if p == src {
			return nil
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() || d.Type()&os.ModeSymlink != 0 {
			if maskedDirs[name] {
				if d.Type()&os.ModeSymlink != 0 {
					if err := os.Remove(p); err != nil {
						return err
					}
				} else {
					if err := os.RemoveAll(p); err != nil {
						return err
					}
				}
				masked = append(masked, filepath.ToSlash(rel)+"/*")
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}
		if !d.IsDir() && maskedNames[name] {
			if err := os.Remove(p); err != nil {
				return err
			}
			masked = append(masked, filepath.ToSlash(rel))
		}
		return nil
	})
	slices.Sort(masked)
	return masked, err
}

func BuildLocalGitRepo(src, patchPath string, masked []string) (string, error) {
	if err := runGit(src, "init", "-q", "-b", "pr"); err != nil {
		return "", err
	}
	if err := runGit(src, "add", "-A", "-f"); err != nil {
		return "", err
	}
	headTree, err := runGitOutput(src, "write-tree")
	if err != nil {
		return "", err
	}

	var baseCommit string
	if patchData, readErr := os.ReadFile(patchPath); readErr == nil && len(strings.TrimSpace(string(patchData))) > 0 {
		args := []string{"apply", "-R", "--index", "--whitespace=nowarn"}
		for _, pat := range masked {
			args = append(args, "--exclude="+pat)
		}
		args = append(args, patchPath)
		if applyErr := runGit(src, args...); applyErr == nil {
			baseTree, wtErr := runGitOutput(src, "write-tree")
			if wtErr == nil {
				if c, cErr := runGitOutput(src, "commit-tree", baseTree, "-m", "base"); cErr == nil {
					baseCommit = c
					_ = runGit(src, "update-ref", "refs/heads/base", baseCommit)
				}
			}
		}
	}

	commitArgs := []string{"commit-tree", headTree}
	if baseCommit != "" {
		commitArgs = append(commitArgs, "-p", baseCommit)
	}
	commitArgs = append(commitArgs, "-m", "pr head")
	headCommit, err := runGitOutput(src, commitArgs...)
	if err != nil {
		return "", err
	}
	if err := runGit(src, "update-ref", "refs/heads/pr", headCommit); err != nil {
		return "", err
	}
	if err := runGit(src, "read-tree", "--reset", "-u", headCommit); err != nil {
		return "", err
	}
	if baseCommit != "" {
		return "base", nil
	}
	return "", nil
}

func updateTaskJSON(taskPath string, masked []string, baseRef string) error {
	taskBytes, err := os.ReadFile(taskPath)
	if err != nil {
		return err
	}
	var task reviewv1.AgentTask
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(taskBytes, &task); err != nil {
		return err
	}
	task.MaskedFiles = masked
	task.Git = &reviewv1.GitInfo{HeadRef: "pr"}
	if baseRef != "" {
		task.Git.BaseRef = &baseRef
	}
	updated, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(&task)
	if err != nil {
		return err
	}
	return os.WriteFile(taskPath, append(updated, '\n'), 0o644)
}

func runGit(dir string, args ...string) error {
	_, err := runGitOutput(dir, args...)
	return err
}

func runGitOutput(dir string, args ...string) (string, error) {
	baseArgs := append([]string{"-c", "safe.directory=*", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false"}, args...)
	cmd := exec.Command("git", baseArgs...)
	cmd.Dir = dir
	cmd.Env = []string{
		"PATH=/usr/bin:/bin",
		"HOME=/tmp",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_AUTHOR_NAME=review-loader",
		"GIT_AUTHOR_EMAIL=loader@review.invalid",
		"GIT_COMMITTER_NAME=review-loader",
		"GIT_COMMITTER_EMAIL=loader@review.invalid",
		"GIT_AUTHOR_DATE=2000-01-01T00:00:00Z",
		"GIT_COMMITTER_DATE=2000-01-01T00:00:00Z",
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %v: %w: %s", args, err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

package loader

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type tarEntry struct {
	name     string
	typeflag byte
	body     string
	linkname string
}

func buildTarBytes(t *testing.T, entries []tarEntry, gzipped bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	var tw *tar.Writer
	var gw *gzip.Writer
	if gzipped {
		gw = gzip.NewWriter(&buf)
		tw = tar.NewWriter(gw)
	} else {
		tw = tar.NewWriter(&buf)
	}
	for _, e := range entries {
		var hdr *tar.Header
		if e.typeflag == tar.TypeXGlobalHeader {
			// key: PAX extended header keyword; value: header record text
			pax := map[string]string{"comment": e.body}
			hdr = &tar.Header{
				Typeflag:   tar.TypeXGlobalHeader,
				Format:     tar.FormatPAX,
				PAXRecords: pax,
			}
		} else {
			hdr = &tar.Header{
				Name:     e.name,
				Typeflag: e.typeflag,
				Mode:     0o644,
				Size:     int64(len(e.body)),
				Linkname: e.linkname,
			}
			if e.typeflag == tar.TypeDir {
				hdr.Mode = 0o755
				hdr.Size = 0
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("write tar header %s: %v", e.name, err)
		}
		if e.typeflag == tar.TypeReg || e.typeflag == 0 {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("write tar body %s: %v", e.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if gw != nil {
		if err := gw.Close(); err != nil {
			t.Fatalf("close gzip writer: %v", err)
		}
	}
	return buf.Bytes()
}

func TestUnpackSourceTGZAndRejectUnsafeEntries(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "src")
	validTar := buildTarBytes(t, []tarEntry{
		{name: "org-repo-sha/", typeflag: tar.TypeDir},
		{name: "org-repo-sha/main.go", typeflag: tar.TypeReg, body: "package main\n"},
		{name: "org-repo-sha/link.go", typeflag: tar.TypeSymlink, linkname: "main.go"},
		{name: "org-repo-sha/hard.go", typeflag: tar.TypeLink, linkname: "org-repo-sha/main.go"},
		{name: "pax_global_header", typeflag: tar.TypeXGlobalHeader, body: "comment=abc"},
	}, true)

	if err := UnpackSourceTGZ(bytes.NewReader(validTar), dst, true); err != nil {
		t.Fatalf("unexpected unpack error: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "main.go"))
	if err != nil || string(got) != "package main\n" {
		t.Fatalf("unexpected unpacked file: %q %v", got, err)
	}
	if _, err := os.ReadFile(filepath.Join(dst, "hard.go")); err != nil {
		t.Fatalf("expected hardlink target readable: %v", err)
	}

	badCases := []struct {
		name  string
		entry tarEntry
	}{
		{"traversal", tarEntry{name: "org-repo-sha/../escape.txt", typeflag: tar.TypeReg, body: "x"}},
		{"absolute_path", tarEntry{name: "/etc/passwd", typeflag: tar.TypeReg, body: "x"}},
		{"absolute_symlink", tarEntry{name: "org-repo-sha/bad", typeflag: tar.TypeSymlink, linkname: "/etc/passwd"}},
		{"escaping_symlink", tarEntry{name: "org-repo-sha/sub/bad", typeflag: tar.TypeSymlink, linkname: "../../outside"}},
		{"escaping_hardlink", tarEntry{name: "org-repo-sha/badhard", typeflag: tar.TypeLink, linkname: "org-repo-sha/../outside"}},
		{"device_node", tarEntry{name: "org-repo-sha/dev", typeflag: tar.TypeChar}},
	}
	for _, tc := range badCases {
		t.Run(tc.name, func(t *testing.T) {
			badTar := buildTarBytes(t, []tarEntry{tc.entry}, true)
			err := UnpackSourceTGZ(bytes.NewReader(badTar), filepath.Join(t.TempDir(), "src"), true)
			if err == nil {
				t.Fatalf("expected rejection for %s", tc.name)
			}
		})
	}
}

func TestExtractSafeTarRejectsChainedSymlinkEscapes(t *testing.T) {
	t.Run("symlink_parent_escape", func(t *testing.T) {
		parent := t.TempDir()
		if err := os.WriteFile(filepath.Join(parent, "outside_secret.txt"), []byte("TOP_SECRET"), 0o600); err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(parent, "src")
		chainedParentTar := buildTarBytes(t, []tarEntry{
			{name: "repo/a/b", typeflag: tar.TypeSymlink, linkname: ".."},
			{name: "repo/a/b/escape", typeflag: tar.TypeSymlink, linkname: "../outside_secret.txt"},
		}, true)
		if err := UnpackSourceTGZ(bytes.NewReader(chainedParentTar), dst, true); err == nil {
			t.Fatal("expected symlink-parent escape archive to be rejected")
		}
		if _, err := os.Lstat(filepath.Join(dst, "escape")); !os.IsNotExist(err) {
			t.Fatalf("expected escaped symlink not to exist, got err=%v", err)
		}
	})

	t.Run("transitive_symlink_hop_escape", func(t *testing.T) {
		parent := t.TempDir()
		if err := os.WriteFile(filepath.Join(parent, "outside_secret.txt"), []byte("TOP_SECRET"), 0o600); err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(parent, "src")
		transitiveTar := buildTarBytes(t, []tarEntry{
			{name: "repo/a/s1", typeflag: tar.TypeSymlink, linkname: ".."},
			{name: "repo/s2", typeflag: tar.TypeSymlink, linkname: "a/s1/../outside_secret.txt"},
		}, true)
		if err := UnpackSourceTGZ(bytes.NewReader(transitiveTar), dst, true); err == nil {
			t.Fatal("expected transitive symlink-hop escape archive to be rejected")
		}
		if _, err := os.Lstat(filepath.Join(dst, "s2")); !os.IsNotExist(err) {
			t.Fatalf("expected s2 symlink cleaned up after rejection, got err=%v", err)
		}
	})

	t.Run("dangling_intra_root_symlink_allowed", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "src")
		danglingIntra := buildTarBytes(t, []tarEntry{
			{name: "repo/sub/missing_link", typeflag: tar.TypeSymlink, linkname: "does_not_exist_yet.txt"},
		}, true)
		if err := UnpackSourceTGZ(bytes.NewReader(danglingIntra), dst, true); err != nil {
			t.Fatalf("expected intra-root dangling symlink to succeed, got: %v", err)
		}
	})
}

func TestPrepareMasksInstructionsAndBuildsTwoBranchRepo(t *testing.T) {
	base := t.TempDir()
	srcRepo := filepath.Join(base, "origin")
	if err := os.MkdirAll(filepath.Join(srcRepo, ".codex/skills"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(srcRepo, ".agents"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustRun := func(dir string, args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = []string{
			"PATH=/usr/bin:/bin",
			"HOME=" + base,
			"GIT_AUTHOR_NAME=t",
			"GIT_AUTHOR_EMAIL=t@t.invalid",
			"GIT_COMMITTER_NAME=t",
			"GIT_COMMITTER_EMAIL=t@t.invalid",
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v\n%s", args, err, out)
		}
		return string(out)
	}
	mustRun(srcRepo, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(srcRepo, "AGENTS.md"), []byte("base rules\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRepo, "main.go"), []byte("package main\nfunc f() int { return 1 }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mustRun(srcRepo, "add", ".")
	mustRun(srcRepo, "commit", "-q", "-m", "base")

	if err := os.WriteFile(filepath.Join(srcRepo, "AGENTS.md"), []byte("INJECTED\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRepo, "AGENTS.override.md"), []byte("OVERRIDE\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRepo, "CLAUDE.md"), []byte("INJECTED\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRepo, "opencode.jsonc"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRepo, ".cursorrules"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRepo, ".codex/skills/evil.md"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRepo, ".agents/rule.md"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRepo, "main.go"), []byte("package main\nfunc f() int { return 2 }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	diffPatch := mustRun(srcRepo, "diff")

	workSrc := filepath.Join(base, "work/src")
	workIn := filepath.Join(base, "work/in")
	workOut := filepath.Join(base, "work/out")

	srcTar := buildTarBytes(t, []tarEntry{
		{name: "repo-head/main.go", typeflag: tar.TypeReg, body: "package main\nfunc f() int { return 2 }\n"},
		{name: "repo-head/AGENTS.md", typeflag: tar.TypeReg, body: "INJECTED\n"},
		{name: "repo-head/AGENTS.override.md", typeflag: tar.TypeReg, body: "OVERRIDE\n"},
		{name: "repo-head/CLAUDE.md", typeflag: tar.TypeReg, body: "INJECTED\n"},
		{name: "repo-head/opencode.jsonc", typeflag: tar.TypeReg, body: "{}"},
		{name: "repo-head/.cursorrules", typeflag: tar.TypeReg, body: "x"},
		{name: "repo-head/.codex/skills/evil.md", typeflag: tar.TypeReg, body: "x"},
		{name: "repo-head/.agents/rule.md", typeflag: tar.TypeReg, body: "x"},
	}, true)
	if err := UnpackSourceTGZ(bytes.NewReader(srcTar), workSrc, true); err != nil {
		t.Fatalf("unpack source: %v", err)
	}

	taskJSON, err := protojson.Marshal(&reviewv1.AgentTask{
		RunId:   "run-1",
		SrcDir:  workSrc,
		OutDir:  workOut,
		Harness: &reviewv1.HarnessRef{Profile: "pr-agent", Adapter: "pr-agent"},
	})
	if err != nil {
		t.Fatal(err)
	}
	inputsTar := buildTarBytes(t, []tarEntry{
		{name: "diff.patch", typeflag: tar.TypeReg, body: diffPatch},
		{name: "rules.md", typeflag: tar.TypeReg, body: "trusted base rules\n"},
		{name: "task.json", typeflag: tar.TypeReg, body: string(taskJSON)},
	}, false)

	paths := Paths{SrcDir: workSrc, InDir: workIn, OutDir: workOut}
	if err := Prepare(bytes.NewReader(inputsTar), paths); err != nil {
		t.Fatalf("prepare workspace: %v", err)
	}

	for _, gone := range []string{"AGENTS.md", "AGENTS.override.md", "CLAUDE.md", "opencode.jsonc", ".cursorrules", ".codex", ".agents"} {
		if _, err := os.Stat(filepath.Join(workSrc, gone)); !os.IsNotExist(err) {
			t.Fatalf("expected %s masked from src", gone)
		}
	}

	gitDiff := mustRun(workSrc, "diff", "base...pr")
	if !strings.Contains(gitDiff, "+func f() int { return 2 }") {
		t.Fatalf("expected git diff base...pr to contain PR diff, got:\n%s", gitDiff)
	}

	updatedBytes, err := os.ReadFile(filepath.Join(workIn, "task.json"))
	if err != nil {
		t.Fatal(err)
	}
	var updated reviewv1.AgentTask
	if err := protojson.Unmarshal(updatedBytes, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.GetGit().GetBaseRef() != "base" || updated.GetGit().GetHeadRef() != "pr" {
		t.Fatalf("unexpected git info: %+v", updated.Git)
	}
	for _, want := range []string{".agents/*", ".codex/*", "AGENTS.md", "AGENTS.override.md", "CLAUDE.md", ".cursorrules", "opencode.jsonc"} {
		if !slices.Contains(updated.MaskedFiles, want) {
			t.Fatalf("missing %s in MaskedFiles: %v", want, updated.MaskedFiles)
		}
	}
}

func TestReadReportSecurityAndCaps(t *testing.T) {
	outDir := t.TempDir()
	reportPath := filepath.Join(outDir, "report.json")
	if err := os.WriteFile(reportPath, []byte(`{"summary":"ok"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if err := ReadReport(&got, outDir); err != nil {
		t.Fatalf("read valid report: %v", err)
	}
	if got.String() != `{"summary":"ok"}` {
		t.Fatalf("unexpected report content: %q", got.String())
	}

	symDir := t.TempDir()
	if err := os.Symlink(reportPath, filepath.Join(symDir, "report.json")); err != nil {
		t.Fatal(err)
	}
	if err := ReadReport(&bytes.Buffer{}, symDir); err == nil {
		t.Fatal("expected symlink rejection via O_NOFOLLOW")
	}

	bigDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(bigDir, "report.json"), bytes.Repeat([]byte("a"), MaxReportBytes+1), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ReadReport(&bytes.Buffer{}, bigDir); err == nil {
		t.Fatal("expected oversize report rejection")
	}
}

func TestBuildLocalGitRepoPatchApplyFallback(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "main.go"), []byte("package main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	badPatch := filepath.Join(base, "diff.patch")
	if err := os.WriteFile(badPatch, []byte("diff --git a/missing.go b/missing.go\n--- a/missing.go\n+++ b/missing.go\n@@ -1 +1 @@\n-old\n+new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	baseRef, err := BuildLocalGitRepo(src, badPatch, nil)
	if err != nil {
		t.Fatalf("unexpected error on non-applying patch fallback: %v", err)
	}
	if baseRef != "" {
		t.Fatalf("expected empty baseRef on failed reverse-apply, got %q", baseRef)
	}
}

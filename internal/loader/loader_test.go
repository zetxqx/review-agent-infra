package loader

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
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
	size     int64
}

func buildTarBytes(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
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
			sz := int64(len(e.body))
			if e.size > 0 {
				sz = e.size
			}
			hdr = &tar.Header{
				Name:     e.name,
				Typeflag: e.typeflag,
				Mode:     0o644,
				Size:     sz,
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
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	return buf.Bytes()
}

// Return map key: archive entry path; value: tar header and raw body bytes.
func readBundleEntries(t *testing.T, tgz []byte) map[string]struct {
	hdr  tar.Header
	body []byte
} {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(tgz))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	// key: archive entry path; value: tar header and raw body bytes
	out := make(map[string]struct {
		hdr  tar.Header
		body []byte
	})
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		var body []byte
		if hdr.Typeflag == tar.TypeReg || hdr.Typeflag == tar.TypeRegA {
			body, err = io.ReadAll(tr)
			if err != nil {
				t.Fatalf("read tar body %s: %v", hdr.Name, err)
			}
		}
		out[hdr.Name] = struct {
			hdr  tar.Header
			body []byte
		}{hdr: *hdr, body: body}
	}
	return out
}

// Return map key: input file path relative to .review/in; value: raw bytes.
func minimalTaskInputs(t *testing.T) map[string][]byte {
	t.Helper()
	rawTask, err := protojson.Marshal(&reviewv1.AgentTask{
		RunId:   "run-bundle-1",
		Harness: &reviewv1.HarnessRef{Profile: "pr-agent", Adapter: "pr-agent"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// key: input file path relative to .review/in; value: raw bytes
	return map[string][]byte{
		"task.json":  rawTask,
		"diff.patch": []byte("diff --git a/main.go b/main.go\n"),
	}
}

func TestBuildBundleTGZStripsTopDirMasksInstructionsAndWritesInputs(t *testing.T) {
	srcTar := buildTarBytes(t, []tarEntry{
		{name: "org-repo-sha/", typeflag: tar.TypeDir},
		{name: "org-repo-sha/main.go", typeflag: tar.TypeReg, body: "package main\nfunc f() int { return 2 }\n"},
		{name: "org-repo-sha/link.go", typeflag: tar.TypeSymlink, linkname: "main.go"},
		{name: "org-repo-sha/hard.go", typeflag: tar.TypeLink, linkname: "org-repo-sha/main.go"},
		{name: "org-repo-sha/AGENTS.md", typeflag: tar.TypeReg, body: "INJECTED\n"},
		{name: "org-repo-sha/AGENTS.override.md", typeflag: tar.TypeReg, body: "OVERRIDE\n"},
		{name: "org-repo-sha/CLAUDE.md", typeflag: tar.TypeReg, body: "INJECTED\n"},
		{name: "org-repo-sha/opencode.jsonc", typeflag: tar.TypeReg, body: "{}"},
		{name: "org-repo-sha/.cursorrules", typeflag: tar.TypeReg, body: "x"},
		{name: "org-repo-sha/.codex/skills/evil.md", typeflag: tar.TypeReg, body: "x"},
		{name: "org-repo-sha/.agents/rule.md", typeflag: tar.TypeReg, body: "x"},
		{name: "pax_global_header", typeflag: tar.TypeXGlobalHeader, body: "comment=abc"},
	})

	tarPath := filepath.Join(t.TempDir(), "snapshot.tar.gz")
	if err := os.WriteFile(tarPath, srcTar, 0o600); err != nil {
		t.Fatal(err)
	}

	inputs := minimalTaskInputs(t)
	inputs["rules/AGENTS.md"] = []byte("trusted base rule\n")
	inputs["ci_failure.log"] = []byte("FAIL TestRace\n")

	var out bytes.Buffer
	if err := BuildBundleTGZ(tarPath, inputs, &out); err != nil {
		t.Fatalf("BuildBundleTGZ: %v", err)
	}

	entries := readBundleEntries(t, out.Bytes())
	if got := string(entries["src/main.go"].body); got != "package main\nfunc f() int { return 2 }\n" {
		t.Fatalf("unexpected src/main.go: %q", got)
	}
	if entries["src/link.go"].hdr.Typeflag != tar.TypeSymlink || entries["src/link.go"].hdr.Linkname != "main.go" {
		t.Fatalf("unexpected symlink entry: %+v", entries["src/link.go"].hdr)
	}
	if entries["src/hard.go"].hdr.Typeflag != tar.TypeLink || entries["src/hard.go"].hdr.Linkname != "src/main.go" {
		t.Fatalf("unexpected hardlink entry: %+v", entries["src/hard.go"].hdr)
	}

	for _, maskedPath := range []string{
		"src/AGENTS.md",
		"src/AGENTS.override.md",
		"src/CLAUDE.md",
		"src/opencode.jsonc",
		"src/.cursorrules",
		"src/.codex/skills/evil.md",
		"src/.agents/rule.md",
	} {
		if _, exists := entries[maskedPath]; exists {
			t.Fatalf("expected %s stripped from bundle", maskedPath)
		}
	}

	if string(entries[".review/in/rules/AGENTS.md"].body) != "trusted base rule\n" {
		t.Fatalf("missing trusted rules entry in bundle")
	}
	if string(entries[".review/in/ci_failure.log"].body) != "FAIL TestRace\n" {
		t.Fatalf("missing ci_failure.log entry in bundle")
	}

	var updated reviewv1.AgentTask
	if err := protojson.Unmarshal(entries[".review/in/task.json"].body, &updated); err != nil {
		t.Fatalf("unmarshal updated task.json: %v", err)
	}
	if updated.SrcDir != "/workspace/src" || updated.DiffFile != "/workspace/.review/in/diff.patch" ||
		updated.RulesDir != "/workspace/.review/in/rules" || updated.OutDir != "/workspace/.review/out" {
		t.Fatalf("unexpected task paths: %+v", &updated)
	}
	if updated.GetCiLogsFile() != "/workspace/.review/in/ci_failure.log" {
		t.Fatalf("unexpected ci_logs_file: %q", updated.GetCiLogsFile())
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

func TestBuildBundleTGZRejectsUnsafeArchiveEntries(t *testing.T) {
	badCases := []struct {
		name    string
		entries []tarEntry
	}{
		{"traversal", []tarEntry{{name: "org-repo-sha/../escape.txt", typeflag: tar.TypeReg, body: "x"}}},
		{"absolute_path", []tarEntry{{name: "/etc/passwd", typeflag: tar.TypeReg, body: "x"}}},
		{"backslash_path", []tarEntry{{name: "org-repo-sha/a\\b", typeflag: tar.TypeReg, body: "x"}}},
		{"absolute_symlink", []tarEntry{{name: "org-repo-sha/bad", typeflag: tar.TypeSymlink, linkname: "/etc/passwd"}}},
		{"escaping_symlink", []tarEntry{{name: "org-repo-sha/sub/bad", typeflag: tar.TypeSymlink, linkname: "../../outside"}}},
		{"escaping_hardlink", []tarEntry{{name: "org-repo-sha/badhard", typeflag: tar.TypeLink, linkname: "org-repo-sha/../outside"}}},
		{"unresolved_hardlink", []tarEntry{{name: "org-repo-sha/badhard", typeflag: tar.TypeLink, linkname: "org-repo-sha/nonexistent.go"}}},
		{"device_node", []tarEntry{{name: "org-repo-sha/dev", typeflag: tar.TypeChar}}},
		{"symlink_parent_escape", []tarEntry{
			{name: "repo/a/b", typeflag: tar.TypeSymlink, linkname: ".."},
			{name: "repo/a/b/escape", typeflag: tar.TypeSymlink, linkname: "../outside_secret.txt"},
		}},
		{"symlink_parent_file_write", []tarEntry{
			{name: "repo/a/b", typeflag: tar.TypeSymlink, linkname: ".."},
			{name: "repo/a/b/pwn.txt", typeflag: tar.TypeReg, body: "x"},
		}},
		{"transitive_symlink_hop_escape", []tarEntry{
			{name: "repo/a/s1", typeflag: tar.TypeSymlink, linkname: ".."},
			{name: "repo/s2", typeflag: tar.TypeSymlink, linkname: "a/s1/../outside_secret.txt"},
		}},
		{"symlink_cycle", []tarEntry{
			{name: "repo/s1", typeflag: tar.TypeSymlink, linkname: "s2"},
			{name: "repo/s2", typeflag: tar.TypeSymlink, linkname: "s1"},
		}},
		{"symlink_overwrites_file", []tarEntry{
			{name: "repo/a.txt", typeflag: tar.TypeReg, body: "hi"},
			{name: "repo/a.txt", typeflag: tar.TypeSymlink, linkname: "other.txt"},
		}},
	}
	for _, tc := range badCases {
		t.Run(tc.name, func(t *testing.T) {
			badTar := buildTarBytes(t, tc.entries)
			var out bytes.Buffer
			err := BuildBundleTGZFromReader(bytes.NewReader(badTar), minimalTaskInputs(t), &out)
			if err == nil {
				t.Fatalf("expected rejection for %s", tc.name)
			}
		})
	}
}

func TestBuildBundleTGZAllowsDanglingIntraRootSymlinkAndRejectsOversizedFile(t *testing.T) {
	danglingTar := buildTarBytes(t, []tarEntry{
		{name: "repo/sub/missing_link", typeflag: tar.TypeSymlink, linkname: "does_not_exist_yet.txt"},
	})
	var out bytes.Buffer
	if err := BuildBundleTGZFromReader(bytes.NewReader(danglingTar), minimalTaskInputs(t), &out); err != nil {
		t.Fatalf("expected intra-root dangling symlink allowed, got: %v", err)
	}
	entries := readBundleEntries(t, out.Bytes())
	if entries["src/sub/missing_link"].hdr.Linkname != "does_not_exist_yet.txt" {
		t.Fatalf("missing dangling symlink in bundle: %+v", entries["src/sub/missing_link"].hdr)
	}

	var bigTarBuf bytes.Buffer
	gw := gzip.NewWriter(&bigTarBuf)
	tw := tar.NewWriter(gw)
	_ = tw.WriteHeader(&tar.Header{
		Name:     "repo/huge.bin",
		Typeflag: tar.TypeReg,
		Mode:     0o644,
		Size:     maxFileBytes + 1,
	})
	_ = gw.Close()
	if err := BuildBundleTGZFromReader(bytes.NewReader(bigTarBuf.Bytes()), minimalTaskInputs(t), &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "byte cap") {
		t.Fatalf("expected oversize file rejection, got: %v", err)
	}
}

func TestBuildBundleTGZStripsRootAndNestedGitMetadata(t *testing.T) {
	srcTar := buildTarBytes(t, []tarEntry{
		{name: "org-repo-sha/", typeflag: tar.TypeDir},
		{name: "org-repo-sha/main.go", typeflag: tar.TypeReg, body: "package main\n"},
		{name: "org-repo-sha/.git/", typeflag: tar.TypeDir},
		{name: "org-repo-sha/.git/config", typeflag: tar.TypeReg, body: "[core]\n\tfsmonitor = /bin/evil\n"},
		{name: "org-repo-sha/.git/hooks/pre-commit", typeflag: tar.TypeReg, body: "#!/bin/sh\nexit 0\n"},
		{name: "org-repo-sha/vendor/sub/.git/config", typeflag: tar.TypeReg, body: "[filter \"x\"]\n\tclean = /bin/evil\n"},
		{name: "org-repo-sha/vendor/sub/lib.go", typeflag: tar.TypeReg, body: "package sub\n"},
		{name: "org-repo-sha/modules/.git", typeflag: tar.TypeReg, body: "gitdir: ../../outside\n"},
	})

	var out bytes.Buffer
	if err := BuildBundleTGZFromReader(bytes.NewReader(srcTar), minimalTaskInputs(t), &out); err != nil {
		t.Fatalf("BuildBundleTGZFromReader: %v", err)
	}
	entries := readBundleEntries(t, out.Bytes())
	for name := range entries {
		if strings.Contains("/"+name+"/", "/.git/") {
			t.Fatalf("expected git metadata %s stripped from bundle", name)
		}
	}
	for _, kept := range []string{"src/main.go", "src/vendor/sub/lib.go"} {
		if _, ok := entries[kept]; !ok {
			t.Fatalf("expected %s kept in bundle", kept)
		}
	}
	var updated reviewv1.AgentTask
	if err := protojson.Unmarshal(entries[".review/in/task.json"].body, &updated); err != nil {
		t.Fatalf("unmarshal updated task.json: %v", err)
	}
	for _, want := range []string{".git/*", "vendor/sub/.git/*", "modules/.git/*"} {
		if !slices.Contains(updated.MaskedFiles, want) {
			t.Fatalf("missing %s in MaskedFiles: %v", want, updated.MaskedFiles)
		}
	}
}

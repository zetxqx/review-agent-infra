package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestCLISubcommands(t *testing.T) {
	base := t.TempDir()
	srcDir := filepath.Join(base, "src")
	inDir := filepath.Join(base, "in")
	outDir := filepath.Join(base, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}

	var srcBuf bytes.Buffer
	gw := gzip.NewWriter(&srcBuf)
	tw := tar.NewWriter(gw)
	_ = tw.WriteHeader(&tar.Header{Name: "top/app.go", Typeflag: tar.TypeReg, Mode: 0o644, Size: 13})
	_, _ = tw.Write([]byte("package main\n"))
	_ = tw.Close()
	_ = gw.Close()

	if err := run([]string{"unpack-src", "--src-dir", srcDir}, &srcBuf, &bytes.Buffer{}); err != nil {
		t.Fatalf("unpack-src failed: %v", err)
	}

	taskJSON, _ := protojson.Marshal(&reviewv1.AgentTask{RunId: "cli-run", SrcDir: srcDir, OutDir: outDir})
	var inBuf bytes.Buffer
	itw := tar.NewWriter(&inBuf)
	_ = itw.WriteHeader(&tar.Header{Name: "task.json", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(taskJSON))})
	_, _ = itw.Write(taskJSON)
	_ = itw.Close()

	if err := run([]string{"prepare", "--src-dir", srcDir, "--in-dir", inDir}, &inBuf, &bytes.Buffer{}); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}

	if err := os.WriteFile(filepath.Join(outDir, "report.json"), []byte(`{"summary":"clean"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run([]string{"read-report", "--out-dir", outDir}, nil, &out); err != nil {
		t.Fatalf("read-report failed: %v", err)
	}
	if out.String() != `{"summary":"clean"}` {
		t.Fatalf("unexpected report output: %q", out.String())
	}

	if err := run([]string{"bogus"}, nil, &bytes.Buffer{}); err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

package sandbox

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

func TestDockerRunnerRemovesNamedContainerWhenRunIsCanceled(t *testing.T) {
	tmp := t.TempDir()
	logFile := filepath.Join(tmp, "docker_calls.log")
	fakeDocker := filepath.Join(tmp, "fake-docker.sh")
	script := strings.Join([]string{
		"#!/usr/bin/env bash",
		"echo \"$*\" >> " + logFile,
		"if [ \"$1\" = rm ]; then exit 0; fi",
		"cat > /dev/null",
		"exec sleep 30",
	}, "\n")
	if err := os.WriteFile(fakeDocker, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)
	runner := &DockerRunner{DockerBin: fakeDocker, DefaultImage: "test-sandbox:1.0", Network: "none"}
	start := time.Now()
	err := runner.Run(ctx, contract.SandboxSpec{
		ShortRunID:     "cancel1",
		ControllerURL:  "http://10.96.0.42:8081",
		TaskToken:      "opaque-docker-cancel",
		TimeoutSeconds: 60,
	})
	if err == nil {
		t.Fatal("expected canceled docker run to fail")
	}
	if time.Since(start) > 20*time.Second {
		t.Fatalf("canceled docker run took too long: %v", time.Since(start))
	}

	raw, readErr := os.ReadFile(logFile)
	if readErr != nil {
		t.Fatalf("read docker log: %v", readErr)
	}
	calls := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(calls) != 2 {
		t.Fatalf("expected run and rm invocations, got %d:\n%s", len(calls), raw)
	}
	runFields := strings.Fields(calls[0])
	var name string
	for i, f := range runFields {
		if f == "--name" && i+1 < len(runFields) {
			name = runFields[i+1]
		}
	}
	if !strings.HasPrefix(name, "review-sbx-cancel1-") {
		t.Fatalf("expected --name review-sbx-cancel1-*, got %q in %s", name, calls[0])
	}
	if calls[1] != "rm -f "+name {
		t.Fatalf("expected forced removal of %q, got %q", name, calls[1])
	}
}

func TestLocalProcessRunnerRetriesReviewKitInstallAfterCanceledRun(t *testing.T) {
	tmp := t.TempDir()
	pyDir := filepath.Join(tmp, "python")
	if err := os.MkdirAll(pyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fakeUV := filepath.Join(tmp, "fake-uv.sh")
	script := strings.Join([]string{
		"#!/usr/bin/env bash",
		"set -euo pipefail",
		"mkdir -p \"$2/.venv/bin\"",
		"touch \"$2/.venv/bin/review-kit\"",
	}, "\n")
	if err := os.WriteFile(fakeUV, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake uv: %v", err)
	}
	runner := &LocalProcessRunner{PythonProjectDir: pyDir, UVBin: fakeUV}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runner.ensureReviewKit(canceled); err == nil {
		t.Fatal("expected install with canceled context to fail")
	}
	kit, err := runner.ensureReviewKit(context.Background())
	if err != nil {
		t.Fatalf("second install must retry after a canceled first run: %v", err)
	}
	if want := filepath.Join(pyDir, ".venv", "bin", "review-kit"); len(kit) != 1 || kit[0] != want {
		t.Fatalf("unexpected review-kit command %v, want %s", kit, want)
	}
}

package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

var _ contract.SandboxRunner = (*LocalProcessRunner)(nil)

type LocalProcessRunner struct {
	RepoRoot         string
	PythonProjectDir string
	UVBin            string
	WorkParentDir    string
	OnCompleted      func(workRoot string)

	pyOnce sync.Once
	pyKit  string
	pyErr  error
}

func NewLocalProcessRunner(repoRoot string) *LocalProcessRunner {
	return &LocalProcessRunner{RepoRoot: repoRoot}
}

func (r *LocalProcessRunner) Close() error {
	return nil
}

func (r *LocalProcessRunner) Run(ctx context.Context, spec contract.SandboxSpec) error {
	timeoutSec := spec.TimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = 300
	}
	kitTimeout := kitExecTimeout(timeoutSec)
	runCtx, cancel := context.WithTimeout(ctx, kitTimeout+60*time.Second)
	defer cancel()

	kitCmd, err := r.ensureReviewKit(runCtx)
	if err != nil {
		return err
	}

	workRoot, err := os.MkdirTemp(r.WorkParentDir, "review-sbx-*")
	if err != nil {
		return err
	}
	defer func() {
		if r.OnCompleted != nil {
			r.OnCompleted(workRoot)
		}
		restoreWritableRecursive(workRoot)
		_ = os.RemoveAll(workRoot)
	}()

	scratchDir := filepath.Join(workRoot, "scratch", "home")
	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		return err
	}

	// key: stdin bootstrap JSON field name; value: controller sandbox URL or opaque task token
	bootstrapPayload, err := json.Marshal(map[string]string{
		"controller_url": spec.ControllerURL,
		"task_token":     spec.TaskToken,
	})
	if err != nil {
		return err
	}

	kitArgs := append(
		slices.Clone(kitCmd[1:]),
		"run",
		"--workspace", workRoot,
		"--scratch-dir", scratchDir,
	)
	if err := runExec(runCtx, workRoot, kitCmd[0], kitArgs, bytes.NewReader(bootstrapPayload), nil, kitTimeout, spec.TaskToken); err != nil {
		return fmt.Errorf("review-kit run: %w", err)
	}
	return nil
}

func (r *LocalProcessRunner) resolveRepoRoot() (string, error) {
	if r.RepoRoot != "" {
		return filepath.Abs(r.RepoRoot)
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		candidate := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
		if _, err := os.Stat(filepath.Join(candidate, "go.mod")); err == nil {
			return candidate, nil
		}
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for cur := wd; ; cur = filepath.Dir(cur) {
		if _, err := os.Stat(filepath.Join(cur, "go.mod")); err == nil {
			return cur, nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
	}
	return "", errors.New("unable to locate repository root containing go.mod")
}

func (r *LocalProcessRunner) ensureReviewKit(ctx context.Context) ([]string, error) {
	r.pyOnce.Do(func() {
		pyDir := r.PythonProjectDir
		if pyDir == "" {
			repoRoot, err := r.resolveRepoRoot()
			if err != nil {
				r.pyErr = err
				return
			}
			pyDir = filepath.Join(repoRoot, "python")
		}
		absPyDir, err := filepath.Abs(pyDir)
		if err != nil {
			r.pyErr = err
			return
		}
		venvKit := filepath.Join(absPyDir, ".venv", "bin", "review-kit")
		if info, statErr := os.Stat(venvKit); statErr == nil && !info.IsDir() {
			r.pyKit = venvKit
			return
		}

		uvBin, err := resolveUVBinary(r.UVBin)
		if err != nil {
			r.pyErr = err
			return
		}
		cmd := exec.CommandContext(ctx, uvBin, "--project", absPyDir, "sync", "--quiet")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if syncErr := cmd.Run(); syncErr != nil {
			r.pyErr = fmt.Errorf("uv sync %s: %w (%s)", absPyDir, syncErr, strings.TrimSpace(stderr.String()))
			return
		}
		if info, statErr := os.Stat(venvKit); statErr == nil && !info.IsDir() {
			r.pyKit = venvKit
			return
		}
		r.pyErr = fmt.Errorf("review-kit binary not found at %s after uv sync", venvKit)
	})
	if r.pyErr != nil {
		return nil, r.pyErr
	}
	return []string{r.pyKit}, nil
}

func resolveUVBinary(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if p, err := exec.LookPath("uv"); err == nil {
		return p, nil
	}
	if home, err := os.UserHomeDir(); err == nil {
		cand := filepath.Join(home, ".local", "bin", "uv")
		if info, statErr := os.Stat(cand); statErr == nil && !info.IsDir() {
			return cand, nil
		}
	}
	return "", errors.New("uv binary not found in PATH or ~/.local/bin/uv")
}

func restoreWritableRecursive(root string) {
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if d.IsDir() {
			_ = os.Chmod(p, 0o700)
		} else {
			_ = os.Chmod(p, 0o600)
		}
		return nil
	})
}

func runExec(ctx context.Context, cwd, bin string, args []string, stdin io.Reader, stdout io.Writer, timeout time.Duration, secretTokens ...string) error {
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(execCtx, bin, args...)
	if cwd != "" {
		cmd.Dir = cwd
	}
	if stdin != nil {
		cmd.Stdin = stdin
	}
	stderr := &stderrTail{max: 4096}
	if stdout != nil {
		cmd.Stdout = stdout
	} else {
		cmd.Stdout = io.Discard
	}
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return withStderr(fmt.Errorf("%s %v: %w", filepath.Base(bin), args, err), stderr.String(), secretTokens...)
	}
	return nil
}

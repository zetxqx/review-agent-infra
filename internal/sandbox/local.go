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

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

var _ contract.SandboxRunner = (*LocalProcessRunner)(nil)

type LocalProcessRunner struct {
	RepoRoot         string
	LoaderBin        string
	PythonProjectDir string
	UVBin            string
	WorkParentDir    string
	OnPrepared       func(srcDir, inDir string)

	loaderOnce     sync.Once
	builtLoaderBin string
	builtLoaderDir string
	loaderBuildErr error

	pyOnce sync.Once
	pyKit  string
	pyErr  error
}

func NewLocalProcessRunner(repoRoot string) *LocalProcessRunner {
	return &LocalProcessRunner{RepoRoot: repoRoot}
}

func (r *LocalProcessRunner) Close() error {
	if r.builtLoaderDir != "" {
		return os.RemoveAll(r.builtLoaderDir)
	}
	return nil
}

func (r *LocalProcessRunner) Run(ctx context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
	timeoutSec := spec.TimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = 300
	}
	kitTimeout := kitExecTimeout(timeoutSec)
	runCtx, cancel := context.WithTimeout(ctx, kitTimeout+60*time.Second)
	defer cancel()

	loaderBin, err := r.ensureLoaderBin(runCtx)
	if err != nil {
		return nil, err
	}
	kitCmd, err := r.ensureReviewKit(runCtx)
	if err != nil {
		return nil, err
	}

	workRoot, err := os.MkdirTemp(r.WorkParentDir, "review-sbx-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		restoreWritableRecursive(workRoot)
		_ = os.RemoveAll(workRoot)
	}()

	srcDir := filepath.Join(workRoot, "src")
	inDir := filepath.Join(workRoot, ".review", "in")
	outDir := filepath.Join(workRoot, ".review", "out")
	scratchDir := filepath.Join(workRoot, "scratch", "home")
	for _, d := range []string{srcDir, inDir, outDir, scratchDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}

	snapFile, err := os.Open(spec.TarballPath)
	if err != nil {
		return nil, fmt.Errorf("open snapshot tarball: %w", err)
	}
	unpackErr := runExec(runCtx, "", loaderBin, []string{"unpack-src", "--src-dir", srcDir}, snapFile, nil, 60*time.Second)
	_ = snapFile.Close()
	if unpackErr != nil {
		return nil, fmt.Errorf("loader unpack-src: %w", unpackErr)
	}

	rewrittenInputs, err := rewriteTaskPaths(spec.Inputs, srcDir, inDir, outDir)
	if err != nil {
		return nil, err
	}
	inputsTar, err := buildInputsTar(rewrittenInputs)
	if err != nil {
		return nil, err
	}
	if err := runExec(runCtx, "", loaderBin, []string{"prepare", "--src-dir", srcDir, "--in-dir", inDir}, bytes.NewReader(inputsTar), nil, 30*time.Second); err != nil {
		return nil, fmt.Errorf("loader prepare: %w", err)
	}

	if err := lockReadOnlyRecursive(srcDir); err != nil {
		return nil, fmt.Errorf("chmod read-only src: %w", err)
	}
	if err := lockReadOnlyRecursive(inDir); err != nil {
		return nil, fmt.Errorf("chmod read-only in: %w", err)
	}

	if r.OnPrepared != nil {
		r.OnPrepared(srcDir, inDir)
	}

	// key: stdin credential JSON field name; value: adapter base URL or ephemeral JWT task token
	credsPayload, err := json.Marshal(map[string]string{
		"openai_base_url": spec.AdapterURL,
		"openai_api_key":  spec.TaskToken,
	})
	if err != nil {
		return nil, err
	}

	kitArgs := append(
		slices.Clone(kitCmd[1:]),
		"run",
		"--in-dir", inDir,
		"--out-dir", outDir,
		"--scratch-dir", scratchDir,
	)
	if err := runExec(runCtx, workRoot, kitCmd[0], kitArgs, bytes.NewReader(credsPayload), nil, kitTimeout); err != nil {
		return nil, fmt.Errorf("review-kit run: %w", err)
	}

	reportOut := &cappedBuffer{max: maxReportReadBytes}
	if err := runExec(runCtx, "", loaderBin, []string{"read-report", "--out-dir", outDir}, nil, reportOut, 15*time.Second); err != nil {
		return nil, fmt.Errorf("loader read-report: %w", err)
	}
	return decodeAndValidateReport(reportOut.Bytes())
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

func (r *LocalProcessRunner) ensureLoaderBin(ctx context.Context) (string, error) {
	if r.LoaderBin != "" {
		return r.LoaderBin, nil
	}
	r.loaderOnce.Do(func() {
		repoRoot, err := r.resolveRepoRoot()
		if err != nil {
			r.loaderBuildErr = err
			return
		}
		prebuilt := filepath.Join(repoRoot, "bin", "loader")
		if info, statErr := os.Stat(prebuilt); statErr == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			r.builtLoaderBin = prebuilt
			return
		}
		tmpDir, err := os.MkdirTemp("", "review-loader-bin-*")
		if err != nil {
			r.loaderBuildErr = err
			return
		}
		outBin := filepath.Join(tmpDir, "loader")
		cmd := exec.CommandContext(ctx, "go", "build", "-o", outBin, "./cmd/loader")
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GO111MODULE=on")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if buildErr := cmd.Run(); buildErr != nil {
			_ = os.RemoveAll(tmpDir)
			r.loaderBuildErr = fmt.Errorf("build cmd/loader: %w (%s)", buildErr, strings.TrimSpace(stderr.String()))
			return
		}
		r.builtLoaderDir = tmpDir
		r.builtLoaderBin = outBin
	})
	return r.builtLoaderBin, r.loaderBuildErr
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

func lockReadOnlyRecursive(root string) error {
	var dirs []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, p)
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		perm := fs.FileMode(0o444)
		if info.Mode()&0o111 != 0 {
			perm = 0o555
		}
		return os.Chmod(p, perm)
	})
	if err != nil {
		return err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Chmod(dirs[i], 0o555); err != nil {
			return err
		}
	}
	return nil
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

func runExec(ctx context.Context, cwd, bin string, args []string, stdin io.Reader, stdout io.Writer, timeout time.Duration) error {
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(execCtx, bin, args...)
	if cwd != "" {
		cmd.Dir = cwd
	}
	if stdin != nil {
		cmd.Stdin = stdin
	}
	var stderr bytes.Buffer
	if stdout != nil {
		cmd.Stdout = stdout
	} else {
		cmd.Stdout = io.Discard
	}
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %v: %w (%s)", filepath.Base(bin), args, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

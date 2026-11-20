package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

const DefaultSandboxImage = "review-agent-infra/poc-sandbox:local"

var _ contract.SandboxRunner = (*DockerRunner)(nil)

type DockerRunner struct {
	DockerBin     string
	DefaultImage  string
	Network       string
	ExtraRunArgs  []string
	WorkParentDir string
}

func NewDockerRunner(defaultImage, network string) *DockerRunner {
	return &DockerRunner{
		DockerBin:    "docker",
		DefaultImage: defaultImage,
		Network:      network,
	}
}

func (r *DockerRunner) Run(ctx context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
	dockerBin := r.DockerBin
	if dockerBin == "" {
		dockerBin = "docker"
	}
	image := spec.HarnessImage
	if image == "" {
		image = r.DefaultImage
	}
	if image == "" {
		image = DefaultSandboxImage
	}
	netMode := r.Network
	if netMode == "" {
		netMode = "bridge"
	}
	timeoutSec := spec.TimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = 300
	}
	userSpec := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())

	kitTimeout := kitExecTimeout(timeoutSec)
	runCtx, cancel := context.WithTimeout(ctx, kitTimeout+90*time.Second)
	defer cancel()

	workRoot, err := os.MkdirTemp(r.WorkParentDir, "review-docker-sbx-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		restoreWritableRecursive(workRoot)
		_ = os.RemoveAll(workRoot)
	}()

	srcDir := filepath.Join(workRoot, "src")
	inDir := filepath.Join(workRoot, "in")
	outDir := filepath.Join(workRoot, "out")
	scratchDir := filepath.Join(workRoot, "scratch")
	for _, d := range []string{srcDir, inDir, outDir, scratchDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}

	snapFile, err := os.Open(spec.TarballPath)
	if err != nil {
		return nil, fmt.Errorf("open snapshot tarball: %w", err)
	}
	unpackArgs := []string{
		"run", "--rm", "-i",
		"--user", userSpec,
		"--network=none",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"-v", srcDir + ":/workspace/src:rw",
		"--entrypoint", "/usr/local/bin/loader",
		image, "unpack-src",
	}
	unpackErr := runExec(runCtx, "", dockerBin, unpackArgs, snapFile, nil, 90*time.Second)
	_ = snapFile.Close()
	if unpackErr != nil {
		return nil, fmt.Errorf("docker loader unpack-src: %w", unpackErr)
	}

	containerInputs, err := rewriteTaskPaths(spec.Inputs, "/workspace/src", "/workspace/.review/in", "/workspace/.review/out")
	if err != nil {
		return nil, err
	}
	inputsTar, err := buildInputsTar(containerInputs)
	if err != nil {
		return nil, err
	}
	prepareArgs := []string{
		"run", "--rm", "-i",
		"--user", userSpec,
		"--network=none",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"-v", srcDir + ":/workspace/src:rw",
		"-v", inDir + ":/workspace/.review/in:rw",
		"--entrypoint", "/usr/local/bin/loader",
		image, "prepare",
	}
	if err := runExec(runCtx, "", dockerBin, prepareArgs, bytes.NewReader(inputsTar), nil, 45*time.Second); err != nil {
		return nil, fmt.Errorf("docker loader prepare: %w", err)
	}

	// key: stdin credential JSON field name; value: adapter base URL or ephemeral JWT task token
	credsPayload, err := json.Marshal(map[string]string{
		"openai_base_url": spec.AdapterURL,
		"openai_api_key":  spec.TaskToken,
	})
	if err != nil {
		return nil, err
	}

	harnessArgs := []string{
		"run", "--rm", "-i",
		"--user", userSpec,
		"--read-only",
		"--tmpfs", "/tmp:rw,noexec,nosuid,size=64m,mode=1777",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"--network", netMode,
	}
	if netMode != "host" {
		harnessArgs = append(harnessArgs, "--dns", "127.0.0.1")
	}
	harnessArgs = append(
		harnessArgs,
		"-v", srcDir+":/workspace/src:ro",
		"-v", inDir+":/workspace/.review/in:ro",
		"-v", outDir+":/workspace/.review/out:rw",
		"-v", scratchDir+":/scratch/home:rw",
	)
	harnessArgs = append(harnessArgs, r.ExtraRunArgs...)
	harnessArgs = append(harnessArgs, image, "review-kit", "run", "--scratch-dir", "/scratch/home")

	if err := runExec(runCtx, "", dockerBin, harnessArgs, bytes.NewReader(credsPayload), nil, kitTimeout); err != nil {
		return nil, fmt.Errorf("docker harness run: %w", err)
	}

	reportOut := &cappedBuffer{max: maxReportReadBytes}
	readArgs := []string{
		"run", "--rm",
		"--user", userSpec,
		"--network=none",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"-v", outDir + ":/workspace/.review/out:ro",
		"--entrypoint", "/usr/local/bin/loader",
		image, "read-report",
	}
	if err := runExec(runCtx, "", dockerBin, readArgs, nil, reportOut, 20*time.Second); err != nil {
		return nil, fmt.Errorf("docker loader read-report: %w", err)
	}
	return decodeAndValidateReport(reportOut.Bytes())
}

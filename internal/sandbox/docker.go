package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

const (
	DefaultSandboxImage = "review-agent-infra/poc-sandbox:local"
	sandboxContainerUID = 65532
	sandboxContainerGID = 65532
)

var _ contract.SandboxRunner = (*DockerRunner)(nil)

type DockerRunner struct {
	DockerBin    string
	DefaultImage string
	Network      string
	ExtraRunArgs []string
}

func NewDockerRunner(defaultImage, network string) *DockerRunner {
	return &DockerRunner{
		DockerBin:    "docker",
		DefaultImage: defaultImage,
		Network:      network,
	}
}

func (r *DockerRunner) Run(ctx context.Context, spec contract.SandboxSpec) error {
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
	userSpec := fmt.Sprintf("%d:%d", sandboxContainerUID, sandboxContainerGID)

	kitTimeout := kitExecTimeout(timeoutSec)
	runCtx, cancel := context.WithTimeout(ctx, kitTimeout+60*time.Second)
	defer cancel()

	// key: stdin bootstrap JSON field name; value: controller sandbox URL or opaque task token
	bootstrapPayload, err := json.Marshal(map[string]string{
		"controller_url": spec.ControllerURL,
		"task_token":     spec.TaskToken,
	})
	if err != nil {
		return err
	}

	args := []string{
		"run", "--rm", "-i",
		"--user", userSpec,
		"--read-only",
		"--tmpfs", fmt.Sprintf("/workspace:rw,noexec,nosuid,nodev,size=2g,uid=%d,gid=%d", sandboxContainerUID, sandboxContainerGID),
		"--tmpfs", fmt.Sprintf("/scratch:rw,exec,nosuid,nodev,size=2g,uid=%d,gid=%d", sandboxContainerUID, sandboxContainerGID),
		"--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=512m,mode=1777",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"--network", netMode,
	}
	if netMode != "host" {
		args = append(args, "--dns", "127.0.0.1")
	}
	args = append(args, r.ExtraRunArgs...)
	args = append(args, image, "review-kit", "run")

	if err := runExec(runCtx, "", dockerBin, args, bytes.NewReader(bootstrapPayload), nil, kitTimeout, spec.TaskToken); err != nil {
		return fmt.Errorf("docker harness run: %w", err)
	}
	return nil
}

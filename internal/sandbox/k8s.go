package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/utils/ptr"
)

var _ contract.SandboxRunner = (*K8sSandboxRunner)(nil)

var nonDNSLabel = regexp.MustCompile(`[^a-z0-9-]+`)

type ExecFunc func(ctx context.Context, podName, container string, cmd []string, stdin io.Reader, stdout io.Writer) error

type K8sConfig struct {
	Namespace                    string
	LoaderImage                  string
	RuntimeClassName             string
	WaitRunningTimeout           time.Duration
	PollInterval                 time.Duration
	ActiveDeadlinePaddingSeconds int
	OwnerPod                     *corev1.Pod
	Executor                     ExecFunc
}

type K8sSandboxRunner struct {
	clientset kubernetes.Interface
	restCfg   *rest.Config
	cfg       K8sConfig
}

func NewK8sSandboxRunner(clientset kubernetes.Interface, restCfg *rest.Config, cfg K8sConfig) *K8sSandboxRunner {
	if cfg.Namespace == "" {
		cfg.Namespace = "review-sandbox"
	}
	if cfg.LoaderImage == "" {
		cfg.LoaderImage = "ghcr.io/zetxqx/review-loader:0.1.0"
	}
	if cfg.RuntimeClassName == "" {
		cfg.RuntimeClassName = "gvisor"
	}
	if cfg.WaitRunningTimeout <= 0 {
		cfg.WaitRunningTimeout = 120 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.ActiveDeadlinePaddingSeconds <= 0 {
		cfg.ActiveDeadlinePaddingSeconds = 300
	}
	return &K8sSandboxRunner{
		clientset: clientset,
		restCfg:   restCfg,
		cfg:       cfg,
	}
}

func sanitizeDNSLabel(raw string) string {
	cleaned := strings.Trim(nonDNSLabel.ReplaceAllString(strings.ToLower(raw), "-"), "-")
	if len(cleaned) > 63 {
		cleaned = strings.TrimRight(cleaned[:63], "-")
	}
	if cleaned == "" {
		return "default"
	}
	return cleaned
}

func PodNameFor(spec contract.SandboxSpec) string {
	suffix := "primary"
	if spec.Role == "shadow" {
		suffix = "shadow-" + sanitizeDNSLabel(spec.HarnessProfile)
	}
	runSlug := sanitizeDNSLabel(spec.ShortRunID)
	name := fmt.Sprintf("review-sbx-%s-s%d-a%d-%s", runSlug, spec.ShardIndex, spec.Attempt, suffix)
	if len(name) > 63 {
		name = strings.TrimRight(name[:63], "-")
	}
	return name
}

func BuildHardenedPod(podName string, spec contract.SandboxSpec, cfg K8sConfig) *corev1.Pod {
	ns := cfg.Namespace
	if ns == "" {
		ns = "review-sandbox"
	}
	loaderImage := cfg.LoaderImage
	if loaderImage == "" {
		loaderImage = "ghcr.io/zetxqx/review-loader:0.1.0"
	}
	rtClass := cfg.RuntimeClassName
	if rtClass == "" {
		rtClass = "gvisor"
	}
	harnessImage := spec.HarnessImage
	if harnessImage == "" {
		harnessImage = DefaultSandboxImage
	}
	timeoutSec := spec.TimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = 900
	}
	padSec := cfg.ActiveDeadlinePaddingSeconds
	if padSec <= 0 {
		padSec = 300
	}
	role := spec.Role
	if role == "" {
		role = "primary"
	}

	// key: Kubernetes Pod label name; value: label value
	labels := map[string]string{
		"app":            "review-sandbox",
		"review-run":     sanitizeDNSLabel(spec.ShortRunID),
		"review-role":    sanitizeDNSLabel(role),
		"review-profile": sanitizeDNSLabel(spec.HarnessProfile),
	}

	containerSecurity := &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}

	var owners []metav1.OwnerReference
	if cfg.OwnerPod != nil && cfg.OwnerPod.Namespace == ns && cfg.OwnerPod.UID != "" {
		owners = append(owners, metav1.OwnerReference{
			APIVersion: "v1",
			Kind:       "Pod",
			Name:       cfg.OwnerPod.Name,
			UID:        cfg.OwnerPod.UID,
		})
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            podName,
			Namespace:       ns,
			Labels:          labels,
			OwnerReferences: owners,
		},
		Spec: corev1.PodSpec{
			RuntimeClassName:             ptr.To(rtClass),
			RestartPolicy:                corev1.RestartPolicyNever,
			ActiveDeadlineSeconds:        ptr.To(int64(timeoutSec + padSec)),
			AutomountServiceAccountToken: ptr.To(false),
			EnableServiceLinks:           ptr.To(false),
			DNSPolicy:                    corev1.DNSNone,
			DNSConfig: &corev1.PodDNSConfig{
				Nameservers: []string{"127.0.0.1"},
				Options: []corev1.PodDNSConfigOption{
					{Name: "timeout", Value: ptr.To("1")},
					{Name: "attempts", Value: ptr.To("1")},
				},
			},
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   ptr.To(true),
				RunAsUser:      ptr.To(int64(65532)),
				RunAsGroup:     ptr.To(int64(65532)),
				FSGroup:        ptr.To(int64(65532)),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{
				{
					Name:            "loader",
					Image:           loaderImage,
					Command:         []string{"/usr/local/bin/loader", "wait"},
					SecurityContext: containerSecurity.DeepCopy(),
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("1"),
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "src", MountPath: "/workspace/src"},
						{Name: "review-in", MountPath: "/workspace/.review/in"},
						{Name: "review-out", MountPath: "/workspace/.review/out"},
						{Name: "loader-tmp", MountPath: "/tmp"},
					},
				},
				{
					Name:            "harness",
					Image:           harnessImage,
					Command:         []string{"review-kit", "wait"},
					SecurityContext: containerSecurity.DeepCopy(),
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "src", MountPath: "/workspace/src", ReadOnly: true},
						{Name: "review-in", MountPath: "/workspace/.review/in", ReadOnly: true},
						{Name: "review-out", MountPath: "/workspace/.review/out"},
						{Name: "scratch", MountPath: "/scratch"},
						{Name: "harness-tmp", MountPath: "/tmp"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{Name: "src", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(2<<30, resource.BinarySI)}}},
				{Name: "review-in", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(64<<20, resource.BinarySI)}}},
				{Name: "review-out", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(64<<20, resource.BinarySI)}}},
				{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(2<<30, resource.BinarySI)}}},
				{Name: "loader-tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(64<<20, resource.BinarySI)}}},
				{Name: "harness-tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(512<<20, resource.BinarySI)}}},
			},
		},
	}
}

func (r *K8sSandboxRunner) Run(ctx context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
	timeoutSec := spec.TimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = 900
	}
	kitTimeout := kitExecTimeout(timeoutSec)
	runCtx, cancel := context.WithTimeout(ctx, kitTimeout+180*time.Second)
	defer cancel()

	podName := PodNameFor(spec)
	pod := BuildHardenedPod(podName, spec, r.cfg)

	defer func() {
		delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer delCancel()
		err := r.clientset.CoreV1().Pods(r.cfg.Namespace).Delete(delCtx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: ptr.To(int64(0)),
		})
		if err != nil && !apierrors.IsNotFound(err) {
			_ = err
		}
	}()

	if _, err := r.clientset.CoreV1().Pods(r.cfg.Namespace).Create(runCtx, pod, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create sandbox pod: %w", err)
	}
	if err := r.waitRunning(runCtx, podName); err != nil {
		return nil, err
	}

	snapFile, err := os.Open(spec.TarballPath)
	if err != nil {
		return nil, fmt.Errorf("open snapshot tarball: %w", err)
	}
	defer snapFile.Close()
	if err := r.execStep(runCtx, podName, "loader", []string{"/usr/local/bin/loader", "unpack-src"}, 90*time.Second, snapFile, nil); err != nil {
		return nil, fmt.Errorf("unpack source snapshot: %w", err)
	}

	rewritten, err := rewriteTaskPaths(spec.Inputs, "/workspace/src", "/workspace/.review/in", "/workspace/.review/out")
	if err != nil {
		return nil, err
	}
	inputsTar, err := buildInputsTar(rewritten)
	if err != nil {
		return nil, err
	}
	if err := r.execStep(runCtx, podName, "loader", []string{"/usr/local/bin/loader", "prepare"}, 30*time.Second, bytes.NewReader(inputsTar), nil); err != nil {
		return nil, fmt.Errorf("prepare workspace: %w", err)
	}

	// key: stdin credential JSON field name; value: adapter base URL or ephemeral JWT task token
	secretJSON, err := json.Marshal(map[string]string{
		"openai_base_url": spec.AdapterURL,
		"openai_api_key":  spec.TaskToken,
	})
	if err != nil {
		return nil, err
	}
	if err := r.execStep(runCtx, podName, "harness", []string{"review-kit", "run", "--scratch-dir", "/scratch/home"}, kitTimeout, bytes.NewReader(secretJSON), nil); err != nil {
		return nil, fmt.Errorf("run harness kit: %w", err)
	}

	out := &cappedBuffer{max: maxReportReadBytes}
	if err := r.execStep(runCtx, podName, "loader", []string{"/usr/local/bin/loader", "read-report"}, 20*time.Second, nil, out); err != nil {
		return nil, fmt.Errorf("read report: %w", err)
	}
	return decodeAndValidateReport(out.Bytes())
}

func (r *K8sSandboxRunner) waitRunning(ctx context.Context, podName string) error {
	waitCtx, cancel := context.WithTimeout(ctx, r.cfg.WaitRunningTimeout)
	defer cancel()

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	var last *corev1.Pod
	for {
		pod, err := r.clientset.CoreV1().Pods(r.cfg.Namespace).Get(waitCtx, podName, metav1.GetOptions{})
		if err == nil {
			last = pod
			switch pod.Status.Phase {
			case corev1.PodRunning:
				return nil
			case corev1.PodFailed, corev1.PodSucceeded:
				return fmt.Errorf("sandbox pod %s terminated prematurely with phase %s", podName, pod.Status.Phase)
			}
		} else if waitCtx.Err() == nil && !apierrors.IsNotFound(err) {
			return err
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait sandbox pod %s running: %w%s", podName, waitCtx.Err(), podStuckReasons(last))
		case <-ticker.C:
		}
	}
}

func (r *K8sSandboxRunner) execStep(ctx context.Context, podName, container string, cmd []string, timeout time.Duration, stdin io.Reader, stdout io.Writer) error {
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if r.cfg.Executor != nil {
		return r.cfg.Executor(execCtx, podName, container, cmd, stdin, stdout)
	}

	req := r.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(r.cfg.Namespace).
		Name(podName).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   cmd,
			Stdin:     stdin != nil,
			Stdout:    stdout != nil,
			Stderr:    true,
		}, scheme.ParameterCodec)

	wsExec, err := remotecommand.NewWebSocketExecutor(r.restCfg, "GET", req.URL().String())
	if err != nil {
		return err
	}
	spdyExec, err := remotecommand.NewSPDYExecutor(r.restCfg, "POST", req.URL())
	if err != nil {
		return err
	}
	exec, err := remotecommand.NewFallbackExecutor(wsExec, spdyExec, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	if err != nil {
		return err
	}
	stderr := &stderrTail{max: 4096}
	streamErr := exec.StreamWithContext(execCtx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	})
	return withStderr(streamErr, stderr.String())
}

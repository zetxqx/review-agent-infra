package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/zetxqx/review-agent-infra/pkg/contract"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

var _ contract.SandboxRunner = (*K8sSandboxRunner)(nil)

var nonDNSLabel = regexp.MustCompile(`[^a-z0-9-]+`)

const (
	bootstrapMountDir = "/var/run/review-task"
	bootstrapFileName = "bootstrap.json"
)

type K8sConfig struct {
	Namespace                    string
	RuntimeClassName             string
	WaitRunningTimeout           time.Duration
	PollInterval                 time.Duration
	ActiveDeadlinePaddingSeconds int
	OwnerPod                     *corev1.Pod
}

type K8sSandboxRunner struct {
	clientset kubernetes.Interface
	cfg       K8sConfig
}

func NewK8sSandboxRunner(clientset kubernetes.Interface, cfg K8sConfig) *K8sSandboxRunner {
	if cfg.Namespace == "" {
		cfg.Namespace = "review-sandbox"
	}
	if cfg.RuntimeClassName == "" {
		cfg.RuntimeClassName = "gvisor"
	}
	if cfg.WaitRunningTimeout <= 0 {
		cfg.WaitRunningTimeout = 300 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.ActiveDeadlinePaddingSeconds <= 0 {
		cfg.ActiveDeadlinePaddingSeconds = 600
	}
	return &K8sSandboxRunner{
		clientset: clientset,
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
		sum := sha256.Sum256([]byte(name))
		name = strings.TrimRight(name[:54], "-") + "-" + hex.EncodeToString(sum[:])[:8]
	}
	return name
}

// Return map key: Kubernetes label name; value: label value.
func sandboxLabels(spec contract.SandboxSpec) map[string]string {
	role := spec.Role
	if role == "" {
		role = "primary"
	}
	// key: Kubernetes label name; value: label value
	return map[string]string{
		"app":            "review-sandbox",
		"review-run":     sanitizeDNSLabel(spec.ShortRunID),
		"review-role":    sanitizeDNSLabel(role),
		"review-profile": sanitizeDNSLabel(spec.HarnessProfile),
	}
}

func BuildBootstrapSecret(secretName string, spec contract.SandboxSpec, cfg K8sConfig) (*corev1.Secret, error) {
	ns := cfg.Namespace
	if ns == "" {
		ns = "review-sandbox"
	}
	// key: bootstrap JSON field name; value: controller sandbox URL or opaque task token
	payload, err := json.Marshal(map[string]string{
		"controller_url": spec.ControllerURL,
		"task_token":     spec.TaskToken,
	})
	if err != nil {
		return nil, err
	}
	// key: Secret data entry name; value: raw bootstrap JSON bytes
	data := map[string][]byte{
		bootstrapFileName: payload,
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: ns,
			Labels:    sandboxLabels(spec),
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}, nil
}

func BuildHardenedPod(podName, secretName string, spec contract.SandboxSpec, cfg K8sConfig) *corev1.Pod {
	ns := cfg.Namespace
	if ns == "" {
		ns = "review-sandbox"
	}
	if secretName == "" {
		secretName = podName
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
		padSec = 600
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
			Labels:          sandboxLabels(spec),
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
					Name:                     "sandbox",
					Image:                    harnessImage,
					Command:                  []string{"review-kit", "run", "--bootstrap", bootstrapMountDir + "/" + bootstrapFileName},
					TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: ptr.To(false),
						ReadOnlyRootFilesystem:   ptr.To(true),
						Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "workspace", MountPath: "/workspace"},
						{Name: "scratch", MountPath: "/scratch"},
						{Name: "tmp", MountPath: "/tmp"},
						{Name: "bootstrap", MountPath: bootstrapMountDir, ReadOnly: true},
					},
				},
			},
			Volumes: []corev1.Volume{
				{Name: "workspace", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(2<<30, resource.BinarySI)}}},
				{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(2<<30, resource.BinarySI)}}},
				{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(512<<20, resource.BinarySI)}}},
				{Name: "bootstrap", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName:  secretName,
					DefaultMode: ptr.To(int32(0o400)),
				}}},
			},
		},
	}
}

func ResolveSandboxURL(ctx context.Context, rawURL string) (string, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return "", nil
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse sandbox URL %q: %w", rawURL, err)
	}
	host := u.Hostname()
	if host == "" || net.ParseIP(host) != nil {
		return trimmed, nil
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(lookupCtx, host)
	if err != nil || len(addrs) == 0 {
		return "", fmt.Errorf("resolve controller hostname %q for DNS-disabled sandbox: %w", host, err)
	}

	selected := addrs[0].IP
	for _, addr := range addrs {
		if v4 := addr.IP.To4(); v4 != nil {
			selected = v4
			break
		}
	}

	if port := u.Port(); port != "" {
		u.Host = net.JoinHostPort(selected.String(), port)
	} else if selected.To4() == nil {
		u.Host = "[" + selected.String() + "]"
	} else {
		u.Host = selected.String()
	}
	return u.String(), nil
}

func (r *K8sSandboxRunner) Run(ctx context.Context, spec contract.SandboxSpec) error {
	timeoutSec := spec.TimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = 900
	}
	kitTimeout := kitExecTimeout(timeoutSec)
	runCtx, cancel := context.WithTimeout(ctx, kitTimeout+r.cfg.WaitRunningTimeout+120*time.Second)
	defer cancel()

	resolvedURL, err := ResolveSandboxURL(runCtx, spec.ControllerURL)
	if err != nil {
		return err
	}
	spec.ControllerURL = resolvedURL

	podName := PodNameFor(spec)
	secretName := podName

	secret, err := BuildBootstrapSecret(secretName, spec, r.cfg)
	if err != nil {
		return fmt.Errorf("build bootstrap secret: %w", err)
	}
	pod := BuildHardenedPod(podName, secretName, spec, r.cfg)

	var createdSecretUID, createdPodUID types.UID
	var secretCreated, podCreated bool
	defer func() {
		if !secretCreated && !podCreated {
			return
		}
		delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer delCancel()
		if podCreated {
			if delErr := r.clientset.CoreV1().Pods(r.cfg.Namespace).Delete(delCtx, podName, ownedDeleteOptions(createdPodUID)); delErr != nil && !apierrors.IsNotFound(delErr) {
				_ = delErr
			}
		}
		if secretCreated {
			if delErr := r.clientset.CoreV1().Secrets(r.cfg.Namespace).Delete(delCtx, secretName, ownedDeleteOptions(createdSecretUID)); delErr != nil && !apierrors.IsNotFound(delErr) {
				_ = delErr
			}
		}
	}()

	createdSecret, err := r.clientset.CoreV1().Secrets(r.cfg.Namespace).Create(runCtx, secret, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create sandbox bootstrap secret: %w", err)
	}
	secretCreated = true
	if createdSecret != nil {
		createdSecretUID = createdSecret.UID
	}

	createdPod, err := r.clientset.CoreV1().Pods(r.cfg.Namespace).Create(runCtx, pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create sandbox pod: %w", err)
	}
	podCreated = true
	if createdPod != nil {
		createdPodUID = createdPod.UID
	}

	if createdPod != nil && createdPod.UID != "" {
		ownerPatch, patchErr := json.Marshal(struct {
			Metadata struct {
				OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
			} `json:"metadata"`
		}{
			Metadata: struct {
				OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
			}{
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "Pod",
						Name:       createdPod.Name,
						UID:        createdPod.UID,
					},
				},
			},
		})
		if patchErr == nil {
			_, _ = r.clientset.CoreV1().Secrets(r.cfg.Namespace).Patch(runCtx, secretName, types.MergePatchType, ownerPatch, metav1.PatchOptions{})
		}
	}

	return r.waitCompletion(runCtx, podName, spec.TaskToken)
}

func ownedDeleteOptions(uid types.UID) metav1.DeleteOptions {
	opts := metav1.DeleteOptions{GracePeriodSeconds: ptr.To(int64(0))}
	if uid != "" {
		opts.Preconditions = &metav1.Preconditions{UID: ptr.To(uid)}
	}
	return opts
}

func (r *K8sSandboxRunner) waitCompletion(ctx context.Context, podName, taskToken string) error {
	pendingDeadline := time.Now().Add(r.cfg.WaitRunningTimeout)
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	var last *corev1.Pod
	for {
		pod, err := r.clientset.CoreV1().Pods(r.cfg.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err == nil {
			last = pod
			switch pod.Status.Phase {
			case corev1.PodSucceeded:
				return nil
			case corev1.PodFailed:
				return fmt.Errorf("sandbox pod %s failed%s", podName, podTerminatedReasons(pod, taskToken))
			case corev1.PodPending, "":
				if time.Now().After(pendingDeadline) {
					return fmt.Errorf("wait sandbox pod %s running: %w%s", podName, context.DeadlineExceeded, podStuckReasons(pod))
				}
			}
		} else if ctx.Err() == nil && !apierrors.IsNotFound(err) {
			return err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait sandbox pod %s completion: %w%s", podName, ctx.Err(), podStuckReasons(last))
		case <-ticker.C:
		}
	}
}

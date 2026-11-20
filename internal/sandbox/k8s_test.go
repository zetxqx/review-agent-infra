package sandbox

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestPodNameForShadowDisambiguation(t *testing.T) {
	p1 := PodNameFor(contract.SandboxSpec{ShortRunID: "run12345", ShardIndex: 0, Attempt: 0, Role: "primary", HarnessProfile: "openhands"})
	s1 := PodNameFor(contract.SandboxSpec{ShortRunID: "run12345", ShardIndex: 0, Attempt: 0, Role: "shadow", HarnessProfile: "codex"})
	s2 := PodNameFor(contract.SandboxSpec{ShortRunID: "run12345", ShardIndex: 0, Attempt: 0, Role: "shadow", HarnessProfile: "mini-swe-agent"})
	if p1 == s1 || s1 == s2 {
		t.Fatalf("pod names must be distinct: primary=%q s1=%q s2=%q", p1, s1, s2)
	}
	longName := PodNameFor(contract.SandboxSpec{
		ShortRunID:     strings.Repeat("a", 40),
		ShardIndex:     1,
		Attempt:        2,
		Role:           "shadow",
		HarnessProfile: strings.Repeat("b", 40),
	})
	if len(longName) > 63 {
		t.Fatalf("pod name exceeded 63 chars: %d %q", len(longName), longName)
	}
}

func TestBuildHardenedPodSecurityAndOwnerReference(t *testing.T) {
	spec := contract.SandboxSpec{
		ShortRunID:     "7f3a9c1e",
		HarnessProfile: "openhands",
		HarnessImage:   "ghcr.io/zetxqx/review-harness-openhands:1.0.0",
		TimeoutSeconds: 900,
	}

	crossNSOwner := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ctrl", Namespace: "review-system", UID: types.UID("uid-ctrl")},
	}
	pod := BuildHardenedPod("sbx-1", spec, K8sConfig{
		Namespace:                    "review-sandbox",
		OwnerPod:                     crossNSOwner,
		ActiveDeadlinePaddingSeconds: 300,
	})

	if pod.Namespace != "review-sandbox" || ptr.Deref(pod.Spec.RuntimeClassName, "") != "gvisor" {
		t.Fatalf("unexpected namespace/runtimeClass: %s %v", pod.Namespace, pod.Spec.RuntimeClassName)
	}
	if ptr.Deref(pod.Spec.AutomountServiceAccountToken, true) || ptr.Deref(pod.Spec.EnableServiceLinks, true) {
		t.Fatal("expected SA token mount and service links disabled")
	}
	if pod.Spec.DNSPolicy != corev1.DNSNone || len(pod.Spec.DNSConfig.Nameservers) != 1 || pod.Spec.DNSConfig.Nameservers[0] != "127.0.0.1" {
		t.Fatalf("unexpected DNS isolation: %+v %+v", pod.Spec.DNSPolicy, pod.Spec.DNSConfig)
	}
	if ptr.Deref(pod.Spec.ActiveDeadlineSeconds, 0) != 1200 {
		t.Fatalf("expected activeDeadlineSeconds=1200, got %v", pod.Spec.ActiveDeadlineSeconds)
	}
	if len(pod.OwnerReferences) != 0 {
		t.Fatalf("cross-namespace ownerReference must be omitted, got %+v", pod.OwnerReferences)
	}

	var loaderC, harnessC *corev1.Container
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.Name == "loader" {
			loaderC = c
		} else if c.Name == "harness" {
			harnessC = c
		}
		if !ptr.Deref(c.SecurityContext.ReadOnlyRootFilesystem, false) || ptr.Deref(c.SecurityContext.AllowPrivilegeEscalation, true) {
			t.Fatalf("container %s missing read-only rootfs or escalation lock", c.Name)
		}
	}
	if loaderC == nil || harnessC == nil {
		t.Fatal("expected both loader and harness containers")
	}
	for _, vm := range harnessC.VolumeMounts {
		if (vm.MountPath == "/workspace/src" || vm.MountPath == "/workspace/.review/in") && !vm.ReadOnly {
			t.Fatalf("harness mount %s must be ReadOnly", vm.MountPath)
		}
	}

	sameNSOwner := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ctrl", Namespace: "review-sandbox", UID: types.UID("uid-same")},
	}
	podSameNS := BuildHardenedPod("sbx-2", spec, K8sConfig{Namespace: "review-sandbox", OwnerPod: sameNSOwner})
	if len(podSameNS.OwnerReferences) != 1 || podSameNS.OwnerReferences[0].UID != types.UID("uid-same") {
		t.Fatalf("expected same-namespace controller Pod ownerReference, got %+v", podSameNS.OwnerReferences)
	}
}

func TestK8sSandboxRunnerFourStepExecAndCancellationCleanup(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		p := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		p.Status.Phase = corev1.PodRunning
		_ = cs.Tracker().Add(p)
		return true, p, nil
	})

	tarPath := filepath.Join(t.TempDir(), "snap.tar.gz")
	if err := os.WriteFile(tarPath, []byte("tgz-payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	taskBytes, err := protojson.Marshal(&reviewv1.AgentTask{
		RunId:   "run-1",
		Harness: &reviewv1.HarnessRef{Profile: "openhands", Adapter: "openhands"},
	})
	if err != nil {
		t.Fatal(err)
	}

	var calls []string
	var sawTokenInHarnessStdin bool
	var leakedTokenInPrepare bool
	reportOut, err := protojson.Marshal(&reviewv1.ReviewReport{
		Summary: "checked",
		Findings: []*reviewv1.ReviewFinding{
			{File: "a.go", Line: 10, Title: "race", Severity: reviewv1.Severity_SEVERITY_HIGH},
		},
		Provenance: &reviewv1.Provenance{Status: reviewv1.ReportStatus_REPORT_STATUS_OK},
	})
	if err != nil {
		t.Fatal(err)
	}

	runner := NewK8sSandboxRunner(cs, nil, K8sConfig{
		Namespace:          "review-sandbox",
		WaitRunningTimeout: 5 * time.Second,
		PollInterval:       5 * time.Millisecond,
		Executor: func(_ context.Context, _, container string, cmd []string, stdin io.Reader, stdout io.Writer) error {
			calls = append(calls, container+":"+strings.Join(cmd, " "))
			switch cmd[1] {
			case "unpack-src":
				b, _ := io.ReadAll(stdin)
				if string(b) != "tgz-payload" {
					return errors.New("unexpected snapshot stream")
				}
			case "prepare":
				b, _ := io.ReadAll(stdin)
				if bytes.Contains(b, []byte("jwt-secret-token")) {
					leakedTokenInPrepare = true
				}
				tr := tar.NewReader(bytes.NewReader(b))
				if hdr, err := tr.Next(); err != nil || hdr == nil {
					return errors.New("empty inputs tar")
				}
			case "run":
				b, _ := io.ReadAll(stdin)
				// key: decoded credential field name; value: decoded value
				var creds map[string]string
				if err := json.Unmarshal(b, &creds); err != nil {
					return err
				}
				sawTokenInHarnessStdin = creds["openai_api_key"] == "jwt-secret-token" && creds["openai_base_url"] == "http://10.96.0.42:8080/v1"
			case "read-report":
				_, err := stdout.Write(reportOut)
				return err
			}
			return nil
		},
	})

	// key: file path relative to /workspace/.review/in; value: raw file bytes
	inputs := map[string][]byte{
		"task.json":  taskBytes,
		"diff.patch": []byte("diff --git a/a.go b/a.go\n"),
	}
	spec := contract.SandboxSpec{
		ShortRunID:     "7f3a9c1e",
		ShardIndex:     0,
		Attempt:        0,
		Role:           "primary",
		HarnessProfile: "openhands",
		HarnessImage:   "ghcr.io/zetxqx/review-harness-openhands:1.0.0",
		TarballPath:    tarPath,
		Inputs:         inputs,
		AdapterURL:     "http://10.96.0.42:8080/v1",
		TaskToken:      "jwt-secret-token",
		TimeoutSeconds: 60,
	}

	report, err := runner.Run(context.Background(), spec)
	if err != nil {
		t.Fatalf("k8s runner run failed: %v", err)
	}
	if len(calls) != 4 || !sawTokenInHarnessStdin || leakedTokenInPrepare || len(report.Findings) != 1 {
		t.Fatalf("unexpected execution state: calls=%v token=%v leaked=%v report=%+v", calls, sawTokenInHarnessStdin, leakedTokenInPrepare, report)
	}
	if _, err := cs.CoreV1().Pods("review-sandbox").Get(context.Background(), PodNameFor(spec), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected pod deleted after run, got err=%v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelRunner := NewK8sSandboxRunner(cs, nil, K8sConfig{
		Namespace:          "review-sandbox",
		WaitRunningTimeout: 5 * time.Second,
		PollInterval:       5 * time.Millisecond,
		Executor: func(ctx context.Context, _, _ string, cmd []string, _ io.Reader, _ io.Writer) error {
			if cmd[1] == "run" {
				cancel()
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	})
	if _, err := cancelRunner.Run(cancelCtx, spec); err == nil {
		t.Fatal("expected cancellation error")
	}
	if _, err := cs.CoreV1().Pods("review-sandbox").Get(context.Background(), PodNameFor(spec), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected pod deleted via WithoutCancel even after ctx cancellation, got err=%v", err)
	}
}

func TestK8sSandboxRunnerGrantsKitFinalizeGraceBeyondWallClock(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		p := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		p.Status.Phase = corev1.PodRunning
		_ = cs.Tracker().Add(p)
		return true, p, nil
	})

	tarPath := filepath.Join(t.TempDir(), "snap.tar.gz")
	if err := os.WriteFile(tarPath, []byte("tgz-payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	taskBytes, err := protojson.Marshal(&reviewv1.AgentTask{
		RunId:   "run-grace",
		Harness: &reviewv1.HarnessRef{Profile: "openhands", Adapter: "openhands"},
	})
	if err != nil {
		t.Fatal(err)
	}
	reportOut, err := protojson.Marshal(&reviewv1.ReviewReport{
		Provenance: &reviewv1.Provenance{Status: reviewv1.ReportStatus_REPORT_STATUS_HARNESS_FAILED},
	})
	if err != nil {
		t.Fatal(err)
	}

	var kitBudget time.Duration
	runner := NewK8sSandboxRunner(cs, nil, K8sConfig{
		Namespace:          "review-sandbox",
		WaitRunningTimeout: 5 * time.Second,
		PollInterval:       5 * time.Millisecond,
		Executor: func(ctx context.Context, _, _ string, cmd []string, _ io.Reader, stdout io.Writer) error {
			switch cmd[1] {
			case "run":
				deadline, ok := ctx.Deadline()
				if !ok {
					return errors.New("kit exec context has no deadline")
				}
				kitBudget = time.Until(deadline)
			case "read-report":
				_, err := stdout.Write(reportOut)
				return err
			}
			return nil
		},
	})

	const wallClock = 60 * time.Second
	if _, err := runner.Run(context.Background(), contract.SandboxSpec{
		ShortRunID:     "9b2d4f10",
		Role:           "primary",
		HarnessProfile: "openhands",
		TarballPath:    tarPath,
		// key: file path relative to /workspace/.review/in; value: raw file bytes
		Inputs: map[string][]byte{
			"task.json":  taskBytes,
			"diff.patch": []byte("diff --git a/a.go b/a.go\n"),
		},
		AdapterURL:     "http://10.96.0.42:8080/v1",
		TaskToken:      "jwt-secret-token",
		TimeoutSeconds: int(wallClock / time.Second),
	}); err != nil {
		t.Fatalf("k8s runner run failed: %v", err)
	}
	if kitBudget <= wallClock || kitBudget > wallClock+kitFinalizeGrace {
		t.Fatalf("kit exec budget = %v, want within (%v, %v]", kitBudget, wallClock, wallClock+kitFinalizeGrace)
	}
}

func TestOrphanPodReaper(t *testing.T) {
	now := time.Now().UTC()
	cs := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "failed-expired",
				Namespace:         "review-sandbox",
				CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
				// key: Kubernetes Pod label name; value: label value
				Labels: map[string]string{"app": "review-sandbox"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodFailed},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "stuck-running-old",
				Namespace:         "review-sandbox",
				CreationTimestamp: metav1.NewTime(now.Add(-30 * time.Minute)),
				// key: Kubernetes Pod label name; value: label value
				Labels: map[string]string{"app": "review-sandbox"},
			},
			Spec:   corev1.PodSpec{ActiveDeadlineSeconds: ptr.To(int64(900))},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "active-running",
				Namespace:         "review-sandbox",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Minute)),
				// key: Kubernetes Pod label name; value: label value
				Labels: map[string]string{"app": "review-sandbox"},
			},
			Spec:   corev1.PodSpec{ActiveDeadlineSeconds: ptr.To(int64(900))},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
	)

	reaper := NewOrphanPodReaper(cs, "review-sandbox", 5*time.Minute)
	reaper.now = func() time.Time { return now }

	deleted, err := reaper.ReapOnce(context.Background())
	if err != nil {
		t.Fatalf("reap failed: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 reaped pods, got %d", deleted)
	}
	remaining, err := cs.CoreV1().Pods("review-sandbox").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(remaining.Items) != 1 || remaining.Items[0].Name != "active-running" {
		t.Fatalf("expected only active-running left, got %+v err=%v", remaining.Items, err)
	}
}

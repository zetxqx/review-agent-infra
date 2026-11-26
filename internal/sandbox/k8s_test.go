package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/zetxqx/review-agent-infra/pkg/contract"
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

func TestBuildHardenedPodSingleContainerSecurityAndSecretMount(t *testing.T) {
	spec := contract.SandboxSpec{
		ShortRunID:     "7f3a9c1e",
		HarnessProfile: "openhands",
		HarnessImage:   "ghcr.io/zetxqx/review-harness-openhands:1.0.0",
		ControllerURL:  "http://10.96.0.42:8081",
		TaskToken:      "opaque-token",
		TimeoutSeconds: 900,
	}

	crossNSOwner := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ctrl", Namespace: "review-system", UID: types.UID("uid-ctrl")},
	}
	pod := BuildHardenedPod("sbx-1", "sbx-1", spec, K8sConfig{
		Namespace:                    "review-sandbox",
		OwnerPod:                     crossNSOwner,
		ActiveDeadlinePaddingSeconds: 300,
	})

	if pod.Namespace != "review-sandbox" || ptr.Deref(pod.Spec.RuntimeClassName, "") != "gvisor" {
		t.Fatalf("unexpected namespace/runtimeClass: %s %v", pod.Namespace, pod.Spec.RuntimeClassName)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("expected RestartPolicy=Never, got %s", pod.Spec.RestartPolicy)
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
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("expected exactly 1 sandbox container, got %d", len(pod.Spec.Containers))
	}
	c := pod.Spec.Containers[0]
	if c.Name != "sandbox" {
		t.Fatalf("expected container name sandbox, got %q", c.Name)
	}
	if c.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
		t.Fatalf("expected FallbackToLogsOnError, got %q", c.TerminationMessagePolicy)
	}
	if !ptr.Deref(c.SecurityContext.ReadOnlyRootFilesystem, false) || ptr.Deref(c.SecurityContext.AllowPrivilegeEscalation, true) {
		t.Fatalf("container %s missing read-only rootfs or escalation lock", c.Name)
	}
	wantCmd := "review-kit run --bootstrap /var/run/review-task/bootstrap.json"
	if strings.Join(c.Command, " ") != wantCmd {
		t.Fatalf("container command = %q, want %q", strings.Join(c.Command, " "), wantCmd)
	}

	var sawBootstrapMount bool
	for _, vm := range c.VolumeMounts {
		if vm.MountPath == "/var/run/review-task" {
			sawBootstrapMount = vm.ReadOnly && vm.Name == "bootstrap"
		}
	}
	if !sawBootstrapMount {
		t.Fatalf("missing read-only /var/run/review-task mount: %+v", c.VolumeMounts)
	}

	var sawSecretVol bool
	for _, v := range pod.Spec.Volumes {
		if v.Name == "bootstrap" && v.Secret != nil && v.Secret.SecretName == "sbx-1" && ptr.Deref(v.Secret.DefaultMode, 0) == 0o400 {
			sawSecretVol = true
		}
	}
	if !sawSecretVol {
		t.Fatalf("missing 0400 bootstrap secret volume: %+v", pod.Spec.Volumes)
	}

	sameNSOwner := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ctrl", Namespace: "review-sandbox", UID: types.UID("uid-same")},
	}
	podSameNS := BuildHardenedPod("sbx-2", "sbx-2", spec, K8sConfig{Namespace: "review-sandbox", OwnerPod: sameNSOwner})
	if len(podSameNS.OwnerReferences) != 1 || podSameNS.OwnerReferences[0].UID != types.UID("uid-same") {
		t.Fatalf("expected same-namespace controller Pod ownerReference, got %+v", podSameNS.OwnerReferences)
	}
}

func TestK8sSandboxRunnerCreatesSecretPatchesOwnerResolvesIPAndCleansUp(t *testing.T) {
	cs := fake.NewSimpleClientset()
	var createdSecretData []byte
	var patchedOwner bool

	cs.PrependReactor("create", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		s := action.(k8stesting.CreateAction).GetObject().(*corev1.Secret).DeepCopy()
		createdSecretData = append([]byte(nil), s.Data["bootstrap.json"]...)
		_ = cs.Tracker().Add(s)
		return true, s, nil
	})
	cs.PrependReactor("patch", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pa := action.(k8stesting.PatchAction)
		if strings.Contains(string(pa.GetPatch()), "pod-uid-123") {
			patchedOwner = true
		}
		return false, nil, nil
	})
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		p := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		p.UID = types.UID("pod-uid-123")
		p.Status.Phase = corev1.PodSucceeded
		_ = cs.Tracker().Add(p)
		return true, p, nil
	})

	runner := NewK8sSandboxRunner(cs, K8sConfig{
		Namespace:          "review-sandbox",
		WaitRunningTimeout: 5 * time.Second,
		PollInterval:       5 * time.Millisecond,
	})

	spec := contract.SandboxSpec{
		ShortRunID:     "7f3a9c1e",
		ShardIndex:     0,
		Attempt:        0,
		Role:           "primary",
		HarnessProfile: "openhands",
		HarnessImage:   "ghcr.io/zetxqx/review-harness-openhands:1.0.0",
		ControllerURL:  "http://localhost:8081",
		TaskToken:      "opaque-secret-token",
		TimeoutSeconds: 60,
	}

	if err := runner.Run(context.Background(), spec); err != nil {
		t.Fatalf("k8s runner run failed: %v", err)
	}
	if !patchedOwner {
		t.Fatal("expected bootstrap secret OwnerReferences patched with Pod UID")
	}

	// key: bootstrap JSON field name; value: decoded value
	var boot map[string]string
	if err := json.Unmarshal(createdSecretData, &boot); err != nil {
		t.Fatalf("unmarshal bootstrap secret: %v", err)
	}
	parsedURL, err := url.Parse(boot["controller_url"])
	if err != nil || net.ParseIP(parsedURL.Hostname()) == nil || parsedURL.Port() != "8081" {
		t.Fatalf("expected IP-resolved controller_url with port 8081 in secret, got %q", boot["controller_url"])
	}
	if boot["task_token"] != "opaque-secret-token" {
		t.Fatalf("unexpected task_token in bootstrap secret: %+v", boot)
	}

	podName := PodNameFor(spec)
	if _, err := cs.CoreV1().Pods("review-sandbox").Get(context.Background(), podName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected pod deleted after run, got err=%v", err)
	}
	if _, err := cs.CoreV1().Secrets("review-sandbox").Get(context.Background(), podName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected secret deleted after run, got err=%v", err)
	}
}

func TestK8sSandboxRunnerSurfacesPodFailureAndCleansUpOnCancel(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		p := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		p.UID = types.UID("pod-uid-oom")
		p.Status.Phase = corev1.PodFailed
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "sandbox",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 137,
				Reason:   "OOMKilled",
				Message:  "killed while carrying opaque-secret-token",
			}},
		}}
		_ = cs.Tracker().Add(p)
		return true, p, nil
	})

	runner := NewK8sSandboxRunner(cs, K8sConfig{
		Namespace:          "review-sandbox",
		WaitRunningTimeout: 5 * time.Second,
		PollInterval:       5 * time.Millisecond,
	})
	spec := contract.SandboxSpec{
		ShortRunID:     "oom001",
		Role:           "primary",
		HarnessProfile: "openhands",
		ControllerURL:  "http://10.96.0.42:8081",
		TaskToken:      "opaque-secret-token",
		TimeoutSeconds: 60,
	}
	err := runner.Run(context.Background(), spec)
	if err == nil || !strings.Contains(err.Error(), "OOMKilled") || strings.Contains(err.Error(), "opaque-secret-token") {
		t.Fatalf("expected OOMKilled error with redacted token, got %v", err)
	}

	csCancel := fake.NewSimpleClientset()
	csCancel.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		p := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		p.UID = types.UID("pod-uid-cancel")
		p.Status.Phase = corev1.PodRunning
		_ = csCancel.Tracker().Add(p)
		return true, p, nil
	})
	cancelCtx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancel()
	}()
	cancelRunner := NewK8sSandboxRunner(csCancel, K8sConfig{
		Namespace:          "review-sandbox",
		WaitRunningTimeout: 5 * time.Second,
		PollInterval:       5 * time.Millisecond,
	})
	if err := cancelRunner.Run(cancelCtx, spec); err == nil {
		t.Fatal("expected cancellation error")
	}
	podName := PodNameFor(spec)
	if _, err := csCancel.CoreV1().Pods("review-sandbox").Get(context.Background(), podName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected pod deleted via WithoutCancel after cancellation, got err=%v", err)
	}
	if _, err := csCancel.CoreV1().Secrets("review-sandbox").Get(context.Background(), podName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected secret deleted via WithoutCancel after cancellation, got err=%v", err)
	}
}

func TestBuildHardenedPodDefaults(t *testing.T) {
	pod := BuildHardenedPod("review-sbx-defaults", "review-sbx-defaults", contract.SandboxSpec{
		ShortRunID:     "d3f4u17",
		Role:           "primary",
		HarnessProfile: "openhands",
		HarnessImage:   "registry.example/sandbox:1",
		TimeoutSeconds: 1800,
	}, K8sConfig{})
	if got := ptr.Deref(pod.Spec.ActiveDeadlineSeconds, 0); got != 2400 {
		t.Fatalf("expected activeDeadlineSeconds=2400 for an 1800 s wall clock, got %d", got)
	}
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(pod.Spec.Containers))
	}
	c := pod.Spec.Containers[0]
	got := [4]string{
		c.Resources.Requests.Cpu().String(),
		c.Resources.Requests.Memory().String(),
		c.Resources.Limits.Cpu().String(),
		c.Resources.Limits.Memory().String(),
	}
	want := [4]string{"500m", "1Gi", "2", "4Gi"}
	if got != want {
		t.Fatalf("container %s resources (cpu req, mem req, cpu lim, mem lim) = %v, want %v", c.Name, got, want)
	}
}

func TestOrphanPodReaperSweepsPodsAndOrphanSecrets(t *testing.T) {
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
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "active-running",
				Namespace:         "review-sandbox",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Minute)),
				// key: Kubernetes Secret label name; value: label value
				Labels: map[string]string{"app": "review-sandbox"},
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "v1", Kind: "Pod", Name: "active-running", UID: types.UID("u-active")},
				},
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "orphan-owned-secret",
				Namespace:         "review-sandbox",
				CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
				// key: Kubernetes Secret label name; value: label value
				Labels: map[string]string{"app": "review-sandbox"},
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "v1", Kind: "Pod", Name: "failed-expired", UID: types.UID("u-dead")},
				},
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "orphan-unowned-old",
				Namespace:         "review-sandbox",
				CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Minute)),
				// key: Kubernetes Secret label name; value: label value
				Labels: map[string]string{"app": "review-sandbox"},
			},
		},
	)

	reaper := NewOrphanPodReaper(cs, "review-sandbox", 5*time.Minute)
	reaper.now = func() time.Time { return now }

	deleted, err := reaper.ReapOnce(context.Background())
	if err != nil {
		t.Fatalf("reap failed: %v", err)
	}
	if deleted != 4 {
		t.Fatalf("expected 4 reaped resources (2 pods + 2 orphan secrets), got %d", deleted)
	}
	remainingPods, err := cs.CoreV1().Pods("review-sandbox").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(remainingPods.Items) != 1 || remainingPods.Items[0].Name != "active-running" {
		t.Fatalf("expected only active-running pod left, got %+v err=%v", remainingPods.Items, err)
	}
	remainingSecrets, err := cs.CoreV1().Secrets("review-sandbox").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(remainingSecrets.Items) != 1 || remainingSecrets.Items[0].Name != "active-running" {
		t.Fatalf("expected only active-running secret left, got %+v err=%v", remainingSecrets.Items, err)
	}
}

func TestPodNameForLongProfilesKeepsDistinctHashedNames(t *testing.T) {
	prefix := strings.Repeat("p", 50)
	a := PodNameFor(contract.SandboxSpec{ShortRunID: "run12345", Role: "shadow", HarnessProfile: prefix + "-alpha"})
	b := PodNameFor(contract.SandboxSpec{ShortRunID: "run12345", Role: "shadow", HarnessProfile: prefix + "-bravo"})
	if a == b {
		t.Fatalf("long shadow profiles collided: %q", a)
	}
	for _, name := range []string{a, b} {
		if len(name) > 63 || strings.Trim(name, "-") != name || nonDNSLabel.MatchString(name) {
			t.Fatalf("invalid DNS label %q", name)
		}
	}
	if again := PodNameFor(contract.SandboxSpec{ShortRunID: "run12345", Role: "shadow", HarnessProfile: prefix + "-alpha"}); again != a {
		t.Fatalf("pod name not deterministic: %q vs %q", a, again)
	}
}

func TestK8sSandboxRunnerCreateConflictLeavesSiblingResources(t *testing.T) {
	spec := contract.SandboxSpec{
		ShortRunID:     "dup001",
		Role:           "primary",
		HarnessProfile: "openhands",
		ControllerURL:  "http://10.96.0.42:8081",
		TaskToken:      "opaque-secret-token",
		TimeoutSeconds: 60,
	}
	name := PodNameFor(spec)
	// key: Kubernetes label name; value: label value
	labels := map[string]string{"app": "review-sandbox"}
	cs := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "review-sandbox", UID: types.UID("sibling-pod"), Labels: labels}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "review-sandbox", UID: types.UID("sibling-secret"), Labels: labels}},
	)
	runner := NewK8sSandboxRunner(cs, K8sConfig{Namespace: "review-sandbox", WaitRunningTimeout: time.Second, PollInterval: 5 * time.Millisecond})
	err := runner.Run(context.Background(), spec)
	if err == nil || !apierrors.IsAlreadyExists(errors.Unwrap(err)) {
		t.Fatalf("expected AlreadyExists create error, got %v", err)
	}
	if _, err := cs.CoreV1().Pods("review-sandbox").Get(context.Background(), name, metav1.GetOptions{}); err != nil {
		t.Fatalf("sibling pod must survive a conflicting run: %v", err)
	}
	if _, err := cs.CoreV1().Secrets("review-sandbox").Get(context.Background(), name, metav1.GetOptions{}); err != nil {
		t.Fatalf("sibling secret must survive a conflicting run: %v", err)
	}
	for _, action := range cs.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatalf("conflicting run must not delete anything, saw %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}

	csPodConflict := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "review-sandbox", UID: types.UID("sibling-pod"), Labels: labels}},
	)
	podConflictRunner := NewK8sSandboxRunner(csPodConflict, K8sConfig{Namespace: "review-sandbox", WaitRunningTimeout: time.Second, PollInterval: 5 * time.Millisecond})
	if err := podConflictRunner.Run(context.Background(), spec); err == nil {
		t.Fatal("expected pod create conflict error")
	}
	if _, err := csPodConflict.CoreV1().Pods("review-sandbox").Get(context.Background(), name, metav1.GetOptions{}); err != nil {
		t.Fatalf("sibling pod must survive a pod create conflict: %v", err)
	}
	if _, err := csPodConflict.CoreV1().Secrets("review-sandbox").Get(context.Background(), name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("secret created by the failing run must be cleaned up, got err=%v", err)
	}
}

func TestK8sSandboxRunnerDeletesPodWithUIDPrecondition(t *testing.T) {
	cs := fake.NewSimpleClientset()
	var podDeleteUID types.UID
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		p := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		p.UID = types.UID("pod-uid-precond")
		p.Status.Phase = corev1.PodSucceeded
		_ = cs.Tracker().Add(p)
		return true, p, nil
	})
	cs.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		opts := action.(k8stesting.DeleteActionImpl).DeleteOptions
		if opts.Preconditions != nil && opts.Preconditions.UID != nil {
			podDeleteUID = *opts.Preconditions.UID
		}
		return false, nil, nil
	})
	runner := NewK8sSandboxRunner(cs, K8sConfig{Namespace: "review-sandbox", WaitRunningTimeout: time.Second, PollInterval: 5 * time.Millisecond})
	if err := runner.Run(context.Background(), contract.SandboxSpec{
		ShortRunID:     "uid001",
		Role:           "primary",
		HarnessProfile: "openhands",
		ControllerURL:  "http://10.96.0.42:8081",
		TaskToken:      "opaque-secret-token",
		TimeoutSeconds: 60,
	}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if podDeleteUID != "pod-uid-precond" {
		t.Fatalf("expected pod delete precondition UID pod-uid-precond, got %q", podDeleteUID)
	}
}

func TestOrphanPodReaperKeepsSecretOfPodCreatedBetweenLists(t *testing.T) {
	now := time.Now().UTC()
	cs := fake.NewSimpleClientset()
	var raced bool
	cs.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		snapshot, err := cs.Tracker().List(corev1.SchemeGroupVersion.WithResource("pods"), corev1.SchemeGroupVersion.WithKind("Pod"), "review-sandbox")
		if err != nil {
			return true, nil, err
		}
		if !raced {
			raced = true
			// key: Kubernetes label name; value: label value
			labels := map[string]string{"app": "review-sandbox"}
			_ = cs.Tracker().Add(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "fresh", Namespace: "review-sandbox", UID: types.UID("u-fresh"), CreationTimestamp: metav1.NewTime(now), Labels: labels},
				Status:     corev1.PodStatus{Phase: corev1.PodPending},
			})
			_ = cs.Tracker().Add(&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "fresh",
					Namespace:         "review-sandbox",
					CreationTimestamp: metav1.NewTime(now),
					Labels:            labels,
					OwnerReferences:   []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "fresh", UID: types.UID("u-fresh")}},
				},
			})
		}
		return true, snapshot, nil
	})

	reaper := NewOrphanPodReaper(cs, "review-sandbox", 5*time.Minute)
	reaper.now = func() time.Time { return now }
	if _, err := reaper.ReapOnce(context.Background()); err != nil {
		t.Fatalf("reap failed: %v", err)
	}
	if _, err := cs.CoreV1().Secrets("review-sandbox").Get(context.Background(), "fresh", metav1.GetOptions{}); err != nil {
		t.Fatalf("bootstrap secret of a pod created between lists must survive: %v", err)
	}
}

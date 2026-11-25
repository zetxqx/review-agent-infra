package sandbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zetxqx/review-agent-infra/pkg/contract"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestStderrTailKeepsLastBytes(t *testing.T) {
	tail := &stderrTail{max: 16}
	_, _ = tail.Write([]byte("0123456789"))
	_, _ = tail.Write([]byte("abcdefghij"))
	if got := tail.String(); got != "456789abcdefghij" {
		t.Fatalf("stderr tail = %q, want the last 16 bytes", got)
	}
}

func TestWithStderrAppendsRedactedTail(t *testing.T) {
	base := errors.New("command terminated with exit code 1")
	got := withStderr(base, "Traceback: bad token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.c2lnbmF0dXJl here\n")
	if !errors.Is(got, base) {
		t.Fatalf("expected the original error to stay wrapped, got %v", got)
	}
	msg := got.Error()
	if !strings.Contains(msg, "exit code 1") || !strings.Contains(msg, "stderr:") || !strings.Contains(msg, "[redacted]") || strings.Contains(msg, "eyJhbGci") {
		t.Fatalf("unexpected error message: %q", msg)
	}
	cut := withStderr(base, "tail starts mid-token: eyJzdWIiOiJ4In0.c2lnbmF0dXJl").Error()
	if strings.Contains(cut, "eyJzdWIi") || strings.Contains(cut, "c2lnbmF0dXJl") {
		t.Fatalf("expected a token cut at the tail boundary to be redacted, got %q", cut)
	}
	if same := withStderr(base, " \n"); same != base {
		t.Fatalf("expected the error unchanged when stderr is blank, got %v", same)
	}
	if withStderr(nil, "noise") != nil {
		t.Fatal("expected nil when there is no error")
	}
}

func TestWaitRunningReportsWhyThePodIsStuck(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		p := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		p.Status.Phase = corev1.PodPending
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "harness",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason:  "ImagePullBackOff",
				Message: "Back-off pulling image registry.example/sandbox:missing",
			}},
		}}
		_ = cs.Tracker().Add(p)
		return true, p, nil
	})
	runner := NewK8sSandboxRunner(cs, nil, K8sConfig{
		Namespace:          "review-sandbox",
		WaitRunningTimeout: 100 * time.Millisecond,
		PollInterval:       10 * time.Millisecond,
	})
	_, err := runner.Run(context.Background(), contract.SandboxSpec{
		ShortRunID:     "57uck000",
		Role:           "primary",
		HarnessProfile: "openhands",
		TarballPath:    "/nonexistent",
		TimeoutSeconds: 60,
	})
	if err == nil || !strings.Contains(err.Error(), "harness") || !strings.Contains(err.Error(), "ImagePullBackOff") {
		t.Fatalf("expected the timeout error to name the harness container and ImagePullBackOff, got %v", err)
	}
}

package sandbox

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
)

var jwtLikePattern = regexp.MustCompile(`eyJ[A-Za-z0-9_-]{6,}(?:\.[A-Za-z0-9_-]+)*`)

type stderrTail struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (s *stderrTail) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = append(s.buf, p...)
	if over := len(s.buf) - s.max; over > 0 {
		s.buf = append([]byte(nil), s.buf[over:]...)
	}
	return len(p), nil
}

func (s *stderrTail) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.buf)
}

func withStderr(err error, stderr string) error {
	if err == nil {
		return nil
	}
	trimmed := strings.TrimSpace(stderr)
	if trimmed == "" {
		return err
	}
	return fmt.Errorf("%w (stderr: %s)", err, jwtLikePattern.ReplaceAllString(trimmed, "[redacted]"))
}

func podStuckReasons(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	var parts []string
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse {
			parts = append(parts, fmt.Sprintf("unscheduled: %s: %s", c.Reason, c.Message))
		}
	}
	statuses := append(append([]corev1.ContainerStatus(nil), pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...)
	for _, cs := range statuses {
		if w := cs.State.Waiting; w != nil && w.Reason != "" {
			parts = append(parts, fmt.Sprintf("%s: %s: %s", cs.Name, w.Reason, w.Message))
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf(" (phase %s)", pod.Status.Phase)
	}
	return " (" + strings.Join(parts, "; ") + ")"
}

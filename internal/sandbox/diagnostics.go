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

func redactOutput(text string, secretTokens ...string) string {
	out := strings.TrimSpace(text)
	for _, tok := range secretTokens {
		if tok != "" {
			out = strings.ReplaceAll(out, tok, "[redacted]")
		}
	}
	return jwtLikePattern.ReplaceAllString(out, "[redacted]")
}

func withStderr(err error, stderr string, secretTokens ...string) error {
	if err == nil {
		return nil
	}
	trimmed := redactOutput(stderr, secretTokens...)
	if trimmed == "" {
		return err
	}
	return fmt.Errorf("%w (stderr: %s)", err, trimmed)
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

func podTerminatedReasons(pod *corev1.Pod, secretTokens ...string) string {
	if pod == nil {
		return ""
	}
	var parts []string
	if pod.Status.Reason != "" {
		msg := redactOutput(pod.Status.Message, secretTokens...)
		if msg != "" {
			parts = append(parts, fmt.Sprintf("pod: %s: %s", pod.Status.Reason, msg))
		} else {
			parts = append(parts, fmt.Sprintf("pod: %s", pod.Status.Reason))
		}
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if term := cs.State.Terminated; term != nil {
			detail := fmt.Sprintf("%s: exit=%d", cs.Name, term.ExitCode)
			if term.Reason != "" {
				detail += " reason=" + term.Reason
			}
			if msg := redactOutput(term.Message, secretTokens...); msg != "" {
				detail += " message=" + msg
			}
			parts = append(parts, detail)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + strings.Join(parts, "; ") + ")"
}

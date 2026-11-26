package sandbox

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

type OrphanPodReaper struct {
	clientset      kubernetes.Interface
	namespace      string
	graceAfterTerm time.Duration
	fallbackMaxAge time.Duration
	now            func() time.Time
}

func NewOrphanPodReaper(clientset kubernetes.Interface, namespace string, graceAfterTerm time.Duration) *OrphanPodReaper {
	if namespace == "" {
		namespace = "review-sandbox"
	}
	if graceAfterTerm <= 0 {
		graceAfterTerm = 5 * time.Minute
	}
	return &OrphanPodReaper{
		clientset:      clientset,
		namespace:      namespace,
		graceAfterTerm: graceAfterTerm,
		fallbackMaxAge: 30 * time.Minute,
		now:            time.Now,
	}
}

func (r *OrphanPodReaper) shouldReap(pod corev1.Pod, ts time.Time) bool {
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return true
	}
	if pod.CreationTimestamp.IsZero() {
		return false
	}
	maxAge := r.fallbackMaxAge
	if pod.Spec.ActiveDeadlineSeconds != nil && *pod.Spec.ActiveDeadlineSeconds > 0 {
		maxAge = time.Duration(*pod.Spec.ActiveDeadlineSeconds)*time.Second + r.graceAfterTerm
	}
	return ts.Sub(pod.CreationTimestamp.Time) > maxAge
}

// survivingPods key: surviving sandbox Pod name; value: true when Pod remains active.
func (r *OrphanPodReaper) shouldReapSecret(secret corev1.Secret, survivingPods map[string]bool, ts time.Time) bool {
	if survivingPods[secret.Name] {
		return false
	}
	for _, ref := range secret.OwnerReferences {
		if ref.Kind == "Pod" && survivingPods[ref.Name] {
			return false
		}
	}
	if len(secret.OwnerReferences) > 0 {
		return true
	}
	if secret.CreationTimestamp.IsZero() {
		return false
	}
	return ts.Sub(secret.CreationTimestamp.Time) > r.graceAfterTerm
}

func (r *OrphanPodReaper) ReapOnce(ctx context.Context) (int, error) {
	secrets, err := r.clientset.CoreV1().Secrets(r.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=review-sandbox",
	})
	if err != nil {
		return 0, err
	}
	pods, err := r.clientset.CoreV1().Pods(r.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=review-sandbox",
	})
	if err != nil {
		return 0, err
	}
	ts := r.now()
	deleted := 0
	// key: surviving sandbox Pod name; value: true when Pod remains active
	survivingPods := make(map[string]bool, len(pods.Items))
	for _, pod := range pods.Items {
		if !r.shouldReap(pod, ts) {
			survivingPods[pod.Name] = true
			continue
		}
		delErr := r.clientset.CoreV1().Pods(r.namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
			GracePeriodSeconds: ptr.To(int64(0)),
		})
		if delErr == nil || apierrors.IsNotFound(delErr) {
			if delErr == nil {
				deleted++
			}
			continue
		}
		return deleted, delErr
	}

	for _, secret := range secrets.Items {
		if !r.shouldReapSecret(secret, survivingPods, ts) {
			continue
		}
		delErr := r.clientset.CoreV1().Secrets(r.namespace).Delete(ctx, secret.Name, metav1.DeleteOptions{
			GracePeriodSeconds: ptr.To(int64(0)),
		})
		if delErr == nil || apierrors.IsNotFound(delErr) {
			if delErr == nil {
				deleted++
			}
			continue
		}
		return deleted, delErr
	}
	return deleted, nil
}

func (r *OrphanPodReaper) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = r.ReapOnce(ctx)
		}
	}
}

package control

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Backend names the persistence store Temporal is configured against. These
// are not free-form: they are the basenames of the config files in the
// ConfigMap (sqlite.yaml, aerospike.yaml), which is what makes a switch a
// single environment-variable change rather than a redeploy.
type Backend string

const (
	BackendSQLite    Backend = "sqlite"
	BackendAerospike Backend = "aerospike"
)

// envBackend is the variable the server binary reads to pick <env>.yaml out of
// its config directory. See cmd/temporal-aerospike-server: the --env flag it
// feeds to Temporal's config loader is bound to this name.
const envBackend = "TEMPORAL_ENVIRONMENT"

// ParseBackend validates a backend name from the API.
func ParseBackend(s string) (Backend, error) {
	switch Backend(s) {
	case BackendSQLite:
		return BackendSQLite, nil
	case BackendAerospike:
		return BackendAerospike, nil
	default:
		return "", fmt.Errorf("unknown backend %q: expected %q or %q", s, BackendSQLite, BackendAerospike)
	}
}

// BackendController reads and changes which persistence store Temporal runs
// against, by patching one environment variable on its Deployment.
//
// Nothing here restarts anything by hand. Changing the pod template is what
// makes Kubernetes roll the Deployment, so the switch is a single declarative
// write followed by a wait -- no delete-pod, no scale-to-zero, no window where
// the desired state on the API server disagrees with what is running.
type BackendController struct {
	k8s        kubernetes.Interface
	namespace  string
	deployment string
	timeout    time.Duration
}

// NewBackendController builds a controller against the cluster this process is
// running in, falling back to a kubeconfig so the whole thing can be driven
// from a laptop against a k3s cluster during development.
func NewBackendController(cfg Config) (*BackendController, error) {
	restCfg, err := kubeRESTConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("building Kubernetes client: %w", err)
	}
	return newBackendController(clientset, cfg), nil
}

// newBackendController takes the client as an interface so the switch logic can
// be exercised against a fake cluster. This path cannot be tested against a real
// one from a developer machine, and it is the part of the demo with the most
// ways to be subtly wrong.
func newBackendController(k8s kubernetes.Interface, cfg Config) *BackendController {
	return &BackendController{
		k8s:        k8s,
		namespace:  cfg.KubeNamespace,
		deployment: cfg.TemporalDeployment,
		timeout:    cfg.RolloutTimeout,
	}
}

func kubeRESTConfig() (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	if err != rest.ErrNotInCluster {
		return nil, fmt.Errorf("reading in-cluster Kubernetes config: %w", err)
	}

	// Out-of-cluster: honour KUBECONFIG, then the conventional location.
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if rules.ExplicitPath == "" && len(rules.Precedence) == 0 {
		rules.ExplicitPath = filepath.Join(homeDir(), ".kube", "config")
	}
	cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("no in-cluster config and no usable kubeconfig: %w", err)
	}
	return cfg, nil
}

func homeDir() string {
	if h, err := os.UserHomeDir(); err == nil {
		return h
	}
	return ""
}

// Current reports which backend the Deployment is configured for.
func (b *BackendController) Current(ctx context.Context) (Backend, error) {
	dep, err := b.get(ctx)
	if err != nil {
		return "", err
	}
	_, value, err := findBackendEnv(dep)
	if err != nil {
		return "", err
	}
	return ParseBackend(value)
}

// Ready reports whether the Deployment has its full complement of up-to-date,
// available replicas -- that is, whether Temporal is actually serving.
func (b *BackendController) Ready(ctx context.Context) (bool, error) {
	dep, err := b.get(ctx)
	if err != nil {
		return false, err
	}
	return rolloutComplete(dep), nil
}

// Switch patches the backend variable and waits for the rollout to finish,
// reporting each observable step through report.
//
// It is safe to call with the backend already in place: the patch is a no-op,
// Kubernetes does not roll the Deployment, and the wait returns as soon as it
// sees the existing pods are healthy. That matters because the caller's
// sequence -- re-register the namespace, restart the worker -- must still run
// even when the operator clicks the button they are already on.
func (b *BackendController) Switch(ctx context.Context, target Backend, report func(string)) error {
	dep, err := b.get(ctx)
	if err != nil {
		return err
	}
	container, current, err := findBackendEnv(dep)
	if err != nil {
		return err
	}

	if Backend(current) == target {
		report(fmt.Sprintf("deployment %s is already on %s; verifying it is healthy", b.deployment, target))
	} else {
		report(fmt.Sprintf("patching %s=%s on deployment %s (container %s)",
			envBackend, target, b.deployment, container))
	}

	patch, err := backendPatch(container, target)
	if err != nil {
		return err
	}
	patched, err := b.k8s.AppsV1().Deployments(b.namespace).Patch(
		ctx, b.deployment, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("patching %s on deployment %s/%s: %w",
			envBackend, b.namespace, b.deployment, err)
	}

	report("waiting for the rollout to complete")
	return b.waitForRollout(ctx, patched.Generation, report)
}

// backendPatch builds a strategic merge patch. Strategic merge is the right
// tool here and a JSON merge patch is not: the container list merges on `name`
// and the env list on the variable's `name`, so this changes one variable on
// one container and leaves everything else in the pod template untouched. A
// plain merge patch would replace both lists wholesale.
func backendPatch(container string, target Backend) ([]byte, error) {
	patch := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{
						map[string]any{
							"name": container,
							"env": []any{
								map[string]any{"name": envBackend, "value": string(target)},
							},
						},
					},
				},
			},
		},
	}
	b, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("encoding backend patch: %w", err)
	}
	return b, nil
}

// waitForRollout blocks until the Deployment has finished rolling to the
// generation produced by our patch.
//
// generation is the guard against the classic false pass: immediately after a
// patch the status still describes the *previous* ReplicaSet, which looks
// perfectly healthy. Waiting for observedGeneration to catch up is what makes
// "ready" mean the new pods rather than the old ones.
func (b *BackendController) waitForRollout(ctx context.Context, generation int64, report func(string)) error {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastReport string
	for {
		dep, err := b.get(ctx)
		if err != nil {
			return err
		}

		if dep.Status.ObservedGeneration >= generation && rolloutComplete(dep) {
			if err := b.reportReadyPods(ctx, dep, report); err != nil {
				// Pod detail is narration, not a gate: the Deployment status
				// has already told us the rollout is done.
				report(fmt.Sprintf("rollout complete (could not list pods: %v)", err))
				return nil
			}
			return nil
		}

		if msg := rolloutFailure(dep); msg != "" {
			return fmt.Errorf("rollout of %s/%s failed: %s", b.namespace, b.deployment, msg)
		}

		if msg := rolloutProgress(dep); msg != lastReport {
			report(msg)
			lastReport = msg
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("rollout of %s/%s did not complete within %s: %s",
				b.namespace, b.deployment, b.timeout, rolloutProgress(dep))
		case <-ticker.C:
		}
	}
}

func (b *BackendController) reportReadyPods(ctx context.Context, dep *appsv1.Deployment, report func(string)) error {
	pods, err := b.k8s.CoreV1().Pods(b.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set(dep.Spec.Selector.MatchLabels).String(),
	})
	if err != nil {
		return fmt.Errorf("listing pods for %s/%s: %w", b.namespace, b.deployment, err)
	}
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp == nil && podReady(&pod) {
			report(fmt.Sprintf("pod %s is ready", pod.Name))
		}
	}
	return nil
}

func (b *BackendController) get(ctx context.Context) (*appsv1.Deployment, error) {
	dep, err := b.k8s.AppsV1().Deployments(b.namespace).Get(ctx, b.deployment, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("reading deployment %s/%s: %w", b.namespace, b.deployment, err)
	}
	return dep, nil
}

// findBackendEnv locates the container carrying the backend variable and
// returns its current value.
func findBackendEnv(dep *appsv1.Deployment) (container string, value string, err error) {
	for _, c := range dep.Spec.Template.Spec.Containers {
		for _, e := range c.Env {
			if e.Name != envBackend {
				continue
			}
			if e.ValueFrom != nil {
				// A ConfigMap or field reference would mean the value lives
				// somewhere this service has no permission to read, and
				// patching it here would be silently overridden.
				return "", "", fmt.Errorf(
					"container %s sets %s via valueFrom; the control plane needs it set as a literal value",
					c.Name, envBackend)
			}
			return c.Name, e.Value, nil
		}
	}
	return "", "", fmt.Errorf("no container in deployment %s sets %s", dep.Name, envBackend)
}

// rolloutComplete mirrors what `kubectl rollout status` waits for: the new
// ReplicaSet is fully scaled up, every old pod is gone, and the new ones are
// available.
func rolloutComplete(dep *appsv1.Deployment) bool {
	desired := int32(1)
	if dep.Spec.Replicas != nil {
		desired = *dep.Spec.Replicas
	}
	s := dep.Status
	return s.UpdatedReplicas == desired &&
		s.Replicas == desired &&
		s.AvailableReplicas == desired
}

func rolloutProgress(dep *appsv1.Deployment) string {
	desired := int32(1)
	if dep.Spec.Replicas != nil {
		desired = *dep.Spec.Replicas
	}
	return fmt.Sprintf("rollout: %d/%d updated, %d available, %d total",
		dep.Status.UpdatedReplicas, desired, dep.Status.AvailableReplicas, dep.Status.Replicas)
}

// rolloutFailure turns a stuck rollout into an error instead of a timeout.
// Kubernetes sets Progressing=False with reason ProgressDeadlineExceeded when
// it gives up, and waiting out our own timeout after that just delays a message
// the cluster already has.
func rolloutFailure(dep *appsv1.Deployment) string {
	for _, c := range dep.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing &&
			c.Status == corev1.ConditionFalse &&
			c.Reason == "ProgressDeadlineExceeded" {
			return c.Message
		}
	}
	return ""
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

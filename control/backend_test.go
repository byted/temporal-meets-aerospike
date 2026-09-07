package control

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// The Kubernetes path cannot be exercised against a real cluster from a
// developer machine, and it is the part of the demo with the most ways to be
// quietly wrong -- a patch that replaces a list instead of merging into it, a
// rollout wait that passes on the previous ReplicaSet's status. A fake cluster
// catches both.

func testDeployment(backend string) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "temporal", Namespace: "demo", Generation: 1},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "temporal"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "temporal"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "sidecar",
							Image: "busybox",
							Env:   []corev1.EnvVar{{Name: "UNRELATED", Value: "keep-me"}},
						},
						{
							Name:  "temporal",
							Image: "temporal-meets-aerospike/server:dev",
							Env: []corev1.EnvVar{
								{Name: "TEMPORAL_CONFIG_DIR", Value: "/etc/temporal/config"},
								{Name: envBackend, Value: backend},
							},
						},
					},
				},
			},
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Replicas:           1,
			UpdatedReplicas:    1,
			AvailableReplicas:  1,
		},
	}
}

func TestCurrentReadsTheBackendVariable(t *testing.T) {
	cfg := Config{KubeNamespace: "demo", TemporalDeployment: "temporal", RolloutTimeout: 5 * time.Second}
	client := fake.NewSimpleClientset(testDeployment("sqlite"))
	b := newBackendController(client, cfg)

	got, err := b.Current(context.Background())
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	if got != BackendSQLite {
		t.Fatalf("Current = %q, want %q", got, BackendSQLite)
	}
}

// TestSwitchMergesRatherThanReplaces is the assertion that matters. A JSON
// merge patch would blow away the other container and the sibling environment
// variables; only a strategic merge keyed on `name` touches just the one value.
func TestSwitchMergesRatherThanReplaces(t *testing.T) {
	cfg := Config{KubeNamespace: "demo", TemporalDeployment: "temporal", RolloutTimeout: 5 * time.Second}
	client := fake.NewSimpleClientset(testDeployment("sqlite"))
	b := newBackendController(client, cfg)

	var steps []string
	if err := b.Switch(context.Background(), BackendAerospike, func(m string) { steps = append(steps, m) }); err != nil {
		t.Fatalf("Switch: %v", err)
	}

	dep, err := client.AppsV1().Deployments("demo").Get(context.Background(), "temporal", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-reading deployment: %v", err)
	}

	containers := dep.Spec.Template.Spec.Containers
	if len(containers) != 2 {
		t.Fatalf("patch dropped a container: %d remain", len(containers))
	}

	env := map[string]string{}
	var target *corev1.Container
	for i := range containers {
		if containers[i].Name == "temporal" {
			target = &containers[i]
		}
	}
	if target == nil {
		t.Fatal("the temporal container is gone")
	}
	for _, e := range target.Env {
		env[e.Name] = e.Value
	}
	if env[envBackend] != string(BackendAerospike) {
		t.Errorf("%s = %q, want %q", envBackend, env[envBackend], BackendAerospike)
	}
	if env["TEMPORAL_CONFIG_DIR"] != "/etc/temporal/config" {
		t.Errorf("patch dropped the sibling env var; env is now %v", env)
	}
	if containers[0].Env[0].Value != "keep-me" {
		t.Errorf("patch disturbed the other container: %v", containers[0].Env)
	}

	if len(steps) == 0 || !strings.Contains(steps[0], envBackend) {
		t.Errorf("expected the patch to be reported, got %v", steps)
	}
}

// TestSwitchRejectsValueFrom: a variable sourced from a ConfigMap cannot be
// patched here, and silently appearing to succeed would be the worst outcome.
func TestSwitchRejectsValueFrom(t *testing.T) {
	dep := testDeployment("sqlite")
	for i, c := range dep.Spec.Template.Spec.Containers {
		if c.Name != "temporal" {
			continue
		}
		for j, e := range c.Env {
			if e.Name == envBackend {
				dep.Spec.Template.Spec.Containers[i].Env[j] = corev1.EnvVar{
					Name:      envBackend,
					ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}},
				}
			}
		}
	}

	cfg := Config{KubeNamespace: "demo", TemporalDeployment: "temporal", RolloutTimeout: time.Second}
	b := newBackendController(fake.NewSimpleClientset(dep), cfg)

	err := b.Switch(context.Background(), BackendAerospike, func(string) {})
	if err == nil || !strings.Contains(err.Error(), "valueFrom") {
		t.Fatalf("expected a valueFrom error, got %v", err)
	}
}

func TestSwitchFailsWhenTheVariableIsAbsent(t *testing.T) {
	dep := testDeployment("sqlite")
	dep.Spec.Template.Spec.Containers[1].Env = nil

	cfg := Config{KubeNamespace: "demo", TemporalDeployment: "temporal", RolloutTimeout: time.Second}
	b := newBackendController(fake.NewSimpleClientset(dep), cfg)

	if _, err := b.Current(context.Background()); err == nil {
		t.Fatal("expected an error when no container sets the backend variable")
	}
}

// TestWaitForRolloutIgnoresStaleStatus pins the false-pass: a Deployment whose
// status still describes the generation before the patch must not be reported
// as rolled out.
func TestWaitForRolloutIgnoresStaleStatus(t *testing.T) {
	dep := testDeployment("sqlite")
	dep.Status.ObservedGeneration = 1

	cfg := Config{KubeNamespace: "demo", TemporalDeployment: "temporal", RolloutTimeout: 500 * time.Millisecond}
	b := newBackendController(fake.NewSimpleClientset(dep), cfg)

	err := b.waitForRollout(context.Background(), 2, func(string) {})
	if err == nil {
		t.Fatal("expected a timeout while status still describes the previous generation")
	}
	if !strings.Contains(err.Error(), "did not complete") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRolloutFailureSurfacesProgressDeadline(t *testing.T) {
	dep := testDeployment("sqlite")
	dep.Status.AvailableReplicas = 0
	dep.Status.Conditions = []appsv1.DeploymentCondition{{
		Type:    appsv1.DeploymentProgressing,
		Status:  corev1.ConditionFalse,
		Reason:  "ProgressDeadlineExceeded",
		Message: `ReplicaSet "temporal-abc" has timed out progressing.`,
	}}

	cfg := Config{KubeNamespace: "demo", TemporalDeployment: "temporal", RolloutTimeout: 10 * time.Second}
	b := newBackendController(fake.NewSimpleClientset(dep), cfg)

	err := b.waitForRollout(context.Background(), 1, func(string) {})
	if err == nil || !strings.Contains(err.Error(), "timed out progressing") {
		t.Fatalf("expected the cluster's own message, got %v", err)
	}
}

func TestParseBackend(t *testing.T) {
	for _, in := range []string{"sqlite", "aerospike"} {
		if _, err := ParseBackend(in); err != nil {
			t.Errorf("ParseBackend(%q): %v", in, err)
		}
	}
	if _, err := ParseBackend("cassandra"); err == nil {
		t.Error("ParseBackend accepted an unsupported backend")
	}
}

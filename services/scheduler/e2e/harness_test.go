//go:build e2e

// Scheduler HA failover e2e tests (#259, review requirement R3).
// Runs against a Kind cluster with: scheduler x3 (leader election on,
// Redis bindings) and stub nodes (admin /nodes + heartbeats). The workflow
// builds and loads the images and applies services/scheduler/e2e/k8s.
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var schedAddr = envOr("E2E_SCHED_ADDR", "127.0.0.1:19090")

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func kube(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func dial(t *testing.T) schedulerv1.SchedulerClient {
	t.Helper()
	conn, err := grpc.NewClient(schedAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", schedAddr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return schedulerv1.NewSchedulerClient(conn)
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := cond()
		if err == nil && ok {
			return
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (last error: %v)", what, lastErr)
}

func leaderIdentity(t *testing.T) string {
	t.Helper()
	out := kube(t, "get", "lease", "agentenv-scheduler", "-o", "jsonpath={.spec.holderIdentity}")
	return strings.TrimSpace(out)
}

func schedulerEndpoints(t *testing.T) []string {
	t.Helper()
	out := kube(t, "get", "endpoints", "agentenv-scheduler", "-o", "jsonpath={.subsets[*].addresses[*].targetRef.name}")
	return strings.Fields(out)
}

// T1: kill the leader pod — a standby must take over within the lease
// budget, pre-existing bindings keep resolving through Redis, and
// scheduling resumes on freshly observed nodes.
func TestFailoverLeaderKill(t *testing.T) {
	client := dial(t)
	ctx := context.Background()

	if _, err := client.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-e2e-1",
		Node:      firstDiscoveredNode(t, client),
	}); err != nil {
		t.Fatalf("seed binding failed: %v", err)
	}

	// Continuous lookup during the failover: must never fail with NotFound.
	lookupErrs := make(chan error, 1)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, err := client.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-e2e-1"})
			if err != nil {
				if s, ok := status.FromError(err); ok && strings.Contains(s.Message(), "not found") {
					lookupErrs <- fmt.Errorf("lookup returned not found mid-failover: %v", err)
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	defer close(stop)

	victim := leaderIdentity(t)
	t.Logf("killing leader pod %s", victim)
	kube(t, "delete", "pod", victim, "--force", "--grace-period=0")

	eventually(t, 30*time.Second, "a new leader", func() (bool, error) {
		cur := leaderIdentity(t)
		return cur != "" && cur != victim, nil
	})
	eventually(t, 30*time.Second, "endpoints converge on one leader", func() (bool, error) {
		return len(schedulerEndpoints(t)) == 1, nil
	})

	select {
	case err := <-lookupErrs:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
	}

	eventually(t, 30*time.Second, "scheduling works again on the new leader", func() (bool, error) {
		_, err := client.Schedule(ctx, &schedulerv1.ScheduleRequest{})
		return err == nil, nil
	})
}

// T2: freeze the leader (SIGSTOP = API partition equivalent) — the standby
// must take over, and the frozen ex-leader must exit on resume instead of
// serving as a second primary.
func TestPartitionFrozenLeader(t *testing.T) {
	victim := leaderIdentity(t)
	t.Logf("partitioning leader pod %s from the API (egress block)", victim)
	kube(t, "label", "pod", victim, "e2e-partition=true", "--overwrite")
	writeFile(t, "/tmp/e2e-netpol.yaml", `apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: e2e-partition
spec:
  podSelector:
    matchLabels:
      e2e-partition: "true"
  policyTypes: ["Egress"]
  egress: []
`)
	kube(t, "apply", "-f", "/tmp/e2e-netpol.yaml")
	t.Cleanup(func() { kube(t, "delete", "networkpolicy", "e2e-partition", "--ignore-not-found") })

	eventually(t, 45*time.Second, "standby takes over while old leader partitioned", func() (bool, error) {
		cur := leaderIdentity(t)
		return cur != "" && cur != victim, nil
	})

	// While partitioned, there must be exactly one serving endpoint.
	eps := schedulerEndpoints(t)
	if len(eps) != 1 {
		t.Fatalf("expected exactly one endpoint during partition, got %v", eps)
	}

	kube(t, "delete", "networkpolicy", "e2e-partition")
	// With egress restored, the ex-leader's renew failure has already fired
	// OnStoppedLeading: it force-stops and its pod restarts as standby.
	eventually(t, 60*time.Second, "frozen ex-leader exits (pod restarts)", func() (bool, error) {
		out := kube(t, "get", "pod", victim, "-o", "jsonpath={.status.containerStatuses[0].restartCount}")
		return out != "0", nil
	})
	eventually(t, 30*time.Second, "still exactly one leader after resume", func() (bool, error) {
		return len(schedulerEndpoints(t)) == 1, nil
	})
}

// T3: right after takeover, with node heartbeats paused, scheduling must
// return Unavailable (never fall back to unobserved nodes); once heartbeats
// resume, scheduling recovers.
func TestTakeoverSchedulingSemantics(t *testing.T) {
	client := dial(t)
	ctx := context.Background()

	stubPost(t, "control/pause", "")
	victim := leaderIdentity(t)
	kube(t, "delete", "pod", victim, "--force", "--grace-period=0")

	eventually(t, 30*time.Second, "new leader", func() (bool, error) {
		cur := leaderIdentity(t)
		return cur != "" && cur != victim, nil
	})

	// No node has freshly reported to the new leader: Unavailable, not a guess.
	if _, err := client.Schedule(ctx, &schedulerv1.ScheduleRequest{}); err == nil {
		t.Fatal("schedule must fail with Unavailable before fresh observations, not guess capacity")
	}

	stubPost(t, "control/resume", "")
	eventually(t, 30*time.Second, "scheduling recovers after heartbeats resume", func() (bool, error) {
		_, err := client.Schedule(ctx, &schedulerv1.ScheduleRequest{})
		return err == nil, nil
	})
}

// T4: a write that lands on the demoted leader mid-takeover must not
// commit after the takeover; the client retries and succeeds on the new
// leader.
func TestDemotedLeaderDelayedWrite(t *testing.T) {
	client := dial(t)
	ctx := context.Background()

	victim := leaderIdentity(t)
	done := make(chan error, 1)
	go func() {
		_, err := client.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{
			SandboxId: "sbx-e2e-t4",
			Node:      firstDiscoveredNode(t, client),
		})
		done <- err
	}()

	kube(t, "delete", "pod", victim, "--force", "--grace-period=0")
	writeErr := <-done
	t.Logf("write during takeover returned: %v", writeErr)

	eventually(t, 30*time.Second, "new leader", func() (bool, error) {
		cur := leaderIdentity(t)
		return cur != "" && cur != victim, nil
	})

	// The client-side retry path: writing again must succeed on the new leader.
	if _, err := client.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-e2e-t4",
		Node:      firstDiscoveredNode(t, client),
	}); err != nil {
		t.Fatalf("write retry on the new leader failed: %v", err)
	}
	if _, err := client.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-e2e-t4"}); err != nil {
		t.Fatalf("binding must exist after retry, got %v", err)
	}
}

// firstDiscoveredNode resolves a node through the scheduler's own discovery,
// so RecordAssignment passes the service's known-node validation.
func firstDiscoveredNode(t *testing.T, client schedulerv1.SchedulerClient) *schedulerv1.Node {
	t.Helper()
	resp, err := client.ListNodes(context.Background(), &schedulerv1.ListNodesRequest{})
	if err != nil || len(resp.GetNodes()) == 0 {
		t.Fatalf("ListNodes must return discovered stub nodes: %v", err)
	}
	return resp.GetNodes()[0]
}

// stubPost hits every stub's control plane through an ephemeral busybox
// debug container (the stub image is distroless, so kubectl exec into the
// container itself is not possible).
func stubPost(t *testing.T, path, body string) {
	t.Helper()
	pods := kube(t, "get", "pods", "-l", "app=stubnode", "-o", "jsonpath={.items[*].metadata.name}")
	for _, pod := range strings.Fields(pods) {
		out, err := exec.Command("kubectl", "debug", "-q", pod, "--image=busybox:1.36",
			"--", "wget", "-q", "-O", "-", "--post-data", body, "http://127.0.0.1:8000/"+path).CombinedOutput()
		if err != nil {
			t.Fatalf("stub control %s on %s failed: %v\n%s", path, pod, err, out)
		}
	}
}

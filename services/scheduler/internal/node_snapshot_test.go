package scheduler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Sync-node-snapshots fetcher tests (#259): the production pull assembles a
// heartbeat-shaped request from GET /nodes + GET /sandboxes, honoring the
// X-Admin-Token header.

func adminTestServer(t *testing.T, wantKey string, nodeJSON, sandboxesJSON string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
		if wantKey != "" && r.Header.Get("X-Admin-Token") != wantKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(nodeJSON))
	})
	mux.HandleFunc("/sandboxes", func(w http.ResponseWriter, r *http.Request) {
		if wantKey != "" && r.Header.Get("X-Admin-Token") != wantKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(sandboxesJSON))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

const adminNodeFixture = `[{
  "version": "0.2.0",
  "commit": "abc123",
  "id": "node-a",
  "serviceInstanceID": "inst-1",
  "clusterID": "cluster-1",
  "sandboxCount": 2,
  "createSuccesses": 10,
  "createFails": 1,
  "sandboxStartingCount": 1,
  "sandboxPausedCount": 3,
  "machineInfo": {"cpuFamily": "6", "cpuModel": "85", "cpuModelName": "Xeon", "cpuArchitecture": "x86_64", "cpuConfigJSON": "{}"},
  "metrics": {
    "allocatedCPU": 4,
    "allocatedMemoryBytes": 8589934592,
    "cpuPercent": 55,
    "cpuCount": 16,
    "memoryUsedBytes": 17179869184,
    "memoryTotalBytes": 34359738368,
    "pausedAllocatedCPU": 2,
    "pausedAllocatedMemoryBytes": 4294967296,
    "disks": [{"mountPoint": "/", "device": "/dev/ublkb0", "filesystemType": "ext4", "usedBytes": 1024, "totalBytes": 4096}]
  }
}]`

const adminSandboxesFixture = `[{"sandboxID": "sbx-1"}, {"sandboxID": "sbx-2"}]`

func TestAdminSnapshotFetcherAssemblesHeartbeatShape(t *testing.T) {
	srv := adminTestServer(t, "secret", adminNodeFixture, adminSandboxesFixture)
	fetch := NewAdminSnapshotFetcher("secret")

	req, err := fetch(context.Background(), Node{ID: "node-a", Endpoint: srv.URL})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if req.nodeID != "node-a" || req.serviceInstanceID != "inst-1" || req.clusterID != "cluster-1" {
		t.Fatalf("identity fields wrong: %v", req)
	}
	snap := req.snapshot
	if snap == nil {
		t.Fatal("snapshot must be populated")
	}
	if snap.GetAllocatedCpu() != 4 || snap.GetCpuPercent() != 55 || snap.GetSandboxCount() != 2 {
		t.Fatalf("metrics mapping wrong: %+v", snap)
	}
	if snap.GetPausedSandboxCount() != 3 || snap.GetPausedAllocatedCpu() != 2 {
		t.Fatalf("paused mapping wrong: %+v", snap)
	}
	if len(snap.GetDisks()) != 1 || snap.GetDisks()[0].GetDevice() != "/dev/ublkb0" {
		t.Fatalf("disk mapping wrong: %+v", snap.GetDisks())
	}
	// Nodes without the parity fields (older admin API) yield no roster;
	// ingest must skip reconciliation for them.
	if req.sandboxIDs != nil {
		t.Fatalf("degraded pull must leave sandboxIDs nil, got %v", req.sandboxIDs)
	}
}

const adminParityNodeFixture = `[{
  "version": "0.2.0",
  "commit": "abc123",
  "id": "node-a",
  "serviceInstanceID": "inst-1",
  "clusterID": "cluster-1",
  "sandboxCount": 2,
  "createSuccesses": 10,
  "createFails": 1,
  "sandboxStartingCount": 1,
  "sandboxPausedCount": 3,
  "sandboxIDs": ["sbx-1", "sbx-paused"],
  "p2pEndpoint": {"backend": "iroh", "address": "node-a.p2p.local"},
  "machineInfo": {"cpuFamily": "6", "cpuModel": "85", "cpuModelName": "Xeon", "cpuArchitecture": "x86_64", "cpuConfigJSON": "{\"ht\":true}"},
  "metrics": {
    "allocatedCPU": 4,
    "allocatedMemoryBytes": 8589934592,
    "cpuPercent": 55,
    "cpuCount": 16,
    "memoryUsedBytes": 17179869184,
    "memoryTotalBytes": 34359738368,
    "pausedAllocatedCPU": 2,
    "pausedAllocatedMemoryBytes": 4294967296,
    "disks": [{"mountPoint": "/", "device": "/dev/ublkb0", "filesystemType": "ext4", "usedBytes": 1024, "totalBytes": 4096}]
  }
}]`

// Parity path (#341 review): when the admin API exposes the heartbeat fields
// (sandboxIDs, p2pEndpoint, cpuConfigJSON), the pull maps them through.
func TestAdminSnapshotFetcherParityFields(t *testing.T) {
	srv := adminTestServer(t, "", adminParityNodeFixture, "[]")
	fetch := NewAdminSnapshotFetcher("")

	req, err := fetch(context.Background(), Node{ID: "node-a", Endpoint: srv.URL})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if len(req.sandboxIDs) != 2 || req.sandboxIDs[0] != "sbx-1" || req.sandboxIDs[1] != "sbx-paused" {
		t.Fatalf("parity roster mapping wrong: %v", req.sandboxIDs)
	}
	if req.p2pEndpoint == nil || req.p2pEndpoint.GetBackend() != "iroh" || req.p2pEndpoint.GetAddress() != "node-a.p2p.local" {
		t.Fatalf("parity p2p mapping wrong: %v", req.p2pEndpoint)
	}
	if req.machineInfo.GetCpuConfigJson() != `{"ht":true}` {
		t.Fatalf("parity cpuConfigJSON mapping wrong: %q", req.machineInfo.GetCpuConfigJson())
	}
}

func TestAdminSnapshotFetcherAuthFailure(t *testing.T) {
	srv := adminTestServer(t, "secret", adminNodeFixture, adminSandboxesFixture)
	fetch := NewAdminSnapshotFetcher("wrong-key")

	if _, err := fetch(context.Background(), Node{ID: "node-a", Endpoint: srv.URL}); err == nil {
		t.Fatal("fetch with a wrong key must fail")
	}
}

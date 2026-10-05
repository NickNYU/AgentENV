package scheduler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Sync-node-snapshots fetcher tests (#259): the production pull assembles a
// heartbeat-shaped request from GET /nodes + GET /sandboxes, honoring the
// x-api-key header.

func adminTestServer(t *testing.T, wantKey string, nodeJSON, sandboxesJSON string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
		if wantKey != "" && r.Header.Get("x-api-key") != wantKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(nodeJSON))
	})
	mux.HandleFunc("/sandboxes", func(w http.ResponseWriter, r *http.Request) {
		if wantKey != "" && r.Header.Get("x-api-key") != wantKey {
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
	// A pull must not carry a sandbox roster: reconciling from it would
	// delete live bindings (paused sandboxes, template builds) on failover.
	if req.sandboxIDs != nil {
		t.Fatalf("pull must leave sandboxIDs nil, got %v", req.sandboxIDs)
	}
}

func TestAdminSnapshotFetcherAuthFailure(t *testing.T) {
	srv := adminTestServer(t, "secret", adminNodeFixture, adminSandboxesFixture)
	fetch := NewAdminSnapshotFetcher("wrong-key")

	if _, err := fetch(context.Background(), Node{ID: "node-a", Endpoint: srv.URL}); err == nil {
		t.Fatal("fetch with a wrong key must fail")
	}
}

func TestAdminSnapshotFetcherHeartbeatIngestCompatibility(t *testing.T) {
	srv := adminTestServer(t, "", adminNodeFixture, adminSandboxesFixture)
	fetch := NewAdminSnapshotFetcher("")

	svc, registry, store := newTestService(t, []string{"node-a"})
	req, err := fetch(context.Background(), Node{ID: "node-a", Endpoint: srv.URL})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if _, err := svc.ingestNodeReport(req, time.Now()); err != nil {
		t.Fatalf("pulled snapshot must ingest through the shared path: %v", err)
	}
	if registry.PeekObserved("node-a") == nil {
		t.Fatal("pulled snapshot must record an observation")
	}
	// The pull refreshes observations only; bindings are refreshed by
	// heartbeats, so nothing may be written to the store here.
	if _, ok, err := store.Get("sbx-1", time.Now()); err != nil || ok {
		t.Fatalf("pull-ingest must not touch bindings: ok=%v err=%v", ok, err)
	}
}

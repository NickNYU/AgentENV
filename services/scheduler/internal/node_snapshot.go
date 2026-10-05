package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// ============================================================

// nodeSnapshotRefresher is this file's contract: refresh the leader's
// observations and bindings for a set of nodes by pulling their admin
// snapshots (sync-node-snapshots, #259). The only production implementation
// is ConcurrentNodeSnapshotRefresher; callers (the leadership manager) depend on this
// interface, not on the concrete type.
type nodeSnapshotRefresher interface {
	Refresh(ctx context.Context, nodes []Node)
}

// nodeReport is the neutral currency of node-state ingestion (#259): what a
// fetcher returns after successfully pulling a node, and what the shared
// ingest path consumes. Both the Heartbeat RPC handler and
// sync-node-snapshots convert into it; a fetcher never fabricates an RPC
// request.
type nodeReport struct {
	nodeID            string
	serviceInstanceID string
	clusterID         string
	version           string
	commit            string
	machineInfo       *schedulerv1.MachineInfo
	snapshot          *schedulerv1.NodeSnapshot
	sandboxIDs        []string
	p2pEndpoint       *schedulerv1.P2PEndpoint
}

// toHeartbeatRequest converts the report to the registry's proto shape; the
// conversion is contained here so the registry interface stays unchanged.
func (r *nodeReport) toHeartbeatRequest() *schedulerv1.HeartbeatRequest {
	return &schedulerv1.HeartbeatRequest{
		NodeId:            r.nodeID,
		ClusterId:         r.clusterID,
		ServiceInstanceId: r.serviceInstanceID,
		Version:           r.version,
		Commit:            r.commit,
		MachineInfo:       r.machineInfo,
		Snapshot:          r.snapshot,
		SandboxIds:        r.sandboxIDs,
		P2PEndpoint:       r.p2pEndpoint,
	}
}

// NodeSnapshotFetcher retrieves a single node's current snapshot from its
// admin endpoints for sync-node-snapshots on leadership acquisition (#259).
// It returns the fetched data (a nodeReport), not an RPC request. It is an
// injected seam so the retrieval logic is testable without HTTP or node
// credentials.
type NodeSnapshotFetcher func(ctx context.Context, node Node) (*nodeReport, error)

// ConcurrentNodeSnapshotRefresher drives the post-acquisition state rebuild: fan out a
// retrieval of every registry node's snapshot with bounded concurrency and
// feed each response through the shared ingest path (ingestNodeReport — the
// same code a heartbeat takes). A failed retrieval leaves that node
// unobserved; the recovery window's fresh-observations-only rule keeps it out of
// scheduling until its next report.
// nodeReportIngester is the retriever's only dependency on the Service: the
// shared ingest path. The retriever never holds the Service itself.
type nodeReportIngester func(report *nodeReport, now time.Time) error

type ConcurrentNodeSnapshotRefresher struct {
	logger      *zap.Logger
	ingest      nodeReportIngester
	fetch       NodeSnapshotFetcher
	concurrency int
}

func NewConcurrentNodeSnapshotRefresher(logger *zap.Logger, ingest nodeReportIngester, fetch NodeSnapshotFetcher, concurrency int) *ConcurrentNodeSnapshotRefresher {
	if logger == nil {
		logger = zap.NewNop()
	}
	if concurrency <= 0 {
		concurrency = 4
	}
	return &ConcurrentNodeSnapshotRefresher{logger: logger, ingest: ingest, fetch: fetch, concurrency: concurrency}
}

// Refresh pulls a snapshot from every node in nodes and ingests the results.
// Every node is fetched exactly once, successes are ingested, failures are
// logged and leave the node unobserved (the recovery window's
// fresh-observations-only rule keeps it out of scheduling until its next
// report).
func (r *ConcurrentNodeSnapshotRefresher) Refresh(ctx context.Context, nodes []Node) {
	sem := make(chan struct{}, r.concurrency)
	var wg sync.WaitGroup
	for _, node := range nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			report, err := r.fetch(ctx, node)
			if err != nil {
				r.logger.Warn("sync-node-snapshots: node snapshot pull failed",
					zap.String("node_id", node.ID), zap.Error(err))
				return
			}
			if report == nil {
				return
			}
			if err := r.ingest(report, time.Now()); err != nil {
				r.logger.Warn("sync-node-snapshots: ingesting pulled snapshot failed",
					zap.String("node_id", node.ID), zap.Error(err))
			}
		}()
	}
	wg.Wait()
}

// ============================================================

// adminNodeResponse mirrors the agentenv server admin GET /nodes entry
// (openapi: Node). Field names follow the API's camelCase JSON.
type adminNodeResponse struct {
	Version            string `json:"version"`
	Commit             string `json:"commit"`
	ID                 string `json:"id"`
	ServiceInstanceID  string `json:"serviceInstanceID"`
	ClusterID          string `json:"clusterID"`
	SandboxCount       uint32 `json:"sandboxCount"`
	CreateSuccesses    uint64 `json:"createSuccesses"`
	CreateFails        uint64 `json:"createFails"`
	SandboxStartingCnt uint32 `json:"sandboxStartingCount"`
	SandboxPausedCount uint32 `json:"sandboxPausedCount"`
	MachineInfo        struct {
		CPUFamily       string `json:"cpuFamily"`
		CPUModel        string `json:"cpuModel"`
		CPUModelName    string `json:"cpuModelName"`
		CPUArchitecture string `json:"cpuArchitecture"`
		CPUConfigJSON   string `json:"cpuConfigJSON"`
	} `json:"machineInfo"`
	Metrics struct {
		AllocatedCPU               uint32 `json:"allocatedCPU"`
		AllocatedMemoryBytes       uint64 `json:"allocatedMemoryBytes"`
		CPUPercent                 uint32 `json:"cpuPercent"`
		CPUCount                   uint32 `json:"cpuCount"`
		MemoryUsedBytes            uint64 `json:"memoryUsedBytes"`
		MemoryTotalBytes           uint64 `json:"memoryTotalBytes"`
		PausedAllocatedCPU         uint32 `json:"pausedAllocatedCPU"`
		PausedAllocatedMemoryBytes uint64 `json:"pausedAllocatedMemoryBytes"`
		Disks                      []struct {
			MountPoint     string `json:"mountPoint"`
			Device         string `json:"device"`
			FilesystemType string `json:"filesystemType"`
			UsedBytes      uint64 `json:"usedBytes"`
			TotalBytes     uint64 `json:"totalBytes"`
		} `json:"disks"`
	} `json:"metrics"`
}

// adminSandboxEntry mirrors a ListedSandbox entry from GET /sandboxes; only
// the id is needed for binding reconciliation.
type adminSandboxEntry struct {
	SandboxID string `json:"sandboxID"`
}

// NewAdminSnapshotFetcher builds the production NodeSnapshotFetcher (#259,
// sync-node-snapshots): it pulls GET {endpoint}/nodes for observations and
// GET {endpoint}/sandboxes for the sandbox id list, and returns the fetched
// data as a nodeReport for the shared ingest path.
// The node's admin API requires the x-api-key header. The HTTP client is an
// implementation detail with a bounded per-request timeout; tests inject
// through the NodeSnapshotFetcher seam, not this constructor.
func NewAdminSnapshotFetcher(apiKey string) NodeSnapshotFetcher {
	client := &http.Client{Timeout: 5 * time.Second}
	return func(ctx context.Context, node Node) (*nodeReport, error) {
		base := strings.TrimRight(node.Endpoint, "/")

		var nodes []adminNodeResponse
		if err := adminGetJSON(ctx, client, apiKey, base+"/nodes", &nodes); err != nil {
			return nil, fmt.Errorf("pull %s /nodes: %w", node.ID, err)
		}
		var entry *adminNodeResponse
		for i := range nodes {
			if nodes[i].ID == node.ID {
				entry = &nodes[i]
				break
			}
		}
		if entry == nil && len(nodes) == 1 {
			entry = &nodes[0]
		}
		if entry == nil {
			return nil, fmt.Errorf("pull %s /nodes: node not in admin response", node.ID)
		}

		var sandboxes []adminSandboxEntry
		if err := adminGetJSON(ctx, client, apiKey, base+"/sandboxes", &sandboxes); err != nil {
			return nil, fmt.Errorf("pull %s /sandboxes: %w", node.ID, err)
		}

		return reportFromAdmin(entry, sandboxes), nil
	}
}

func adminGetJSON(ctx context.Context, client *http.Client, apiKey, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if strings.TrimSpace(apiKey) != "" {
		req.Header.Set("x-api-key", apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func reportFromAdmin(n *adminNodeResponse, sandboxes []adminSandboxEntry) *nodeReport {
	ids := make([]string, 0, len(sandboxes))
	for _, s := range sandboxes {
		if strings.TrimSpace(s.SandboxID) != "" {
			ids = append(ids, s.SandboxID)
		}
	}
	disks := make([]*schedulerv1.DiskMetric, 0, len(n.Metrics.Disks))
	for _, d := range n.Metrics.Disks {
		disks = append(disks, &schedulerv1.DiskMetric{
			MountPoint:     d.MountPoint,
			Device:         d.Device,
			FilesystemType: d.FilesystemType,
			UsedBytes:      d.UsedBytes,
			TotalBytes:     d.TotalBytes,
		})
	}
	return &nodeReport{
		nodeID:            n.ID,
		clusterID:         n.ClusterID,
		serviceInstanceID: n.ServiceInstanceID,
		version:           n.Version,
		commit:            n.Commit,
		machineInfo: &schedulerv1.MachineInfo{
			CpuFamily:       n.MachineInfo.CPUFamily,
			CpuModel:        n.MachineInfo.CPUModel,
			CpuModelName:    n.MachineInfo.CPUModelName,
			CpuArchitecture: n.MachineInfo.CPUArchitecture,
			CpuConfigJson:   n.MachineInfo.CPUConfigJSON,
		},
		snapshot: &schedulerv1.NodeSnapshot{
			AllocatedCpu:               n.Metrics.AllocatedCPU,
			AllocatedMemoryBytes:       n.Metrics.AllocatedMemoryBytes,
			CpuPercent:                 n.Metrics.CPUPercent,
			CpuCount:                   n.Metrics.CPUCount,
			MemoryUsedBytes:            n.Metrics.MemoryUsedBytes,
			MemoryTotalBytes:           n.Metrics.MemoryTotalBytes,
			Disks:                      disks,
			SandboxCount:               n.SandboxCount,
			SandboxStartingCount:       n.SandboxStartingCnt,
			CreateSuccesses:            n.CreateSuccesses,
			CreateFails:                n.CreateFails,
			ReportedAtUnixMs:           time.Now().UnixMilli(),
			PausedSandboxCount:         n.SandboxPausedCount,
			PausedAllocatedCpu:         n.Metrics.PausedAllocatedCPU,
			PausedAllocatedMemoryBytes: n.Metrics.PausedAllocatedMemoryBytes,
		},
		sandboxIDs: ids,
	}
}

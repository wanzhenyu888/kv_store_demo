package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	kv "kv_store_demo"
	"kv_store_demo/internal/client"
	"kv_store_demo/internal/cluster"
	"kv_store_demo/internal/shard"
)

type startedNode struct {
	id  string
	cmd *exec.Cmd
}

type runConfig struct {
	dir         string
	requests    int
	timeout     time.Duration
	concurrency int
	valueSize   int
	readRatio   int
	duration    time.Duration
	warmup      time.Duration
	reportPath  string
}

type sample struct {
	key     []byte
	value   []byte
	groupID shard.GroupID
}

type workloadReport struct {
	Cluster     clusterReport     `json:"cluster"`
	Workload    workloadConfig    `json:"workload"`
	Result      resultReport      `json:"result"`
	Consistency consistencyReport `json:"consistency"`
}

type clusterReport struct {
	Nodes  int `json:"nodes"`
	Groups int `json:"groups"`
	Shards int `json:"shards"`
}

type workloadConfig struct {
	Requests    int     `json:"requests"`
	Concurrency int     `json:"concurrency"`
	ValueSize   int     `json:"value_size"`
	ReadRatio   int     `json:"read_ratio"`
	DurationSec float64 `json:"duration_sec"`
	WarmupSec   float64 `json:"warmup_sec"`
}

type resultReport struct {
	Ops               uint64         `json:"ops"`
	ReadOps           uint64         `json:"read_ops"`
	WriteOps          uint64         `json:"write_ops"`
	ErrorCount        uint64         `json:"error_count"`
	RetryCount        uint64         `json:"retry_count"`
	NotLeaderCount    uint64         `json:"not_leader_count"`
	BytesWritten      uint64         `json:"bytes_written"`
	ElapsedSec        float64        `json:"elapsed_sec"`
	OpsSec            float64        `json:"ops_sec"`
	ReadOpsSec        float64        `json:"read_ops_sec"`
	WriteOpsSec       float64        `json:"write_ops_sec"`
	AvgLatencyMs      float64        `json:"avg_latency_ms"`
	P50LatencyMs      float64        `json:"p50_latency_ms"`
	P95LatencyMs      float64        `json:"p95_latency_ms"`
	P99LatencyMs      float64        `json:"p99_latency_ms"`
	MaxLatencyMs      float64        `json:"max_latency_ms"`
	GroupDistribution map[string]int `json:"group_distribution"`
}

type consistencyReport struct {
	VerifiedKeys         int `json:"verified_keys"`
	MissingCount         int `json:"missing_count"`
	MismatchCount        int `json:"mismatch_count"`
	ReplicaVerifiedCount int `json:"replica_verified_count"`
}

type opStats struct {
	mu           sync.Mutex
	latencies    []time.Duration
	ops          uint64
	readOps      uint64
	writeOps     uint64
	errors       uint64
	retries      uint64
	notLeader    uint64
	bytesWritten uint64
	groupCounts  map[shard.GroupID]int
}

type expectedSamples struct {
	mu      sync.Mutex
	samples []sample
}

func main() {
	config := parseRunConfig()
	if config.requests <= 0 && config.duration <= 0 {
		log.Fatalf("requests must be positive when duration is not set")
	}
	if config.concurrency <= 0 {
		log.Fatalf("concurrency must be positive")
	}
	if config.valueSize < 0 {
		log.Fatalf("value-size must be non-negative")
	}
	if config.readRatio < -1 || config.readRatio > 100 {
		log.Fatalf("read-ratio must be between 0 and 100, or -1 for legacy write-then-read mode")
	}

	if err := os.RemoveAll(config.dir); err != nil {
		log.Fatalf("clean example dir: %v", err)
	}
	if err := os.MkdirAll(config.dir, 0755); err != nil {
		log.Fatalf("create example dir: %v", err)
	}

	clusterConfig, err := newExampleClusterConfig()
	if err != nil {
		log.Fatalf("create cluster config: %v", err)
	}
	clusterPath := filepath.Join(config.dir, "cluster.json")
	if err := writeClusterConfig(clusterPath, clusterConfig); err != nil {
		log.Fatalf("write cluster config: %v", err)
	}

	nodes, err := startCluster(config.dir, clusterPath, clusterConfig)
	if err != nil {
		stopNodes(nodes)
		log.Fatalf("start cluster: %v", err)
	}
	defer func() {
		stopNodes(nodes)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), config.timeout)
	defer cancel()

	routed, err := client.NewRoutedClient(ctx, clusterConfig)
	if err != nil {
		log.Fatalf("create routed client: %v", err)
	}
	defer routed.Close()

	router, err := clusterConfig.NewRouter()
	if err != nil {
		log.Fatalf("create router: %v", err)
	}

	fmt.Printf("cluster config: %s\n", clusterPath)
	for _, node := range clusterConfig.Nodes {
		fmt.Printf("node %s client_addr=%s data_dir=%s\n", node.ID, node.ClientAddr, filepath.Join(config.dir, node.ID))
	}

	var report workloadReport
	var samples []sample
	if config.readRatio < 0 {
		samples, report = runLegacyWorkload(ctx, routed, router, clusterConfig, config)
	} else {
		samples, report = runMeasuredWorkload(ctx, routed, router, clusterConfig, config)
	}

	if err := retry(ctx, func() error {
		return routed.Flush(ctx)
	}); err != nil {
		log.Fatalf("flush groups: %v", err)
	}

	stopNodes(nodes)
	nodes = nil

	consistency, err := verifyLocalReplicas(clusterConfig, config.dir, samples)
	if err != nil {
		log.Fatalf("verify local replicas: %v", err)
	}
	report.Consistency = consistency
	fmt.Println("replica consistency verification succeeded")

	if config.reportPath != "" {
		if err := writeReport(config.reportPath, report); err != nil {
			log.Fatalf("write report: %v", err)
		}
		fmt.Printf("report: %s\n", config.reportPath)
	}
}

func parseRunConfig() runConfig {
	dir := flag.String("dir", filepath.Join(os.TempDir(), "kv_store_demo_distributed_example"), "example working directory")
	requests := flag.Int("requests", 200, "number of operations in measured mode, or key/value pairs in legacy mode")
	timeout := flag.Duration("timeout", 30*time.Second, "example timeout")
	concurrency := flag.Int("concurrency", 1, "number of concurrent workers in measured mode")
	valueSize := flag.Int("value-size", 0, "value size in bytes; 0 keeps the compact default value format")
	readRatio := flag.Int("read-ratio", -1, "read percentage from 0 to 100; -1 keeps legacy write-then-read mode")
	duration := flag.Duration("duration", 0, "measured workload duration; when set, requests is ignored")
	warmup := flag.Duration("warmup", 0, "unmeasured warmup duration before measured workload")
	reportPath := flag.String("report", "", "optional JSON report output path")
	flag.Parse()

	return runConfig{
		dir:         *dir,
		requests:    *requests,
		timeout:     *timeout,
		concurrency: *concurrency,
		valueSize:   *valueSize,
		readRatio:   *readRatio,
		duration:    *duration,
		warmup:      *warmup,
		reportPath:  *reportPath,
	}
}

func runLegacyWorkload(ctx context.Context, routed *client.RoutedClient, router *shard.Router, clusterConfig cluster.Config, config runConfig) ([]sample, workloadReport) {
	samples := buildSamples("bench/key", config.requests, config.valueSize, router)
	stats := newOpStats()

	writeElapsed := measure("write", len(samples), func() {
		for _, sample := range samples {
			start := time.Now()
			attempts, err := retryMeasured(ctx, func() error {
				return routed.Put(ctx, sample.key, sample.value)
			})
			stats.recordWrite(sample.groupID, len(sample.value), time.Since(start), attempts, err)
			if err != nil {
				log.Fatalf("put %q: %v", sample.key, err)
			}
		}
	})

	readElapsed := measure("read", len(samples), func() {
		for _, sample := range samples {
			start := time.Now()
			var got []byte
			attempts, err := retryMeasured(ctx, func() error {
				var err error
				got, err = routed.Get(ctx, sample.key)
				return err
			})
			stats.recordRead(sample.groupID, time.Since(start), attempts, err)
			if err != nil {
				log.Fatalf("get %q: %v", sample.key, err)
			}
			if !bytes.Equal(got, sample.value) {
				log.Fatalf("get %q = %q, want %q", sample.key, got, sample.value)
			}
		}
	})

	fmt.Printf("client verification succeeded: writes=%s reads=%s\n", writeElapsed, readElapsed)
	return samples, buildReport(clusterConfig, config, stats.snapshot(writeElapsed+readElapsed))
}

func runMeasuredWorkload(ctx context.Context, routed *client.RoutedClient, router *shard.Router, clusterConfig cluster.Config, config runConfig) ([]sample, workloadReport) {
	readSamples := []sample(nil)
	if config.readRatio > 0 {
		preloadCount := config.requests
		if preloadCount <= 0 {
			preloadCount = 1000
		}
		if preloadCount > 10000 {
			preloadCount = 10000
		}
		readSamples = buildSamples("bench/read", preloadCount, config.valueSize, router)
		preloadSamples(ctx, routed, readSamples)
		fmt.Printf("preloaded read set: %d keys\n", len(readSamples))
	}

	if config.warmup > 0 {
		fmt.Printf("warmup: %s\n", config.warmup)
		_, _ = executeWorkload(ctx, routed, router, config, readSamples, true)
	}

	stats, written := executeWorkload(ctx, routed, router, config, readSamples, false)
	if stats.errors > 0 {
		log.Fatalf("workload finished with %d errors", stats.errors)
	}
	elapsed := stats.elapsed
	fmt.Printf("workload: %d ops in %s, %.1f ops/sec\n", stats.ops, elapsed, safeRate(stats.ops, elapsed))
	fmt.Printf("latency: avg=%.3fms p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms\n", stats.avgMs, stats.p50Ms, stats.p95Ms, stats.p99Ms, stats.maxMs)
	fmt.Printf("ops: reads=%d writes=%d errors=%d retries=%d\n", stats.readOps, stats.writeOps, stats.errors, stats.retries)

	verifySamples := append([]sample(nil), readSamples...)
	verifySamples = append(verifySamples, written...)
	verifyClientSamples(ctx, routed, verifySamples)
	fmt.Println("client verification succeeded")

	return verifySamples, buildReport(clusterConfig, config, stats)
}

func preloadSamples(ctx context.Context, routed *client.RoutedClient, samples []sample) {
	for _, sample := range samples {
		if err := retry(ctx, func() error {
			return routed.Put(ctx, sample.key, sample.value)
		}); err != nil {
			log.Fatalf("preload put %q: %v", sample.key, err)
		}
	}
}

func executeWorkload(ctx context.Context, routed *client.RoutedClient, router *shard.Router, config runConfig, readSamples []sample, warmup bool) (statsSnapshot, []sample) {
	stats := newOpStats()
	expected := &expectedSamples{}
	var opCounter uint64
	var writeCounter uint64
	groups := routedGroups(router)
	start := time.Now()
	deadline := time.Time{}
	if config.duration > 0 {
		deadline = start.Add(config.duration)
	}
	if warmup {
		deadline = start.Add(config.warmup)
	}

	workers := config.concurrency
	var wg sync.WaitGroup
	wg.Add(workers)
	for workerID := 0; workerID < workers; workerID++ {
		workerID := workerID
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for {
				opIndex := atomic.AddUint64(&opCounter, 1) - 1
				if deadline.IsZero() {
					if opIndex >= uint64(config.requests) {
						return
					}
				} else if time.Now().After(deadline) {
					return
				}
				if err := ctx.Err(); err != nil {
					stats.recordError()
					return
				}

				doRead := len(readSamples) > 0 && rng.Intn(100) < config.readRatio
				if doRead {
					sample := readSamples[rng.Intn(len(readSamples))]
					runReadOp(ctx, routed, stats, sample)
					continue
				}

				writeIndex := atomic.AddUint64(&writeCounter, 1) - 1
				sample := buildSample("bench/write", int(writeIndex), config.valueSize, router, groups)
				err := runWriteOp(ctx, routed, stats, sample)
				if err == nil && !warmup {
					expected.add(sample)
				}
			}
		}()
	}
	wg.Wait()

	snapshot := stats.snapshot(time.Since(start))
	return snapshot, expected.list()
}

func runReadOp(ctx context.Context, routed *client.RoutedClient, stats *opStats, sample sample) {
	start := time.Now()
	got := []byte(nil)
	attempts, err := retryMeasured(ctx, func() error {
		var err error
		got, err = routed.Get(ctx, sample.key)
		return err
	})
	if err == nil && !bytes.Equal(got, sample.value) {
		err = fmt.Errorf("get %q = %q, want %q", sample.key, got, sample.value)
	}
	stats.recordRead(sample.groupID, time.Since(start), attempts, err)
}

func runWriteOp(ctx context.Context, routed *client.RoutedClient, stats *opStats, sample sample) error {
	start := time.Now()
	attempts, err := retryMeasured(ctx, func() error {
		return routed.Put(ctx, sample.key, sample.value)
	})
	stats.recordWrite(sample.groupID, len(sample.value), time.Since(start), attempts, err)
	return err
}

func verifyClientSamples(ctx context.Context, routed *client.RoutedClient, samples []sample) {
	for _, sample := range samples {
		var got []byte
		if err := retry(ctx, func() error {
			var err error
			got, err = routed.Get(ctx, sample.key)
			return err
		}); err != nil {
			log.Fatalf("verify get %q: %v", sample.key, err)
		}
		if !bytes.Equal(got, sample.value) {
			log.Fatalf("verify get %q = %q, want %q", sample.key, got, sample.value)
		}
	}
}

func newOpStats() *opStats {
	return &opStats{
		groupCounts: make(map[shard.GroupID]int),
	}
}

func (s *opStats) recordRead(groupID shard.GroupID, latency time.Duration, attempts int, err error) {
	s.record(groupID, latency, attempts, err)
	atomic.AddUint64(&s.readOps, 1)
}

func (s *opStats) recordWrite(groupID shard.GroupID, bytes int, latency time.Duration, attempts int, err error) {
	s.record(groupID, latency, attempts, err)
	atomic.AddUint64(&s.writeOps, 1)
	atomic.AddUint64(&s.bytesWritten, uint64(bytes))
}

func (s *opStats) record(groupID shard.GroupID, latency time.Duration, attempts int, err error) {
	atomic.AddUint64(&s.ops, 1)
	if attempts > 1 {
		atomic.AddUint64(&s.retries, uint64(attempts-1))
	}
	if err != nil {
		atomic.AddUint64(&s.errors, 1)
		if client.IsNotLeader(err) {
			atomic.AddUint64(&s.notLeader, 1)
		}
	}

	s.mu.Lock()
	s.latencies = append(s.latencies, latency)
	s.groupCounts[groupID]++
	s.mu.Unlock()
}

func (s *opStats) recordError() {
	atomic.AddUint64(&s.ops, 1)
	atomic.AddUint64(&s.errors, 1)
}

type statsSnapshot struct {
	ops          uint64
	readOps      uint64
	writeOps     uint64
	errors       uint64
	retries      uint64
	notLeader    uint64
	bytesWritten uint64
	elapsed      time.Duration
	avgMs        float64
	p50Ms        float64
	p95Ms        float64
	p99Ms        float64
	maxMs        float64
	groupCounts  map[shard.GroupID]int
}

func (s *opStats) snapshot(elapsed time.Duration) statsSnapshot {
	s.mu.Lock()
	latencies := append([]time.Duration(nil), s.latencies...)
	groupCounts := make(map[shard.GroupID]int, len(s.groupCounts))
	for groupID, count := range s.groupCounts {
		groupCounts[groupID] = count
	}
	s.mu.Unlock()

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var total time.Duration
	for _, latency := range latencies {
		total += latency
	}

	var avgMs float64
	if len(latencies) > 0 {
		avgMs = durationMs(total) / float64(len(latencies))
	}

	return statsSnapshot{
		ops:          atomic.LoadUint64(&s.ops),
		readOps:      atomic.LoadUint64(&s.readOps),
		writeOps:     atomic.LoadUint64(&s.writeOps),
		errors:       atomic.LoadUint64(&s.errors),
		retries:      atomic.LoadUint64(&s.retries),
		notLeader:    atomic.LoadUint64(&s.notLeader),
		bytesWritten: atomic.LoadUint64(&s.bytesWritten),
		elapsed:      elapsed,
		avgMs:        avgMs,
		p50Ms:        percentileMs(latencies, 0.50),
		p95Ms:        percentileMs(latencies, 0.95),
		p99Ms:        percentileMs(latencies, 0.99),
		maxMs:        maxLatencyMs(latencies),
		groupCounts:  groupCounts,
	}
}

func (e *expectedSamples) add(sample sample) {
	e.mu.Lock()
	e.samples = append(e.samples, sample)
	e.mu.Unlock()
}

func (e *expectedSamples) list() []sample {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sample(nil), e.samples...)
}

func percentileMs(latencies []time.Duration, percentile float64) float64 {
	if len(latencies) == 0 {
		return 0
	}
	index := int(float64(len(latencies)-1) * percentile)
	return durationMs(latencies[index])
}

func maxLatencyMs(latencies []time.Duration) float64 {
	if len(latencies) == 0 {
		return 0
	}
	return durationMs(latencies[len(latencies)-1])
}

func durationMs(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func safeRate(count uint64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}

func buildReport(clusterConfig cluster.Config, config runConfig, stats statsSnapshot) workloadReport {
	groupDistribution := make(map[string]int, len(stats.groupCounts))
	for groupID, count := range stats.groupCounts {
		groupDistribution[string(groupID)] = count
	}

	return workloadReport{
		Cluster: clusterReport{
			Nodes:  len(clusterConfig.Nodes),
			Groups: len(clusterConfig.Groups),
			Shards: clusterConfig.ShardConfig.ShardCount,
		},
		Workload: workloadConfig{
			Requests:    config.requests,
			Concurrency: config.concurrency,
			ValueSize:   config.valueSize,
			ReadRatio:   config.readRatio,
			DurationSec: config.duration.Seconds(),
			WarmupSec:   config.warmup.Seconds(),
		},
		Result: resultReport{
			Ops:               stats.ops,
			ReadOps:           stats.readOps,
			WriteOps:          stats.writeOps,
			ErrorCount:        stats.errors,
			RetryCount:        stats.retries,
			NotLeaderCount:    stats.notLeader,
			BytesWritten:      stats.bytesWritten,
			ElapsedSec:        stats.elapsed.Seconds(),
			OpsSec:            safeRate(stats.ops, stats.elapsed),
			ReadOpsSec:        safeRate(stats.readOps, stats.elapsed),
			WriteOpsSec:       safeRate(stats.writeOps, stats.elapsed),
			AvgLatencyMs:      stats.avgMs,
			P50LatencyMs:      stats.p50Ms,
			P95LatencyMs:      stats.p95Ms,
			P99LatencyMs:      stats.p99Ms,
			MaxLatencyMs:      stats.maxMs,
			GroupDistribution: groupDistribution,
		},
	}
}

func writeReport(path string, report workloadReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

func newExampleClusterConfig() (cluster.Config, error) {
	nodes := []cluster.Node{
		{ID: "n1", ClientAddr: mustFreeLocalAddr()},
		{ID: "n2", ClientAddr: mustFreeLocalAddr()},
		{ID: "n3", ClientAddr: mustFreeLocalAddr()},
	}
	g1Addrs := map[string]string{
		"n1": mustFreeLocalAddr(),
		"n2": mustFreeLocalAddr(),
		"n3": mustFreeLocalAddr(),
	}
	g2Addrs := map[string]string{
		"n1": mustFreeLocalAddr(),
		"n2": mustFreeLocalAddr(),
		"n3": mustFreeLocalAddr(),
	}

	config := cluster.Config{
		Nodes: nodes,
		ShardConfig: cluster.Shards{
			ShardCount:   4,
			VirtualNodes: 8,
		},
		ShardRoutes: []cluster.Shard{
			{ID: 0, GroupID: "g1"},
			{ID: 1, GroupID: "g1"},
			{ID: 2, GroupID: "g2"},
			{ID: 3, GroupID: "g2"},
		},
		Groups: []cluster.Group{
			{
				ID: "g1",
				Replicas: []cluster.GroupReplica{
					{NodeID: "n1", RaftAddr: g1Addrs["n1"]},
					{NodeID: "n2", RaftAddr: g1Addrs["n2"]},
					{NodeID: "n3", RaftAddr: g1Addrs["n3"]},
				},
			},
			{
				ID: "g2",
				Replicas: []cluster.GroupReplica{
					{NodeID: "n1", RaftAddr: g2Addrs["n1"]},
					{NodeID: "n2", RaftAddr: g2Addrs["n2"]},
					{NodeID: "n3", RaftAddr: g2Addrs["n3"]},
				},
			},
		},
	}
	return config, config.Validate()
}

func mustFreeLocalAddr() string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("allocate local port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func writeClusterConfig(path string, config cluster.Config) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

func startCluster(rootDir, clusterPath string, config cluster.Config) ([]startedNode, error) {
	binaryPath, err := buildNodeBinary(rootDir)
	if err != nil {
		return nil, err
	}

	nodes := make([]startedNode, 0, len(config.Nodes))
	for i, node := range config.Nodes {
		args := []string{
			"--mode", "raft",
			"--node-id", node.ID,
			"--cluster", clusterPath,
			"--dir", filepath.Join(rootDir, node.ID),
		}
		if i == 0 {
			args = append(args, "--bootstrap")
		}

		cmd := exec.Command(binaryPath, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return nodes, fmt.Errorf("start %s: %w", node.ID, err)
		}
		nodes = append(nodes, startedNode{id: node.ID, cmd: cmd})
	}
	return nodes, nil
}

func buildNodeBinary(rootDir string) (string, error) {
	binaryPath := filepath.Join(rootDir, "kv-node")
	cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/kv-node")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build kv-node binary: %w", err)
	}
	return binaryPath, nil
}

func stopNodes(nodes []startedNode) {
	for i := len(nodes) - 1; i >= 0; i-- {
		node := nodes[i]
		if node.cmd == nil || node.cmd.Process == nil {
			continue
		}
		_ = node.cmd.Process.Signal(os.Interrupt)
	}

	deadline := time.After(5 * time.Second)
	for _, node := range nodes {
		if node.cmd == nil {
			continue
		}
		done := make(chan error, 1)
		go func(cmd *exec.Cmd) {
			done <- cmd.Wait()
		}(node.cmd)
		select {
		case err := <-done:
			if err != nil && !isSignalExit(err) {
				log.Printf("node %s exited: %v", node.id, err)
			}
		case <-deadline:
			_ = node.cmd.Process.Kill()
			_ = node.cmd.Wait()
		}
	}
}

func isSignalExit(err error) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	return true
}

func buildSamples(prefix string, count int, valueSize int, router *shard.Router) []sample {
	groups := routedGroups(router)
	samples := make([]sample, 0, count)
	for i := 0; i < count; i++ {
		samples = append(samples, buildSample(prefix, i, valueSize, router, groups))
	}
	return samples
}

func buildSample(prefix string, index int, valueSize int, router *shard.Router, groups []shard.GroupID) sample {
	target := shard.GroupID("")
	if len(groups) > 0 {
		target = groups[index%len(groups)]
	}
	for attempt := 0; attempt < 10000; attempt++ {
		key := []byte(fmt.Sprintf("%s/%s/%06d/%04d", prefix, target, index, attempt))
		route, err := router.RouteKey(key)
		if err != nil {
			log.Fatalf("route key %q: %v", key, err)
		}
		if target == "" || route.GroupID == target {
			return sample{
				key:     key,
				value:   buildValue(index, valueSize),
				groupID: route.GroupID,
			}
		}
	}
	log.Fatalf("no key found for group %q at sample index %d", target, index)
	return sample{}
}

func routedGroups(router *shard.Router) []shard.GroupID {
	seen := make(map[shard.GroupID]struct{})
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("route-probe/%06d", i))
		route, err := router.RouteKey(key)
		if err != nil {
			log.Fatalf("route probe %q: %v", key, err)
		}
		seen[route.GroupID] = struct{}{}
	}

	groups := make([]shard.GroupID, 0, len(seen))
	for groupID := range seen {
		groups = append(groups, groupID)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i] < groups[j]
	})
	return groups
}

func buildAdhocSample(prefix string, index int, valueSize int, router *shard.Router) sample {
	key := []byte(fmt.Sprintf("%s/%06d", prefix, index))
	route, err := router.RouteKey(key)
	if err != nil {
		log.Fatalf("route key %q: %v", key, err)
	}
	return sample{
		key:     key,
		value:   buildValue(index, valueSize),
		groupID: route.GroupID,
	}
}

func buildValue(index int, valueSize int) []byte {
	base := []byte(fmt.Sprintf("value-%06d", index))
	if valueSize <= 0 {
		return base
	}
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = base[i%len(base)]
	}
	return value
}

func measure(name string, count int, fn func()) time.Duration {
	start := time.Now()
	fn()
	elapsed := time.Since(start)
	opsPerSec := float64(count) / elapsed.Seconds()
	fmt.Printf("%s: %d ops in %s, %.1f ops/sec\n", name, count, elapsed, opsPerSec)
	return elapsed
}

func retry(ctx context.Context, fn func() error) error {
	_, err := retryMeasured(ctx, fn)
	return err
}

func retryMeasured(ctx context.Context, fn func() error) (int, error) {
	var lastErr error
	for attempt := 1; attempt <= 60; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return attempt, lastErr
			}
			return attempt, err
		}
		if err := fn(); err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		return attempt, nil
	}
	return 60, lastErr
}

func verifyLocalReplicas(config cluster.Config, rootDir string, samples []sample) (consistencyReport, error) {
	byGroup := make(map[shard.GroupID][]sample)
	for _, sample := range samples {
		byGroup[sample.groupID] = append(byGroup[sample.groupID], sample)
	}

	groupIDs := make([]string, 0, len(byGroup))
	for groupID := range byGroup {
		groupIDs = append(groupIDs, string(groupID))
	}
	sort.Strings(groupIDs)

	report := consistencyReport{
		VerifiedKeys: len(samples),
	}
	for _, groupIDString := range groupIDs {
		groupID := shard.GroupID(groupIDString)
		group, err := config.FindGroup(groupID)
		if err != nil {
			return report, err
		}
		for _, replica := range group.Replicas {
			dir := filepath.Join(rootDir, replica.NodeID, "engine", "group-"+string(groupID))
			result, err := verifyLocalDB(dir, byGroup[groupID])
			report.MissingCount += result.MissingCount
			report.MismatchCount += result.MismatchCount
			report.ReplicaVerifiedCount++
			if err != nil {
				return report, fmt.Errorf("node %s group %s: %w", replica.NodeID, groupID, err)
			}
		}
	}
	return report, nil
}

func verifyLocalDB(dir string, samples []sample) (consistencyReport, error) {
	db, err := kv.Open(kv.Options{Dir: dir})
	if err != nil {
		return consistencyReport{}, err
	}
	defer db.Close()

	report := consistencyReport{VerifiedKeys: len(samples)}
	for _, sample := range samples {
		got, err := db.Get(sample.key)
		if err != nil {
			return report, err
		}
		if len(got) == 0 && len(sample.value) != 0 {
			report.MissingCount++
			return report, fmt.Errorf("get %q is missing", sample.key)
		}
		if !bytes.Equal(got, sample.value) {
			report.MismatchCount++
			return report, fmt.Errorf("get %q = %q, want %q", sample.key, got, sample.value)
		}
	}
	return report, nil
}

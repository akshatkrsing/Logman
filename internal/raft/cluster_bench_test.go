package raft

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	hashiraft "github.com/hashicorp/raft"
)

func setupBenchmarkCluster(b *testing.B) (*Node, []*Node) {
	b.Helper()

	baseDir := b.TempDir()

	node1, err := NewNode(
		"bench-node1",
		"127.0.0.1:0",
		baseDir+"/node1",
		true,
	)
	if err != nil {
		b.Fatal("node1 startup failed:", err)
	}

	node2, err := NewNode(
		"bench-node2",
		"127.0.0.1:0",
		baseDir+"/node2",
		false,
	)
	if err != nil {
		node1.Shutdown()
		b.Fatal("node2 startup failed:", err)
	}

	node3, err := NewNode(
		"bench-node3",
		"127.0.0.1:0",
		baseDir+"/node3",
		false,
	)
	if err != nil {
		node1.Shutdown()
		node2.Shutdown()
		b.Fatal("node3 startup failed:", err)
	}

	nodes := []*Node{node1, node2, node3}

	b.Cleanup(func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			if err := nodes[i].Shutdown(); err != nil {
				b.Log("shutdown warning:", err)
			}
		}
	})

	// Wait for the initial leader.
	deadline := time.Now().Add(20 * time.Second)

	var leader *Node

	for time.Now().Before(deadline) {
		if node1.Raft.State() == hashiraft.Leader {
			leader = node1
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if leader == nil {
		b.Fatal("initial Raft leader election timed out")
	}

	// Add the second voter.
	if err := leader.AddVoter(
		"bench-node2",
		fmt.Sprint(node2.Transport.LocalAddr()),
	); err != nil {
		b.Fatal("failed to add node2:", err)
	}

	// Add the third voter.
	if err := leader.AddVoter(
		"bench-node3",
		fmt.Sprint(node3.Transport.LocalAddr()),
	); err != nil {
		b.Fatal("failed to add node3:", err)
	}

	// Wait until all three nodes are voters.
	deadline = time.Now().Add(20 * time.Second)

	for time.Now().Before(deadline) {
		config := leader.Raft.GetConfiguration()

		if config.Error() == nil &&
			len(config.Configuration().Servers) == 3 {
			return leader, nodes
		}

		time.Sleep(100 * time.Millisecond)
	}

	b.Fatal("three-node Raft configuration was not established")
	return nil, nil
}

func BenchmarkRaftApplyThreeNodes(b *testing.B) {
	leader, _ := setupBenchmarkCluster(b)

	var counter atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := counter.Add(1)

			entry := LogEntry{
				ID:          fmt.Sprintf("bench-%d", id),
				RequestID:   fmt.Sprintf("request-%d", id),
				Message:     "Benchmark distributed log ingestion",
				Level:       "INFO",
				ServiceName: "benchmark-service",
				Timestamp:   time.Now().Unix(),
				Metadata: map[string]string{
					"environment": "benchmark",
				},
			}

			data, err := json.Marshal(entry)
			if err != nil {
				b.Error(err)
				return
			}

			future := leader.Raft.Apply(data, 10*time.Second)

			if err := future.Error(); err != nil {
				b.Errorf("Raft apply failed: %v", err)
				return
			}

			if result, ok := future.Response().(error); ok {
				b.Errorf("FSM apply failed: %v", result)
				return
			}
		}
	})

	b.StopTimer()

	b.ReportMetric(
		float64(counter.Load())/b.Elapsed().Seconds(),
		"logs/sec",
	)
}

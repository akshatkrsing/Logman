package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akshatkrsing/logman/internal/api"
	"github.com/akshatkrsing/logman/internal/config"
	raftnode "github.com/akshatkrsing/logman/internal/raft"

	"github.com/akshatkrsing/logman/internal/observability"
	hashiraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

const (
	grpcPort       = "50051"
	leaderWaitTime = 60 * time.Second
)

func loadConfigurations() ([]*config.Config, error) {
	paths := []string{
		"./configs/node1.yaml",
		"./configs/node2.yaml",
		"./configs/node3.yaml",
	}

	configurations := make([]*config.Config, 0, len(paths))

	for _, path := range paths {
		cfg, err := config.Load(path)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", path, err)
		}

		configurations = append(configurations, cfg)
	}

	return configurations, nil
}

func findLeader(nodes []*raftnode.Node) *raftnode.Node {
	for _, node := range nodes {
		if node != nil &&
			node.Raft != nil &&
			node.Raft.State() == hashiraft.Leader {
			return node
		}
	}

	return nil
}

func waitForLeader(
	ctx context.Context,
	nodes []*raftnode.Node,
) (*raftnode.Node, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		if leader := findLeader(nodes); leader != nil {
			return leader, nil
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for Raft leader: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func hasVoter(leader *raftnode.Node, nodeID string) (bool, error) {
	future := leader.Raft.GetConfiguration()

	if err := future.Error(); err != nil {
		return false, err
	}

	for _, server := range future.Configuration().Servers {
		if string(server.ID) == nodeID &&
			server.Suffrage == hashiraft.Voter {
			return true, nil
		}
	}

	return false, nil
}

func ensureClusterMembership(
	ctx context.Context,
	nodes []*raftnode.Node,
	configurations []*config.Config,
) error {
	for _, node := range nodes {
		if node.Raft.State() != hashiraft.Leader {
			continue
		}

		for i, member := range nodes {
			if member == node {
				continue
			}

			cfg := configurations[i]

			exists, err := hasVoter(node, cfg.NodeID)
			if err != nil {
				return fmt.Errorf("read Raft configuration: %w", err)
			}

			if exists {
				log.Printf("Node %s is already a cluster voter", cfg.NodeID)
				continue
			}

			log.Printf(
				"Adding node %s at %s",
				cfg.NodeID,
				cfg.RaftBindAddress,
			)

			if err := node.AddVoter(
				cfg.NodeID,
				cfg.RaftBindAddress,
			); err != nil {
				return fmt.Errorf("add voter %s: %w", cfg.NodeID, err)
			}

			log.Printf("Node %s successfully added", cfg.NodeID)
		}

		return nil
	}

	_, err := waitForLeader(ctx, nodes)
	if err != nil {
		return err
	}

	return ensureClusterMembership(ctx, nodes, configurations)
}

func runGRPCWithLeaderFailover(
	ctx context.Context,
	nodes []*raftnode.Node,
	metrics *observability.Metrics,
) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var activeLeader *raftnode.Node
	var grpcServer *grpc.Server

	stopGRPC := func() {
		if grpcServer != nil {
			log.Println("Stopping gRPC service before leader transition")
			grpcServer.GracefulStop()
			grpcServer = nil
			activeLeader = nil
		}
	}

	defer stopGRPC()

	for {
		leader := findLeader(nodes)

		if leader != activeLeader {
			stopGRPC()

			if leader != nil {
				server, err := api.StartGRPCServer(grpcPort, leader, metrics)
				if err != nil {
					return fmt.Errorf("start gRPC server: %w", err)
				}

				grpcServer = server
				activeLeader = leader

				log.Printf(
					"gRPC service is now connected to the active Raft leader on port %s",
					grpcPort,
				)
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func runRetention(
	ctx context.Context,
	nodes []*raftnode.Node,
	interval time.Duration,
	retentionAge time.Duration,
) {
	if interval <= 0 || retentionAge <= 0 {
		log.Println("Automatic log retention is disabled")
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf(
		"Automatic retention enabled: interval=%s, age=%s",
		interval,
		retentionAge,
	)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			leader := findLeader(nodes)

			if leader == nil {
				log.Println("Retention skipped: no Raft leader available")
				continue
			}

			command := raftnode.BatchCommand{
				Type:            "retention",
				BeforeTimestamp: time.Now().Add(-retentionAge).Unix(),
			}

			data, err := json.Marshal(command)
			if err != nil {
				log.Printf("Retention serialization failed: %v", err)
				continue
			}

			future := leader.Raft.Apply(data, 30*time.Second)

			if err := future.Error(); err != nil {
				log.Printf("Retention operation failed: %v", err)
				continue
			}

			removed, ok := future.Response().(int)
			if !ok {
				log.Println("Retention returned an unexpected response")
				continue
			}

			log.Printf("Retention completed: removed %d expired logs", removed)
		}
	}
}

func main() {
	log.Println("Starting Logman distributed log cluster")

	configurations, err := loadConfigurations()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	nodes := make([]*raftnode.Node, 0, len(configurations))

	shutdownNodes := func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			if err := nodes[i].Shutdown(); err != nil {
				log.Printf("Node shutdown error: %v", err)
			}
		}
	}

	defer shutdownNodes()

	for _, cfg := range configurations {
		log.Printf("Starting Raft node %s", cfg.NodeID)

		node, err := raftnode.NewNode(
			cfg.NodeID,
			cfg.RaftBindAddress,
			cfg.DataDir,
			cfg.Bootstrap,
		)

		if err != nil {
			log.Fatalf("Failed to start node %s: %v", cfg.NodeID, err)
		}

		nodes = append(nodes, node)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	leaderCtx, cancel := context.WithTimeout(ctx, leaderWaitTime)

	_, err = waitForLeader(leaderCtx, nodes)
	cancel()

	if err != nil {
		log.Fatalf("Cluster leader election failed: %v", err)
	}

	log.Println("Initial Raft leader elected")

	if err := ensureClusterMembership(ctx, nodes, configurations); err != nil {
		log.Fatalf("Cluster membership initialization failed: %v", err)
	}

	// Initialize Prometheus metrics.
	metrics := observability.NewMetrics()
	metrics.UpdateNodes(nodes)

	// Start HTTP health and monitoring endpoints.
	monitoringServer := observability.NewHTTPServer(
		":8081",
		nodes,
		metrics,
	)

	go func() {
		log.Println("Monitoring HTTP server listening on :8081")

		if err := monitoringServer.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			log.Printf("Monitoring server error: %v", err)
			stop()
		}
	}()

	log.Println("Logman three-node cluster is running")

	// Start automatic log retention.
	retentionInterval := time.Duration(configurations[0].RetentionInterval)
	retentionAge := time.Duration(configurations[0].RetentionAge)

	go runRetention(
		ctx,
		nodes,
		retentionInterval,
		retentionAge,
	)

	// Start gRPC and handle Raft leader changes.
	if err := runGRPCWithLeaderFailover(ctx, nodes, metrics); err != nil {
		log.Printf("Runtime error: %v", err)
	}

	// Gracefully stop monitoring HTTP server.
	log.Println("Shutting down monitoring server")

	shutdownCtx, cancelShutdown := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancelShutdown()

	if err := monitoringServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Monitoring server shutdown error: %v", err)
	}

	log.Println("Logman shutting down gracefully")
}

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/akshatkrsing/logman/internal/api"
	raftnode "github.com/akshatkrsing/logman/internal/raft"

	hashiraft "github.com/hashicorp/raft"
)

func waitForLeader(nodes []*raftnode.Node, timeout time.Duration) *raftnode.Node {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.Raft.State() == hashiraft.Leader {
				return node
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	return nil
}

func printNodeStates(nodes []*raftnode.Node) {
	for i, node := range nodes {
		fmt.Printf(
			"Node %d | State: %s | Leader: %s\n",
			i+1,
			node.Raft.State(),
			node.Raft.Leader(),
		)
	}
}

func hasLog(node *raftnode.Node, logID string) bool {
	for _, entry := range node.FSM.GetLogs() {
		if entry.ID == logID {
			return true
		}
	}
	return false
}

func main() {
	node1, err := raftnode.NewNode("node1", "127.0.0.1:6001", "./data/node1", true)
	if err != nil {
		log.Fatal("Node 1 startup failed:", err)
	}

	node2, err := raftnode.NewNode("node2", "127.0.0.1:6002", "./data/node2", false)
	if err != nil {
		node1.Shutdown()
		log.Fatal("Node 2 startup failed:", err)
	}

	node3, err := raftnode.NewNode("node3", "127.0.0.1:6003", "./data/node3", false)
	if err != nil {
		node1.Shutdown()
		node2.Shutdown()
		log.Fatal("Node 3 startup failed:", err)
	}

	nodes := []*raftnode.Node{node1, node2, node3}

	fmt.Println("All three Raft nodes started.")

	leader := waitForLeader(nodes, 20*time.Second)
	if leader == nil {
		printNodeStates(nodes)
		log.Fatal("Initial leader election timed out")
	}

	fmt.Println("\nInitial leader elected:", leader.Raft.Leader())
	printNodeStates(nodes)

	// Remember the failed node's original identity and storage directory.
	var failedID, failedAddress, failedDataDir string

	switch leader {
	case node1:
		failedID, failedAddress, failedDataDir = "node1", "127.0.0.1:6001", "./data/node1"
	case node2:
		failedID, failedAddress, failedDataDir = "node2", "127.0.0.1:6002", "./data/node2"
	case node3:
		failedID, failedAddress, failedDataDir = "node3", "127.0.0.1:6003", "./data/node3"
	}

	grpcServer, err := api.StartGRPCServer("50051", leader)
	if err != nil {
		log.Fatal("gRPC startup failed:", err)
	}

	fmt.Println("\ngRPC server started on port 50051.")
	fmt.Println("Leader failure simulation begins in 20 seconds...")
	time.Sleep(20 * time.Second)

	fmt.Println("\nSimulating failure of:", failedID)
	grpcServer.Stop()

	if err := leader.Shutdown(); err != nil {
		log.Println("Old leader shutdown warning:", err)
	}

	fmt.Println("Old leader stopped.")
	fmt.Println("Waiting for a new leader...")

	var survivors []*raftnode.Node
	for _, node := range nodes {
		if node != leader {
			survivors = append(survivors, node)
		}
	}

	newLeader := waitForLeader(survivors, 30*time.Second)
	if newLeader == nil {
		printNodeStates(survivors)
		log.Fatal("Failover failed: no new leader elected")
	}

	fmt.Println("\n================================")
	fmt.Println("AUTOMATIC FAILOVER SUCCESSFUL")
	fmt.Println("================================")
	fmt.Println("New leader:", newLeader.Raft.Leader())
	printNodeStates(survivors)

	grpcServer, err = api.StartGRPCServer("50051", newLeader)
	if err != nil {
		log.Fatal("gRPC restart failed:", err)
	}

	fmt.Println("\nLogman is operational on the new leader.")
	fmt.Println("\nSend a log with ID: test-recovery-001")
	fmt.Println("The application will wait for this log before restarting the failed node.")

	// Wait for a fresh write committed while the old leader is offline.
	deadline := time.Now().Add(90 * time.Second)

	for time.Now().Before(deadline) {
		if hasLog(newLeader, "test-recovery-001") {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if !hasLog(newLeader, "test-recovery-001") {
		log.Fatal("Timed out waiting for test-recovery-001")
	}

	fmt.Println("\nNew log committed while failed node was offline.")

	// Restart the failed node with its original ID, address and persistent data.
	fmt.Println("\nRestarting failed node:", failedID)

	recoveredNode, err := raftnode.NewNode(
		failedID,
		failedAddress,
		failedDataDir,
		false,
	)
	if err != nil {
		log.Fatal("Failed node recovery startup failed:", err)
	}

	fmt.Println("Failed node restarted successfully.")

	// Wait for the restarted node to receive the missing log.
	deadline = time.Now().Add(45 * time.Second)

	for time.Now().Before(deadline) {
		if hasLog(recoveredNode, "test-recovery-001") {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if !hasLog(recoveredNode, "test-recovery-001") {
		log.Fatal("Recovery verification failed: restarted node did not receive test-recovery-001")
	}

	fmt.Println("\n================================")
	fmt.Println("NODE RECOVERY SUCCESSFUL")
	fmt.Println("================================")
	fmt.Printf(
		"Node %s recovered and applied test-recovery-001\n",
		failedID,
	)

	allNodes := append(survivors, recoveredNode)
	printNodeStates(allNodes)

	fmt.Println("\nLogman cluster recovery test completed.")

	select {}
}

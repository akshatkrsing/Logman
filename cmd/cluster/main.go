
package main

import (
    "fmt"
    "log"
    "time"

    raftnode "github.com/akshatkrsing/logman/internal/raft"
)

func main() {
    // Start Node 1 and bootstrap the cluster.
    node1, err := raftnode.NewNode(
        "node1", "127.0.0.1:6001",
        "./data/node1", true,
    )
    if err != nil {
        log.Fatal(err)
    }
    defer node1.Raft.Shutdown()

    // Wait for initial leader election.
    deadline := time.Now().Add(10 * time.Second)

    for node1.Raft.Leader() == "" {
        if time.Now().After(deadline) {
            log.Fatal("Leader election timed out")
        }
        time.Sleep(200 * time.Millisecond)
    }

    fmt.Println("Node 1 elected as:", node1.Raft.State())

    // Start Node 2.
    node2, err := raftnode.NewNode(
        "node2", "127.0.0.1:6002",
        "./data/node2", false,
    )
    if err != nil {
        log.Fatal(err)
    }
    defer node2.Raft.Shutdown()

    // Start Node 3.
    node3, err := raftnode.NewNode(
        "node3", "127.0.0.1:6003",
        "./data/node3", false,
    )
    if err != nil {
        log.Fatal(err)
    }
    defer node3.Raft.Shutdown()

    // Add both nodes through the leader.
    if err := node1.AddVoter("node2", "127.0.0.1:6002"); err != nil {
        log.Fatal("Node 2 join failed:", err)
    }

    fmt.Println("Node 2 joined successfully")

    if err := node1.AddVoter("node3", "127.0.0.1:6003"); err != nil {
        log.Fatal("Node 3 join failed:", err)
    }

    fmt.Println("Node 3 joined successfully")

    fmt.Println("\nCluster formation completed")

    fmt.Println("Node 1:", node1.Raft.State())
    fmt.Println("Node 2:", node2.Raft.State())
    fmt.Println("Node 3:", node3.Raft.State())

    time.Sleep(30 * time.Second)
}

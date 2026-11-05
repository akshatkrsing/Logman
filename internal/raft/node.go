
package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	hashiraft "github.com/hashicorp/raft"
	bolt "github.com/hashicorp/raft-boltdb/v2"
)

type Node struct {
	Raft *hashiraft.Raft
	FSM  *FSM
}

func NewNode(nodeID, raftAddress, dataDir string, bootstrap bool) (*Node, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	config := hashiraft.DefaultConfig()
	config.LocalID = hashiraft.ServerID(nodeID)

	addr, err := hashiraft.NewTCPTransport(
		raftAddress,
		nil,
		3,
		10*time.Second,
		os.Stdout,
	)
	if err != nil {
		return nil, fmt.Errorf("create raft transport: %w", err)
	}

	store, err := bolt.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		addr.Close()
		return nil, fmt.Errorf("create raft store: %w", err)
	}

	snapshotStore, err := hashiraft.NewFileSnapshotStore(dataDir, 2, os.Stdout)
	if err != nil {
		addr.Close()
		store.Close()
		return nil, err
	}

	fsm := NewFSM()

	raftNode, err := hashiraft.NewRaft(
		config,
		fsm,
		store,
		store,
		snapshotStore,
		addr,
	)
	if err != nil {
		addr.Close()
		store.Close()
		return nil, err
	}

	node := &Node{
		Raft: raftNode,
		FSM:  fsm,
	}

	if bootstrap {
		configuration := hashiraft.Configuration{
			Servers: []hashiraft.Server{
				{
					ID:      hashiraft.ServerID(nodeID),
					Address: addr.LocalAddr(),
				},
			},
		}

		future := raftNode.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			raftNode.Shutdown()
			return nil, fmt.Errorf("bootstrap cluster: %w", err)
		}
	}

	return node, nil
}

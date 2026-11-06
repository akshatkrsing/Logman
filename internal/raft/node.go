package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	hashiraft "github.com/hashicorp/raft"
	bolt "github.com/hashicorp/raft-boltdb/v2"
)

type Node struct {
	Raft      *hashiraft.Raft
	FSM       *FSM
	Transport *hashiraft.NetworkTransport
	Store     *bolt.BoltStore
}

func NewNode(nodeID, raftAddress, dataDir string, bootstrap bool) (*Node, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	config := hashiraft.DefaultConfig()
	config.LocalID = hashiraft.ServerID(nodeID)

	transport, err := hashiraft.NewTCPTransport(
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
		transport.Close()
		return nil, fmt.Errorf("create raft store: %w", err)
	}

	snapshotStore, err := hashiraft.NewFileSnapshotStore(dataDir, 2, os.Stdout)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, err
	}

	fsm := NewFSM(nodeID)

	raftNode, err := hashiraft.NewRaft(
		config,
		fsm,
		store,
		store,
		snapshotStore,
		transport,
	)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, err
	}

	node := &Node{
		Raft:      raftNode,
		FSM:       fsm,
		Transport: transport,
		Store:     store,
	}

	if bootstrap {
		configurationFuture := raftNode.GetConfiguration()

		if err := configurationFuture.Error(); err != nil {
			node.Shutdown()
			return nil, fmt.Errorf("get raft configuration: %w", err)
		}

		if len(configurationFuture.Configuration().Servers) == 0 {
			configuration := hashiraft.Configuration{
				Servers: []hashiraft.Server{
					{
						ID:      hashiraft.ServerID(nodeID),
						Address: transport.LocalAddr(),
					},
				},
			}

			future := raftNode.BootstrapCluster(configuration)

			if err := future.Error(); err != nil {
				node.Shutdown()
				return nil, fmt.Errorf("bootstrap cluster: %w", err)
			}
		}
	}

	return node, nil
}

func (n *Node) Join(nodeID, raftAddress string) error {
	return n.AddVoter(nodeID, raftAddress)
}

func (n *Node) AddVoter(nodeID, raftAddress string) error {
	future := n.Raft.AddVoter(
		hashiraft.ServerID(nodeID),
		hashiraft.ServerAddress(raftAddress),
		0,
		10*time.Second,
	)

	return future.Error()
}

func (n *Node) Shutdown() error {
	var shutdownErrors []error

	if n.Raft != nil {
		if err := n.Raft.Shutdown().Error(); err != nil {
			shutdownErrors = append(shutdownErrors, err)
		}
	}

	if n.Transport != nil {
		if err := n.Transport.Close(); err != nil {
			shutdownErrors = append(shutdownErrors, err)
		}
	}

	if n.Store != nil {
		if err := n.Store.Close(); err != nil {
			shutdownErrors = append(shutdownErrors, err)
		}
	}

	return errors.Join(shutdownErrors...)
}

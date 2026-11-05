
package raft

import (
	"encoding/json"
	"io"
	"sync"

	hashiraft "github.com/hashicorp/raft"
)

type LogEntry struct {
	ID          string `json:"id"`
	RequestID   string `json:"request_id"`
	Message     string `json:"message"`
	Level       string `json:"level"`
	ServiceName string `json:"service_name"`
	Timestamp   int64  `json:"timestamp"`
}

type FSM struct {
	mu   sync.RWMutex
	logs []LogEntry
}

func NewFSM() *FSM {
	return &FSM{
		logs: make([]LogEntry, 0),
	}
}

func (f *FSM) Apply(logEntry *hashiraft.Log) interface{} {
	var entry LogEntry

	if err := json.Unmarshal(logEntry.Data, &entry); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.logs = append(f.logs, entry)
	return nil
}

func (f *FSM) Snapshot() (hashiraft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	logs := make([]LogEntry, len(f.logs))
	copy(logs, f.logs)

	return &Snapshot{logs: logs}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var logs []LogEntry
	if err := json.NewDecoder(rc).Decode(&logs); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.logs = logs
	return nil
}

func (f *FSM) GetLogs() []LogEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make([]LogEntry, len(f.logs))
	copy(result, f.logs)

	return result
}

type Snapshot struct {
	logs []LogEntry
}

func (s *Snapshot) Persist(sink hashiraft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.logs)
	if err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *Snapshot) Release() {}

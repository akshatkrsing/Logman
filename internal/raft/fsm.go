package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	hashiraft "github.com/hashicorp/raft"
)

type LogEntry struct {
	ID          string            `json:"id"`
	RequestID   string            `json:"request_id"`
	Message     string            `json:"message"`
	Level       string            `json:"level"`
	ServiceName string            `json:"service_name"`
	Timestamp   int64             `json:"timestamp"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type BatchCommand struct {
	Type    string     `json:"type"`
	Entries []LogEntry `json:"entries"`
}

type MetadataKey struct {
	Key   string
	Value string
}

// LogQuery contains exact-match filters used by the FSM indexes.
type LogQuery struct {
	RequestID   string
	Level       string
	Levels      []string
	ServiceName string
	Metadata    map[string]string
}

type FSM struct {
	mu     sync.RWMutex
	logs   []LogEntry
	nodeID string

	requestIDIndex map[string]map[int]struct{}
	levelIndex     map[string]map[int]struct{}
	serviceIndex   map[string]map[int]struct{}
	metadataIndex  map[MetadataKey]map[int]struct{}
}

func NewFSM(nodeID string) *FSM {
	return &FSM{
		logs:           make([]LogEntry, 0),
		nodeID:         nodeID,
		requestIDIndex: make(map[string]map[int]struct{}),
		levelIndex:     make(map[string]map[int]struct{}),
		serviceIndex:   make(map[string]map[int]struct{}),
		metadataIndex:  make(map[MetadataKey]map[int]struct{}),
	}
}

func cloneLogEntry(entry LogEntry) LogEntry {
	cloned := entry

	if entry.Metadata != nil {
		cloned.Metadata = make(map[string]string, len(entry.Metadata))

		for key, value := range entry.Metadata {
			cloned.Metadata[key] = value
		}
	}

	return cloned
}

func cloneLogs(logs []LogEntry) []LogEntry {
	result := make([]LogEntry, len(logs))

	for i, entry := range logs {
		result[i] = cloneLogEntry(entry)
	}

	return result
}

func addToIndex[K comparable](
	index map[K]map[int]struct{},
	key K,
	position int,
) {
	if _, exists := index[key]; !exists {
		index[key] = make(map[int]struct{})
	}

	index[key][position] = struct{}{}
}

func (f *FSM) indexEntryLocked(entry LogEntry, position int) {
	if entry.RequestID != "" {
		addToIndex(f.requestIDIndex, entry.RequestID, position)
	}

	if entry.Level != "" {
		addToIndex(
			f.levelIndex,
			strings.ToUpper(strings.TrimSpace(entry.Level)),
			position,
		)
	}

	if entry.ServiceName != "" {
		addToIndex(
			f.serviceIndex,
			strings.ToLower(strings.TrimSpace(entry.ServiceName)),
			position,
		)
	}

	for key, value := range entry.Metadata {
		addToIndex(f.metadataIndex, MetadataKey{
			Key:   key,
			Value: value,
		}, position)
	}
}

func (f *FSM) rebuildIndexesLocked() {
	f.requestIDIndex = make(map[string]map[int]struct{})
	f.levelIndex = make(map[string]map[int]struct{})
	f.serviceIndex = make(map[string]map[int]struct{})
	f.metadataIndex = make(map[MetadataKey]map[int]struct{})

	for position, entry := range f.logs {
		f.indexEntryLocked(entry, position)
	}
}

func (f *FSM) Apply(log *hashiraft.Log) interface{} {
	var command BatchCommand

	if err := json.Unmarshal(log.Data, &command); err == nil && command.Type == "batch" {
		if len(command.Entries) == 0 {
			return fmt.Errorf("batch cannot be empty")
		}

		f.mu.Lock()
		defer f.mu.Unlock()

		results := make([]LogEntry, 0, len(command.Entries))

		for _, entry := range command.Entries {
			results = append(results, f.applyEntryLocked(entry))
		}

		return results
	}

	// Backward compatibility with previously committed single-log entries.
	var entry LogEntry

	if err := json.Unmarshal(log.Data, &entry); err != nil {
		return fmt.Errorf("unmarshal log entry: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.applyEntryLocked(entry)
}

func (f *FSM) applyEntryLocked(entry LogEntry) LogEntry {
	if entry.RequestID != "" {
		if positions, exists := f.requestIDIndex[entry.RequestID]; exists {
			for position := range positions {
				if position >= 0 && position < len(f.logs) {
					return cloneLogEntry(f.logs[position])
				}
			}
		}
	}

	f.logs = append(f.logs, cloneLogEntry(entry))

	position := len(f.logs) - 1
	f.indexEntryLocked(entry, position)

	return cloneLogEntry(entry)
}

func (f *FSM) Snapshot() (hashiraft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return &Snapshot{
		logs: cloneLogs(f.logs),
	}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var logs []LogEntry

	if err := json.NewDecoder(rc).Decode(&logs); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.logs = cloneLogs(logs)
	f.rebuildIndexesLocked()

	return nil
}

func (f *FSM) GetLogs() []LogEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return cloneLogs(f.logs)
}

// SearchLogs uses indexes to reduce the candidate set.
// Results retain their original Raft application order.
func (f *FSM) SearchLogs(query LogQuery) []LogEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()

	indexSets := make([]map[int]struct{}, 0)

	if query.RequestID != "" {
		indexSets = append(indexSets, f.requestIDIndex[query.RequestID])
	}

	if query.Level != "" {
		indexSets = append(
			indexSets,
			f.levelIndex[strings.ToUpper(strings.TrimSpace(query.Level))],
		)
	}

	if len(query.Levels) > 0 {
		levelPositions := make(map[int]struct{})

		for _, level := range query.Levels {
			normalized := strings.ToUpper(strings.TrimSpace(level))

			if normalized == "" {
				continue
			}

			for position := range f.levelIndex[normalized] {
				levelPositions[position] = struct{}{}
			}
		}

		indexSets = append(indexSets, levelPositions)
	}

	if query.ServiceName != "" {
		indexSets = append(
			indexSets,
			f.serviceIndex[strings.ToLower(strings.TrimSpace(query.ServiceName))],
		)
	}

	for key, value := range query.Metadata {
		indexSets = append(indexSets, f.metadataIndex[MetadataKey{
			Key:   key,
			Value: value,
		}])
	}

	if len(indexSets) == 0 {
		return cloneLogs(f.logs)
	}

	for _, positions := range indexSets {
		if len(positions) == 0 {
			return []LogEntry{}
		}
	}

	sort.Slice(indexSets, func(i, j int) bool {
		return len(indexSets[i]) < len(indexSets[j])
	})

	candidates := indexSets[0]
	matchedPositions := make([]int, 0, len(candidates))

	for position := range candidates {
		matches := true

		for _, positions := range indexSets[1:] {
			if _, exists := positions[position]; !exists {
				matches = false
				break
			}
		}

		if matches {
			matchedPositions = append(matchedPositions, position)
		}
	}

	sort.Ints(matchedPositions)

	result := make([]LogEntry, 0, len(matchedPositions))

	for _, position := range matchedPositions {
		result = append(result, cloneLogEntry(f.logs[position]))
	}

	return result
}

type Snapshot struct {
	logs []LogEntry
}

func (s *Snapshot) Persist(sink hashiraft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.logs); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *Snapshot) Release() {}

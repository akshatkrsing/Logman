package raft

import (
	"encoding/json"
	"fmt"
	"testing"

	hashiraft "github.com/hashicorp/raft"
)

func createBenchmarkFSM(logCount int) *FSM {
	fsm := NewFSM("benchmark-node")

	for i := 0; i < logCount; i++ {
		entry := LogEntry{
			ID:          fmt.Sprintf("log-%d", i),
			RequestID:   fmt.Sprintf("request-%d", i),
			Message:     fmt.Sprintf("Payment transaction processed %d", i),
			Level:       []string{"INFO", "WARN", "ERROR"}[i%3],
			ServiceName: []string{"payment-service", "notification-service"}[i%2],
			Timestamp:   int64(1700000000 + i),
			Metadata: map[string]string{
				"environment": []string{"production", "staging"}[i%2],
				"region":      "ap-south-1",
			},
		}

		data, err := json.Marshal(entry)
		if err != nil {
			panic(err)
		}

		result := fsm.Apply(&hashiraft.Log{
			Index: uint64(i + 1),
			Data:  data,
		})

		if err, ok := result.(error); ok {
			panic(err)
		}
	}

	return fsm
}

// Benchmark applying a new log to the FSM.
func BenchmarkFSMApply(b *testing.B) {
	fsm := NewFSM("benchmark-node")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		entry := LogEntry{
			ID:          fmt.Sprintf("log-%d", i),
			RequestID:   fmt.Sprintf("request-%d", i),
			Message:     "Payment transaction completed",
			Level:       "INFO",
			ServiceName: "payment-service",
			Timestamp:   int64(1700000000 + i),
			Metadata: map[string]string{
				"environment": "production",
				"region":      "ap-south-1",
			},
		}

		data, err := json.Marshal(entry)
		if err != nil {
			b.Fatal(err)
		}

		result := fsm.Apply(&hashiraft.Log{
			Index: uint64(i + 1),
			Data:  data,
		})

		if err, ok := result.(error); ok {
			b.Fatal(err)
		}
	}
}

// Benchmark indexed service and level filtering.
func BenchmarkFSMSearchByServiceAndLevel(b *testing.B) {
	fsm := createBenchmarkFSM(10000)

	query := LogQuery{
		ServiceName: "payment-service",
		Level:       "ERROR",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fsm.SearchLogs(query)
	}
}

// Benchmark exact request ID lookup.
func BenchmarkFSMSearchByRequestID(b *testing.B) {
	fsm := createBenchmarkFSM(10000)

	query := LogQuery{
		RequestID: "request-5000",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fsm.SearchLogs(query)
	}
}

// Benchmark metadata indexing.
func BenchmarkFSMSearchByMetadata(b *testing.B) {
	fsm := createBenchmarkFSM(10000)

	query := LogQuery{
		Metadata: map[string]string{
			"environment": "production",
			"region":      "ap-south-1",
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fsm.SearchLogs(query)
	}
}

// Benchmark retrieving all logs without indexed filters.
func BenchmarkFSMGetLogs(b *testing.B) {
	fsm := createBenchmarkFSM(10000)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fsm.GetLogs()
	}
}

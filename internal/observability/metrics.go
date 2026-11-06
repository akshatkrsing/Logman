package observability

import (
	"encoding/json"
	"net/http"

	raftnode "github.com/akshatkrsing/logman/internal/raft"
	hashiraft "github.com/hashicorp/raft"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry        *prometheus.Registry
	logsIngested    prometheus.Counter
	batchesIngested prometheus.Counter
	ingestionErrors prometheus.Counter
	nodeState       *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()

	m := &Metrics{
		registry: registry,
		logsIngested: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logman_logs_ingested_total",
			Help: "Total successfully ingested logs.",
		}),
		batchesIngested: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logman_batches_ingested_total",
			Help: "Total successfully ingested batches.",
		}),
		ingestionErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logman_ingestion_errors_total",
			Help: "Total failed ingestion operations.",
		}),
		nodeState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "logman_raft_node_state",
			Help: "Raft state: leader=1, follower=2, candidate=3, shutdown=0.",
		}, []string{"node_id", "state"}),
	}

	registry.MustRegister(
		m.logsIngested,
		m.batchesIngested,
		m.ingestionErrors,
		m.nodeState,
	)

	return m
}

func (m *Metrics) RecordLogs(count int) {
	if count > 0 {
		m.logsIngested.Add(float64(count))
	}
}

func (m *Metrics) RecordBatch() {
	m.batchesIngested.Inc()
}

func (m *Metrics) RecordError() {
	m.ingestionErrors.Inc()
}

func (m *Metrics) UpdateNodes(nodes []*raftnode.Node) {
	m.nodeState.Reset()

	for _, node := range nodes {
		if node == nil || node.Raft == nil {
			continue
		}

		switch node.Raft.State() {
		case hashiraft.Leader:
			m.nodeState.WithLabelValues(node.ID, "leader").Set(1)

		case hashiraft.Follower:
			m.nodeState.WithLabelValues(node.ID, "follower").Set(2)

		case hashiraft.Candidate:
			m.nodeState.WithLabelValues(node.ID, "candidate").Set(3)

		default:
			m.nodeState.WithLabelValues(node.ID, "shutdown").Set(0)
		}
	}
}

type HealthResponse struct {
	Status string `json:"status"`
}

func NewHTTPServer(
	address string,
	nodes []*raftnode.Node,
	metrics *Metrics,
) *http.Server {
	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.HandlerFor(
		metrics.registry,
		promhttp.HandlerOpts{},
	))

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		_ = json.NewEncoder(w).Encode(HealthResponse{
			Status: "healthy",
		})
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		leaderAvailable := false

		for _, node := range nodes {
			if node != nil &&
				node.Raft != nil &&
				node.Raft.State() == hashiraft.Leader {
				leaderAvailable = true
				break
			}
		}

		w.Header().Set("Content-Type", "application/json")

		if !leaderAvailable {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(HealthResponse{
				Status: "not_ready",
			})
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(HealthResponse{
			Status: "ready",
		})
	})

	return &http.Server{
		Addr:    address,
		Handler: mux,
	}
}

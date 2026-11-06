package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return err
	}

	*d = Duration(parsed)
	return nil
}

type Config struct {
	NodeID            string   `yaml:"node_id"`
	RaftBindAddress   string   `yaml:"raft_bind_address"`
	GRPCAddress       string   `yaml:"grpc_address"`
	DataDir           string   `yaml:"data_dir"`
	Bootstrap         bool     `yaml:"bootstrap"`
	JoinAddress       string   `yaml:"join_address"`
	SnapshotInterval  Duration `yaml:"snapshot_interval"`
	SnapshotThreshold uint64   `yaml:"snapshot_threshold"`
	MaxLogSize        int      `yaml:"max_log_size"`
	MaxBatchSize      int      `yaml:"max_batch_size"`
	RetentionInterval Duration `yaml:"retention_interval"`
	RetentionAge      Duration `yaml:"retention_age"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.applyEnvironment()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) applyEnvironment() {
	setString := func(key string, target *string) {
		if value, ok := os.LookupEnv(key); ok {
			*target = value
		}
	}

	setString("LOGMAN_NODE_ID", &c.NodeID)
	setString("LOGMAN_RAFT_ADDRESS", &c.RaftBindAddress)
	setString("LOGMAN_GRPC_ADDRESS", &c.GRPCAddress)
	setString("LOGMAN_DATA_DIR", &c.DataDir)
	setString("LOGMAN_JOIN_ADDRESS", &c.JoinAddress)

	if value, ok := os.LookupEnv("LOGMAN_BOOTSTRAP"); ok {
		if parsed, err := strconv.ParseBool(value); err == nil {
			c.Bootstrap = parsed
		}
	}

	if value, ok := os.LookupEnv("LOGMAN_SNAPSHOT_INTERVAL"); ok {
		if parsed, err := time.ParseDuration(value); err == nil {
			c.SnapshotInterval = Duration(parsed)
		}
	}

	if value, ok := os.LookupEnv("LOGMAN_SNAPSHOT_THRESHOLD"); ok {
		if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
			c.SnapshotThreshold = parsed
		}
	}

	if value, ok := os.LookupEnv("LOGMAN_MAX_LOG_SIZE"); ok {
		if parsed, err := strconv.Atoi(value); err == nil {
			c.MaxLogSize = parsed
		}
	}

	if value, ok := os.LookupEnv("LOGMAN_MAX_BATCH_SIZE"); ok {
		if parsed, err := strconv.Atoi(value); err == nil {
			c.MaxBatchSize = parsed
		}
	}

	if value, ok := os.LookupEnv("LOGMAN_RETENTION_INTERVAL"); ok {
		if parsed, err := time.ParseDuration(value); err == nil {
			c.RetentionInterval = Duration(parsed)
		}
	}

	if value, ok := os.LookupEnv("LOGMAN_RETENTION_AGE"); ok {
		if parsed, err := time.ParseDuration(value); err == nil {
			c.RetentionAge = Duration(parsed)
		}
	}
}

func (c *Config) Validate() error {
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("node_id is required")
	}

	if strings.TrimSpace(c.RaftBindAddress) == "" {
		return fmt.Errorf("raft_bind_address is required")
	}

	if strings.TrimSpace(c.GRPCAddress) == "" {
		return fmt.Errorf("grpc_address is required")
	}

	if strings.TrimSpace(c.DataDir) == "" {
		return fmt.Errorf("data_dir is required")
	}

	if _, _, err := net.SplitHostPort(c.RaftBindAddress); err != nil {
		return fmt.Errorf("invalid raft_bind_address: %w", err)
	}

	if _, _, err := net.SplitHostPort(c.GRPCAddress); err != nil {
		return fmt.Errorf("invalid grpc_address: %w", err)
	}

	if c.SnapshotInterval <= 0 {
		return fmt.Errorf("snapshot_interval must be positive")
	}

	if c.SnapshotThreshold == 0 {
		return fmt.Errorf("snapshot_threshold must be positive")
	}

	if c.MaxLogSize <= 0 {
		return fmt.Errorf("max_log_size must be positive")
	}

	if c.MaxBatchSize <= 0 {
		return fmt.Errorf("max_batch_size must be positive")
	}

	if c.Bootstrap && c.JoinAddress != "" {
		return fmt.Errorf("bootstrap node cannot specify join_address")
	}

	retentionInterval := time.Duration(c.RetentionInterval)
	retentionAge := time.Duration(c.RetentionAge)

	if retentionInterval < 0 || retentionAge < 0 {
		return fmt.Errorf("retention settings cannot be negative")
	}

	if (retentionInterval == 0) != (retentionAge == 0) {
		return fmt.Errorf("retention_interval and retention_age must both be configured or both be zero")
	}

	if retentionInterval > 0 && retentionAge < retentionInterval {
		return fmt.Errorf("retention_age must be greater than or equal to retention_interval")
	}

	return nil
}

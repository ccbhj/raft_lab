package raft

import (
	"os"
	"strings"
)

// send heartbeat every 2s
// election time out between [3, 6]s
var raftConfig *Config = &Config{
	HeartBeatIntervalMs:    2000,
	ElectionTimeoutMinMs:   3000,
	ElectionTimeoutRangeMs: 3000,
	PersistDir:             os.TempDir(),
}

// NOTE: USE ONLY MILLISECOND
// NOTE: USE ONLY MILLISECOND
type Config struct {
	HeartBeatIntervalMs    int64
	ElectionTimeoutMinMs   int64
	ElectionTimeoutRangeMs int64
	PersistDir             string
}

func TestConfig() *Config {
	return &Config{
		HeartBeatIntervalMs:    100,
		ElectionTimeoutMinMs:   200,
		ElectionTimeoutRangeMs: 200,
		PersistDir:             os.TempDir(),
	}
}

func GetRaftConfig() Config {
	if strings.ToUpper(os.Getenv("RAFT_ENV")) == "TEST" {
		return *TestConfig()
	}
	return *raftConfig
}

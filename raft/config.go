package raft

import (
	"os"
	"strings"
)

var raftConfig *Config = &Config{
	HeartBeatIntervalMs:    1000,
	ElectionTimeoutMinMs:   1200,
	ElectionTimeoutRangeMs: 500,
}

// NOTE: USE ONLY MILLISECOND
// NOTE: USE ONLY MILLISECOND
type Config struct {
	HeartBeatIntervalMs    int64
	ElectionTimeoutMinMs   int64
	ElectionTimeoutRangeMs int64
}

func TestConfig() *Config {
	return &Config{
		HeartBeatIntervalMs:    100,
		ElectionTimeoutMinMs:   200,
		ElectionTimeoutRangeMs: 400,
	}
}

func GetRaftConfig() Config {
	if strings.ToUpper(os.Getenv("RAFT_ENV")) == "TEST" {
		return *TestConfig()
	}
	return *raftConfig
}

package rpc

import (
	"context"

	log "github.com/ccbhj/raft_lab/logging"
)

const (
	RouterLogKey  = "ROUTER"
	ChannelLogKey = "CHANNEL"
)

func GetRouterLog(ctx context.Context) *log.Logger {
	return log.GetLogger(ctx, RouterLogKey)
}

func GetChannelLog(ctx context.Context) *log.Logger {
	return log.GetLogger(ctx, ChannelLogKey)
}

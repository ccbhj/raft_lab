package rpc

import (
	"context"
	"os"

	"github.com/ccbhj/raft_lab/log"
)

const (
	RouterLogKey  = "ROUTER"
	ChannelLogKey = "CHANNEL"
)

func init() {
	log.InitLogger(os.Stdout, RouterLogKey, []string{log.RequestIdCtxKey})
	log.InitLogger(os.Stdout, ChannelLogKey, []string{log.RequestIdCtxKey})
}

func GetRouterLog(ctx context.Context) *log.Logger {
	return log.GetLogger(ctx, RouterLogKey)
}

func GetChannelLog(ctx context.Context) *log.Logger {
	return log.GetLogger(ctx, ChannelLogKey)
}

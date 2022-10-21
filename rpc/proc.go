package rpc

import (
	"context"
	"encoding/json"
)

type Err string

func (e Err) Error() string {
	return string(e)
}

const (
	OK            Err = "OK"
	ChannelClosed     = "CLOSED"
)

type CallRequest struct {
	ReqId  string `json:"req_id"`
	From   string `json:"from"`
	To     string `json:"to"`
	Method string `json:"func_name"`
	Body   []byte `json:"body"`
}

type CallResponse struct {
	Errmsg Err    `json:"errmsg"`
	Body   []byte `json:"body"`
}

// For router
type (
	RegisterRequest struct {
		Name string `json:"name"`
		Addr string `json:"addr"`
	}

	RegisterResponse struct {
		ErrMsg Err `json:"errmsg"`
	}

	ConfigRequest struct {
		Name      string `json:"name"`
		LatencyMs int    `json:"Latency_ms"`
		Disabled  bool   `json:"disabled"`
	}

	ConfigResponse struct {
		Errmsg string `json:"errmsg"`
	}

	LookupRequest struct {
		Name string `json:"name"`
	}

	LookupResponse struct {
		ErrMsg Err                  `json:"errmsg"`
		Tab    map[string]RouteInfo `json:"tab"`
	}

	PingRequest struct {
	}

	PingResponse struct {
		Name   string `json:"name"`
		ErrMsg Err    `json:"errmsg"`
	}
)

func (c CallRequest) AppendInContext(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, "request_id", c.ReqId)
	ctx = context.WithValue(ctx, "from", c.From)
	ctx = context.WithValue(ctx, "to", c.To)
	return ctx
}

func MustJson(i interface{}) []byte {
	bs, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}

	return bs
}

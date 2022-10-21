package rpc

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func startRouter(t *testing.T) *Router {
	os.Setenv("ROUTER_PORT", "8080")
	router := NewRouter()
	go router.Start()
	time.Sleep(1 * time.Second)
	return router
}

func TestChannel_NewChannel(t *testing.T) {
	const name = "test_chan"
	const routerAddr = "localhost:8080"
	os.Setenv("CHAN_PORT", "9090")
	router := startRouter(t)
	ch, err := NewChannel(context.Background(), name, routerAddr, nil)
	if !assert.NoError(t, err) {
		t.Fatal(err)
	}
	assert.NoError(t, ch.Start())
	assert.NotEmpty(t, router.GetRouterTable()[name])
}

func newTestMethod(i *int) MethodHandler {
	return func(ctx context.Context, req, resp interface{}) error {
		*i += 1
		return nil
	}
}

func TestChannel_Call(t *testing.T) {
	const name = "test_chan"
	const routerAddr = "localhost:8080"

	var testInt int

	type testReq struct{}
	type testResp struct{}
	methodTab := map[string]MethodInfo{
		"test": {
			Request: func() interface{} {
				return &testReq{}
			},
			Response: func() interface{} {
				return &testResp{}
			},
			Handler: newTestMethod(&testInt),
		},
	}
	checkErr := func(e error) {
		if !assert.NoError(t, e) {
			t.Fatal(e)
		}
	}

	startRouter(t)

	os.Setenv("CHAN_PORT", "7070")
	recv, err := NewChannel(context.Background(), "ch1", routerAddr, methodTab)
	if !assert.NoError(t, err) || !assert.NoError(t, recv.Start()) {
		t.Fatal("fail to start recv channel")
	}

	os.Setenv("CHAN_PORT", "9090")
	sender, err := NewChannel(context.Background(), "ch2", routerAddr, methodTab)
	if !assert.NoError(t, err) || !assert.NoError(t, sender.Start()) {
		t.Fatal("fail to start send channel")
	}
	checkErr(sender.Call(context.Background(), "ch1", "test", &testReq{}, &testResp{}))
	assert.Equal(t, 1, testInt)
}

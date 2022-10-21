package rpc

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRouter_handleRegister(t *testing.T) {
	os.Setenv("ROUTER_PORT", "8080")
	router := NewRouter()
	go router.Start()
	time.Sleep(1 * time.Second)

	const name = "test"
	const addr = "127.0.0.1:8888"
	req := &RegisterRequest{
		Name: name,
		Addr: addr,
	}
	data, err := json.Marshal(req)
	assert.NoError(t, err)
	_, err = http.Post("http://0.0.0.0:8080/register", "application/json", bytes.NewBuffer(data))
	assert.NoError(t, err)

	assert.Equal(t, addr, router.GetRouterTable()[name])
	router.Shutdown()
}

func TestRouter_handleLookup(t *testing.T) {
	os.Setenv("ROUTER_PORT", "8080")
	router := NewRouter()
	go router.Start()
	time.Sleep(1 * time.Second)

	lookup := func() map[string]string {
		resp, err := http.Post("http://0.0.0.0:8080/lookup", "application/json", nil)
		assert.NoError(t, err)
		buf, err := io.ReadAll(resp.Body)
		assert.NoError(t, err)
		tab := &LookupResponse{}
		assert.NoError(t, json.Unmarshal(buf, tab))
		return tab.Tab
	}

	assert.EqualValues(t, map[string]string{}, lookup())

	const name = "test"
	const addr = "127.0.0.1:8888"

	req := &RegisterRequest{
		Name: name,
		Addr: addr,
	}
	data, err := json.Marshal(req)
	assert.NoError(t, err)
	_, err = http.Post("http://0.0.0.0:8080/register", "application/json", bytes.NewBuffer(data))
	assert.NoError(t, err)
	assert.Equal(t, addr, router.GetRouterTable()[name])
	assert.EqualValues(t, router.GetRouterTable(), lookup())
}

func TestRouter_healthCheck(t *testing.T) {
	os.Setenv("ROUTER_PORT", "8080")
	router := NewRouter()
	go router.Start()
	time.Sleep(1 * time.Second)

	const name = "test"
	const addr = "127.0.0.1:8888"
	req := &RegisterRequest{
		Name: name,
		Addr: addr,
	}
	data, err := json.Marshal(req)
	assert.NoError(t, err)
	_, err = http.Post("http://0.0.0.0:8080/register", "application/json", bytes.NewBuffer(data))
	assert.NoError(t, err)

	assert.Equal(t, addr, router.GetRouterTable()[name])

	time.Sleep(6 * time.Second)

	assert.EqualValues(t, map[string]string{}, router.GetRouterTable())
	router.Shutdown()
}

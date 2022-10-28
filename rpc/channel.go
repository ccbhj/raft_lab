package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/ccbhj/raft_lab/logging"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

const DefaultTimeout = 5 * time.Second
const DefaultPort = 9090

type MethodHandler func(ctx context.Context, req interface{}, resp interface{}) error

type MethodInfo struct {
	Request  func() interface{}
	Response func() interface{}
	Handler  MethodHandler
}

type Channel struct {
	nReq       int64
	nameHash   int64
	name       string
	routerAddr string

	closed  int64
	closeCh chan struct{}

	MethodHanlder map[string]MethodInfo
}

func NewChannel(ctx context.Context, name, routerAddr string, method map[string]MethodInfo) (*Channel, error) {
	var out io.Writer
	logOut := os.Getenv("CHANNEL_LOG_PATH")
	if logOut == "" || strings.ToUpper(logOut) == "STDOUT" {
		out = os.Stdout
	} else {
		file, err := os.OpenFile(logOut, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			panic(err)
		}
		out = file
	}
	fmt.Printf("channel log path = %s\n", logOut)
	log.InitLogger(out, ChannelLogKey, []string{log.RequestIdCtxKey})

	if routerAddr == "" {
		return nil, errors.New("router addr cannot be empty")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("name cannot empty")
	}
	hsh := fnv.New64()
	hsh.Write([]byte(name))
	sum := int64(hsh.Sum64())
	nameHsh := ((sum & 0xFFFFFFFF) << 32) ^ (sum & 0xFFFFFFF)

	ch := &Channel{
		name:          name,
		routerAddr:    routerAddr,
		closeCh:       make(chan struct{}),
		nameHash:      nameHsh,
		nReq:          0,
		MethodHanlder: method,
	}

	port, err := getPort()
	if err != nil {
		return nil, errors.WithMessage(err, "fail to get port")
	}

	if err := ch.registerName(ctx, name, routerAddr, port, false); err != nil {
		close(ch.closeCh)
		return nil, err
	}

	return ch, nil
}

func (c *Channel) Shutdown() {
	if atomic.CompareAndSwapInt64(&c.closed, 0, 1) {
		fmt.Printf("shutdown channel")
		close(c.closeCh)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c.registerName(ctx, c.Name(), c.routerAddr, 0, true)
	}
}

func (c *Channel) Start() error {
	port, err := getPort()
	if err != nil {
		return errors.WithMessage(err, "fail to get port")
	}

	errCh := make(chan error, 1)
	go func() {
		if err := c.startHttpServer(port); err != nil {
			GetChannelLog(context.Background()).Error("channel http server down: %s", err)
			errCh <- err
		}
	}()
	select {
	case err := <-errCh:
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		c.registerName(ctx, c.Name(), c.routerAddr, 0, true)
		return err
	case <-time.After(1 * time.Second):
		break
	}

	return nil
}

func (c *Channel) GenRequestId() string {
	n := atomic.AddInt64(&c.nReq, 1) - 1
	now := time.Now().UnixMilli()
	hash := (c.nameHash << 55) | (now << 23) | (n & 0x7FFFFF)
	return fmt.Sprintf("%X", uint64(hash))
}

func (c *Channel) Name() string {
	return c.name
}

func (c *Channel) GetRouteTab() (map[string]RouteInfo, error) {
	url := fmt.Sprintf("http://%s%s", c.routerAddr, MethodLookup)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	resp := new(LookupResponse)
	err := post(ctx, url, LookupRequest{Name: c.name}, resp)
	if err != nil {
		return nil, errors.Errorf("fail to get peers")
	}
	if resp.ErrMsg != OK {
		return nil, errors.Errorf("fail to update route table: %s", resp.ErrMsg)
	}
	return resp.Tab, nil
}

func (c *Channel) registerName(ctx context.Context, name, routerAddr string, port int, down bool) error {
	ip := getLocalIP()
	if ip == "" {
		return errors.New("cannot find the local ip addr")
	}

	req := &RegisterRequest{
		Name:   name,
		Addr:   fmt.Sprintf("%s:%d", ip, port),
		Status: !down,
	}
	resp := new(RegisterResponse)
	url := fmt.Sprintf("http://%s%s", routerAddr, MethodRegister)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	err := post(ctx, url, req, resp)
	if err != nil {
		return err
	}
	if resp.ErrMsg != OK {
		return errors.Errorf("fail to register name %s: %s", name, resp.ErrMsg)
	}
	return nil
}

func (c *Channel) startHttpServer(port int) error {
	engine := gin.New()
	if err := os.Mkdir("log", os.ModePerm); err != nil && !os.IsExist(err) {
		return errors.WithMessage(err, "fail to create log dir")
	}
	logFile, err := os.OpenFile(fmt.Sprintf("log/channel_%s_log", c.Name()), os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return errors.WithMessage(err, "fail to create log file")
	}
	GetChannelLog(context.Background()).SetOutput(logFile)

	engine.Use(gin.Recovery())
	engine.Use(gin.LoggerWithWriter(logFile))
	engine.POST(MethodCall, c.handleCall)
	engine.POST(MethodPing, c.handlePing)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: engine,
	}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return errors.WithMessage(err, "server is down")
		}
	case <-c.closeCh:
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			GetChannelLog(ctx).Error("fail to shutdown channel server")
		}
	}

	return nil
}

func (c *Channel) NewRequestId() string {
	seq := atomic.AddInt64(&c.nReq, 1) - 1
	now := time.Now().UnixMilli()

	id := ((c.nameHash & 0xFF) << 55) | (now << 23) | (seq & 0x7FFFFF)

	return fmt.Sprintf("%x", id)
}

func (c *Channel) Call(ctx context.Context, routeTab map[string]RouteInfo, peer, method string, req, resp interface{}) error {
	reqId := log.GetRequestIdFromCtx(ctx)
	if reqId == "" {
		reqId = c.GenRequestId()
		ctx = context.WithValue(ctx, log.RequestIdCtxKey, reqId)
	}

	tab, err := c.GetRouteTab()
	if err != nil {
		return errors.WithMessagef(err, "fail to get peers")
	}

	if routeInfo, in := tab[c.Name()]; in {
		if routeInfo.Disconnect {
			return errors.Errorf("server %s cannot connect to another", c.Name())
		}
		if routeInfo.LatencyMs > 0 {
			time.Sleep(time.Duration(routeInfo.LatencyMs) * time.Millisecond)
		}
	}

	var dstAddr string
	if routeInfo, in := tab[peer]; !in {
		dstAddr = c.routerAddr
	} else {
		dstAddr = routeInfo.Addr
		if routeInfo.Disconnect {
			return errors.Errorf("server %s can not be reached", peer)
		}
		if routeInfo.LatencyMs > 0 {
			time.Sleep(time.Duration(routeInfo.LatencyMs) * time.Millisecond)
		}
	}

	callReq := &CallRequest{
		ReqId:  reqId,
		From:   c.name,
		To:     peer,
		Method: method,
		Body:   MustJson(req),
	}
	callResp := new(CallResponse)
	err = post(ctx,
		fmt.Sprintf("http://%s%s", dstAddr, MethodCall),
		callReq, callResp)
	if err != nil {
		return errors.WithMessage(err, "fail send request")
	}

	if callResp.Errmsg != OK {
		return errors.Errorf("%s", callResp.Errmsg)
	}

	if err := json.Unmarshal(callResp.Body, resp); err != nil {
		return errors.WithMessage(err, "fail to unmarshal response")
	}
	return nil
}

func (c *Channel) handlePing(gctx *gin.Context) {
	if atomic.LoadInt64(&c.closed) != 0 {
		gctx.JSON(http.StatusOK, &PingResponse{Name: c.Name(), ErrMsg: ChannelClosed})
		return
	}
	gctx.JSON(http.StatusOK, &PingResponse{ErrMsg: OK, Name: c.Name()})
}

func (c *Channel) handleCall(gctx *gin.Context) {
	e := func() error {
		callArgs := &CallRequest{}
		err := gctx.ShouldBindJSON(callArgs)
		if err != nil {
			return errors.WithMessage(err, "invalid request body")
		}

		methodInfo, in := c.MethodHanlder[callArgs.Method]
		if !in {
			return errors.Errorf("method handler for %s not found", callArgs.Method)
		}
		req := methodInfo.Request()
		err = json.Unmarshal(callArgs.Body, req)
		if err != nil {
			return errors.WithMessagef(err, "fail to unmarshal request body into %+v", req)
		}
		resp := methodInfo.Response()

		err = methodInfo.Handler(callArgs.AppendInContext(context.Background()), req, resp)
		if err != nil {
			return err
		}
		respData := MustJson(resp)
		gctx.JSON(200, CallResponse{Errmsg: "OK", Body: respData})
		return nil
	}()

	if e != nil {
		gctx.JSON(200, CallResponse{Errmsg: Err(e.Error())})
		return
	}
}

func post(ctx context.Context, url string, req interface{}, resp interface{}) error {
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(MustJson(req)))
	if err != nil {
		return errors.WithMessage(err, "fail to create request")
	}
	request.Header["Content-Type"] = []string{"application/json"}
	request = request.WithContext(ctx)
	httpCli := &http.Client{
		Timeout: 5 * time.Second,
	}
	response, err := httpCli.Do(request)
	if err != nil {
		return errors.WithMessage(err, "fail to send request")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return errors.WithMessagef(err, "response code is not 200, but %d", response.StatusCode)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return errors.WithMessage(err, "fail to read response body")
	}
	err = json.Unmarshal(body, resp)
	if err != nil {
		return errors.WithMessagef(err, "fail to unmarshal response body: %s", string(body))
	}

	return nil
}

// getLocalIP returns the non loopback local IP of the host
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func getPort() (port int, err error) {
	portStr := os.Getenv("CHAN_PORT")
	if portStr != "" {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return 0, errors.WithMessage(err, "invalid port")
		}
	}

	if port == 0 {
		port = DefaultPort
	}
	return port, nil
}

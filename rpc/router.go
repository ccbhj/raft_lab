package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/ccbhj/raft_lab/logging"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

const (
	MethodRegister = "/register"
	MethodCall     = "/call"
	MethodLookup   = "/lookup"
	MethodPing     = "/ping"
	MethodCfg      = "/config"
)

const (
	defaultRouterPort = 8080
)

type Router struct {
	routeTab map[string]RouteInfo

	blocked bool
	lock    *sync.RWMutex

	closed  int64
	closeCh chan struct{}
}

type RouteInfo struct {
	Addr       string
	Disconnect bool
	LatencyMs  int
}

func NewRouter() *Router {
	var out io.Writer
	logOut := os.Getenv("ROUTER_LOG_PATH")
	if logOut == "" || strings.ToUpper(logOut) == "STDOUT" {
		out = os.Stdout
	} else {
		file, err := os.OpenFile(logOut, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			panic(err)
		}
		out = file
	}
	log.InitLogger(out, RouterLogKey, []string{log.RequestIdCtxKey})
	return &Router{
		routeTab: make(map[string]RouteInfo),
		lock:     &sync.RWMutex{},
		closeCh:  make(chan struct{}),
	}
}

func (r *Router) checkPeerHealth(ctx context.Context, addr string) error {
	resp := &PingResponse{}
	if err := post(ctx, addr+MethodPing, &PingRequest{}, resp); err != nil {
		return errors.New("fail to check peer health")
	}

	if resp.ErrMsg == OK {
		return nil
	}
	return errors.New(string(resp.ErrMsg))
}

func (r *Router) checkHealthRoutine(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-r.closeCh:
			return
		case <-ticker.C:
		}

		routeTab := make(map[string]string, len(r.routeTab))
		r.lock.RLock()
		for k, v := range r.routeTab {
			routeTab[k] = v.Addr
		}
		r.lock.RUnlock()

		wg := &sync.WaitGroup{}
		wg.Add(len(routeTab))

		ctx, cancel := context.WithTimeout(context.Background(), interval)
		defer cancel()
		for k, v := range routeTab {
			go func(name, addr string) {
				if err := r.checkPeerHealth(ctx, addr); err != nil {
					r.lock.Lock()
					GetRouterLog(ctx).Info("peer is not alive, err=%s", err)
					delete(r.routeTab, name)
					r.lock.Unlock()
				}
			}(k, v)
		}
	}
}

func (r *Router) handleLookup(gctx *gin.Context) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	tab := r.GetRouterTable()
	gctx.JSON(200, LookupResponse{ErrMsg: OK, Tab: tab})
}

func (r *Router) handleRegister(gctx *gin.Context) {
	r.lock.Lock()
	defer r.lock.Unlock()

	var req RegisterRequest
	if err := gctx.ShouldBind(&req); err != nil {
		gctx.JSON(200, RegisterResponse{ErrMsg: Err(fmt.Sprintf("invalid request body: %s", err))})
		return
	}

	if !req.Status {
		delete(r.routeTab, req.Name)
		gctx.JSON(200, RegisterResponse{ErrMsg: OK})
		return
	}

	if r.blocked {
		gctx.JSON(200, RegisterResponse{ErrMsg: Err("cannot register any peer now")})
		return
	}
	GetRouterLog(gctx).Info("register %s with addr %s", req.Name, req.Addr)
	if _, in := r.routeTab[req.Name]; in {
		gctx.JSON(200, RegisterResponse{ErrMsg: Err("already registered")})
		return
	}

	r.routeTab[req.Name] = RouteInfo{
		Addr:       req.Addr,
		Disconnect: false,
		LatencyMs:  0,
	}
	gctx.JSON(200, RegisterResponse{ErrMsg: OK})
}

func (r *Router) Connect(name string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	info, in := r.routeTab[name]
	if !in {
		panic(fmt.Sprintf("cannot found route info for %s", name))
	}

	info.Disconnect = false
	r.routeTab[name] = info
}

func (r *Router) Disconnect(name string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	info, in := r.routeTab[name]
	if !in {
		panic(fmt.Sprintf("cannot found route info for %s", name))
	}

	info.Disconnect = true
	r.routeTab[name] = info
}

func (r *Router) handleConfig(gctx *gin.Context) {
	r.lock.Lock()
	defer r.lock.Unlock()

	var req ConfigRequest
	if err := gctx.BindJSON(&req); err != nil {
		gctx.JSON(200, ConfigResponse{Errmsg: "invalid request body"})
		return
	}

	info, in := r.routeTab[req.Name]
	if !in {
		gctx.JSON(200, ConfigResponse{Errmsg: "name not register"})
		return
	}

	info.Disconnect = req.Disabled
	info.LatencyMs = req.LatencyMs
	r.routeTab[req.Name] = info

	gctx.JSON(200, ConfigResponse{Errmsg: string(OK)})
}

func (r *Router) handleCall(gctx *gin.Context) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	var req CallRequest
	reqBody, err := ioutil.ReadAll(gctx.Request.Body)
	if err != nil {
		gctx.JSON(200, CallResponse{Errmsg: "fail to read request body"})
	}

	if err := json.Unmarshal(reqBody, &req); err != nil {
		gctx.JSON(200, CallResponse{Errmsg: "invalid request body"})
		return
	}
	info, in := r.routeTab[string(req.To)]
	if !in {
		gctx.JSON(200, CallResponse{Errmsg: "unknown server"})
		return
	}
	if info.Disconnect {
		gctx.JSON(200, CallResponse{Errmsg: "server can not be reached"})
		return
	}
	if info.LatencyMs > 0 {
		time.Sleep(time.Duration(info.LatencyMs) * time.Millisecond)
	}

	res, err := http.Post(info.Addr+MethodCall, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		gctx.JSON(200, CallResponse{Errmsg: "fail to redirect request"})
		return
	}
	defer res.Body.Close()

	respBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		gctx.JSON(200, CallResponse{Errmsg: "fail to read response body"})
		return
	}
	gctx.Data(200, "application/json", respBody)
}

func (r *Router) startHttpServer(port int) error {
	engine := gin.New()
	if err := os.Mkdir("log", os.ModePerm); err != nil && !os.IsExist(err) {
		return errors.WithMessage(err, "fail to create log dir")
	}

	engine.Use(gin.Recovery())
	engine.Use(gin.LoggerWithWriter(GetRouterLog(context.Background()).GetOutput()))
	engine.Use(logResponseBody)
	engine.POST(MethodRegister, r.handleRegister)
	engine.POST(MethodCall, r.handleCall)
	engine.POST(MethodLookup, r.handleLookup)
	engine.POST(MethodCfg, r.handleConfig)

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
	case <-r.closeCh:
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			GetChannelLog(ctx).Error("fail to shutdown router server")
		}
	}

	return nil
}

func (r *Router) Shutdown() {
	if atomic.CompareAndSwapInt64(&r.closed, 0, 1) {
		close(r.closeCh)
	}
}

func (r *Router) Start() error {
	var port int
	var err error
	portStr := os.Getenv("ROUTER_PORT")
	if portStr == "" {
		port = defaultRouterPort
	} else {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			GetRouterLog(context.Background()).Error("invalid port %s, fallback to default port %d", portStr, defaultRouterPort)
		}
	}
	fmt.Printf("start router at :%d\n", port)

	errCh := make(chan error, 1)
	go func() {
		if err := r.startHttpServer(port); err != nil {
			GetChannelLog(context.Background()).Error("channel http server down: %s", err)
			errCh <- err
		}
	}()
	select {
	case err := <-errCh:
		r.Shutdown()
		return err
	case <-time.After(1 * time.Second):
		break
	}

	// go r.checkHealthRoutine(5 * time.Second)
	return nil
}

type responseBodyWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (r responseBodyWriter) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

func logResponseBody(c *gin.Context) {
	c.Set(log.RequestIdCtxKey, c.Request.Header.Get(log.RequestIdCtxKey))
	request, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		GetRouterLog(c).Error("fail to log request, copy body error")
		c.String(500, "fail to log request, copy body error")
		return
	}
	c.Request.Body = ioutil.NopCloser(bytes.NewReader(request))
	GetRouterLog(c).Info("[Router] request body: " + string(request))

	w := &responseBodyWriter{body: &bytes.Buffer{}, ResponseWriter: c.Writer}
	c.Writer = w
	c.Next()
	GetRouterLog(c).Info("[Router] response body: " + w.body.String())
}

func (r *Router) GetRouterTable() map[string]RouteInfo {
	r.lock.RLock()
	defer r.lock.RUnlock()
	tab := make(map[string]RouteInfo, len(r.routeTab))
	for k, v := range r.routeTab {
		tab[k] = v
	}

	return tab
}

func (r *Router) SetBlock(blocked bool) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.blocked = blocked
}

func (r *Router) IsBlocked() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.blocked
}

func (r *Router) Clean(peer string) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	addr, in := r.routeTab[peer]
	delete(r.routeTab, peer)
	if in {
		fmt.Printf("clean %s : %s\n", peer, addr.Addr)
	}
	return in
}

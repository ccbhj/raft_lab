package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/ccbhj/raft_lab/raft"
	"github.com/ccbhj/raft_lab/rpc"
)

type ExecContext struct {
	rf        *raft.Raft
	router    *rpc.Router
	commands  map[string]Command
	counter   int64
	commitIdx int64
	closed    chan struct{}
}

func newRouterExecContext(logDir string) *ExecContext {
	os.Setenv("ROUTER_LOG_PATH", path.Join(logDir, "router.log"))
	router := rpc.NewRouter()
	if err := router.Start(); err != nil {
		fatal(err)
	}

	cmds := make(map[string]Command)
	for name, cmd := range routerCommands {
		cmds[name] = cmd
	}
	for name, cmd := range commonCommands {
		cmds[name] = cmd
	}
	return &ExecContext{
		rf:       nil,
		router:   router,
		commands: cmds,
		closed:   make(chan struct{}),
	}
}

func newRaftExecContext(name, routerAddr, logDir string) *ExecContext {
	name = strings.TrimSpace(name)
	if name == "" {
		fatal(errors.New("need a peer name"))
	}
	routerAddr = strings.TrimSpace(routerAddr)
	if routerAddr == "" {
		fatal(errors.New("need router address"))
	}
	os.Setenv("CHANNEL_LOG_PATH", path.Join(logDir, fmt.Sprintf("channel_%s.log", name)))
	os.Setenv("RAFT_LOG_PATH", path.Join(logDir, fmt.Sprintf("raft_%s.log", name)))
	rf, err := raft.MakeRaft(name, routerAddr)
	if err != nil {
		fatal(err)
		flag.Usage()
	}
	cmds := make(map[string]Command)
	for name, cmd := range raftCommands {
		cmds[name] = cmd
	}
	for name, cmd := range commonCommands {
		cmds[name] = cmd
	}
	e := &ExecContext{
		commands: cmds,
		rf:       rf,
		closed:   make(chan struct{}),
	}
	go e.recvCommand()
	return e
}

func NewExecContext() *ExecContext {
	var (
		routerMode bool
		logDir     string
		name       string
		routerAddr string
	)
	flag.BoolVar(&routerMode, "r", false, "start router instead of raft")
	flag.StringVar(&logDir, "l", "./log", "log path(empty for stdout), ./log")
	flag.StringVar(&name, "n", "", "raft peer name")
	flag.StringVar(&routerAddr, "a", "", "router address, ip:port")
	flag.Parse()

	if routerMode {
		return newRouterExecContext(logDir)
	}

	return newRaftExecContext(name, routerAddr, logDir)
}

func (c *ExecContext) Close() {
	close(c.closed)

	if c.rf != nil {
		c.rf.Shutdown()
	}

	if c.router != nil {
		c.router.Shutdown()
	}
}

func (c *ExecContext) ListCommand() {
	for name, cmd := range c.commands {
		fmt.Printf("%-12s - %-s\n", name, cmd.Usage)
	}
}

func (c *ExecContext) recvCommand() {
	for {
		select {
		case msg := <-c.rf.Applied():
			c.applyCommand(msg)
		case <-c.closed:
			return
		}
	}
}

func (c *ExecContext) applyCommand(applyMsg raft.ApplyMsg) {
	switch v := applyMsg.Command.(type) {
	case string:
		switch strings.ToUpper(v) {
		case "INCR":
			c.counter++
		case "DECR":
			c.counter--
		}
	}
	c.commitIdx = applyMsg.CommandIndex
}

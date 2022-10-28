package raft

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ccbhj/raft_lab/rpc"
	"github.com/stretchr/testify/assert"
)

type testClient struct {
	replicaLogs map[string]map[int64]interface{}
	router      *rpc.Router
	rfs         map[string]*Raft
	closed      chan struct{}
	maxIndex    int64
	lock        *sync.Mutex
	t           *testing.T
	applyErr    map[string]string
}

func (c *testClient) printReplicatLogs() {
	for name := range c.rfs {
		cmds := make([]string, 0, len(c.replicaLogs[name]))
		terms := make([]int64, 0, len(cmds))
		for term := range c.replicaLogs[name] {
			terms = append(terms, term)
		}
		sort.Slice(terms, func(i, j int) bool {
			return terms[i] < terms[j]
		})
		for _, term := range terms {
			cmds = append(cmds,
				fmt.Sprintf("\t%d: %v", term, c.replicaLogs[name][term]))
		}
		c.t.Logf("%s: \n%s\n", name, strings.Join(cmds, "\n"))
	}
}

func (c *testClient) checkLogs(from string, m ApplyMsg) (string, bool) {
	errMsg := ""
	v := m.Command
	for name, logs := range c.replicaLogs {
		if old, oldOk := c.replicaLogs[name][m.CommandIndex]; oldOk && !assert.EqualValues(c.t, old, v) {
			log.Printf("%v: log %v; server %v\n", from, logs, c.replicaLogs[name])
			errMsg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
				m.CommandIndex, from, m.Command, name, old)
		}
	}

	_, prevOk := c.replicaLogs[from][m.CommandIndex-1]
	c.replicaLogs[from][m.CommandIndex] = v
	if m.CommandIndex > c.maxIndex {
		c.maxIndex = m.CommandIndex
	}
	return errMsg, prevOk
}

func setupClients(t *testing.T, router *rpc.Router, rfs map[string]*Raft) *testClient {
	cl := make(map[string]map[int64]interface{}, len(rfs))
	closeCh := make(chan struct{}, 1)
	cli := &testClient{
		t:           t,
		router:      router,
		replicaLogs: cl,
		rfs:         rfs,
		closed:      closeCh,
		lock:        &sync.Mutex{},
		applyErr:    make(map[string]string, len(rfs)),
	}
	for name := range cli.rfs {
		cl[name] = make(map[int64]interface{}, 1<<5)
		go func(rf *Raft) {
			for {
				select {
				case msg := <-rf.Applied():
					if !msg.CommandValid {
						continue
					}
					cli.lock.Lock()
					errMsg, prevOk := cli.checkLogs(rf.Name(), msg)
					cli.lock.Unlock()
					if msg.CommandIndex > 0 && !prevOk {
						errMsg = fmt.Sprintf("server %v apply out of order %v", rf.Name(), msg.CommandIndex)
					}
					if errMsg != "" {
						log.Fatalf("apply error: %v\n", errMsg)
						cli.applyErr[rf.Name()] = errMsg
					}
				case <-closeCh:
					return
				}
			}
		}(rfs[name])
	}

	return cli
}

func (c *testClient) nCommitted(idx int64) (int, interface{}) {
	count := 0
	var cmd interface{} = nil
	for name := range c.rfs {
		if c.applyErr[name] != "" {
			c.t.Fatal(c.applyErr[name])
		}

		c.lock.Lock()
		cmd1, ok := c.replicaLogs[name][idx]
		c.lock.Unlock()

		if ok {
			if count > 0 && !assert.EqualValues(c.t, cmd, cmd1) {
				c.t.Fatalf("committed value do not match: index %v, %v, %v\n",
					idx, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}

	return count, cmd
}

func (c *testClient) one(cmd interface{}, expectedServers int, retry bool) int64 {
	t0 := time.Now()
	routeTab := c.router.GetRouterTable()
	for time.Since(t0).Seconds() < 10 {
		// try all the servers, maybe one is the leader.
		index := int64(-1)
		for name, routeInfo := range routeTab {
			var rf *Raft
			if !routeInfo.Disconnect {
				rf = c.rfs[name]
			}
			if rf != nil {
				index1, _, ok := rf.commitCommond(cmd)
				if ok {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := c.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					// committed
					if cmd1 == cmd {
						// and it was the command we submitted.
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if retry == false {
				c.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}

func TestBasicAgree(t *testing.T) {
	t.Log("basic agreement")
	r := initRouter(t)

	names := []string{"AA", "BB", "CC"}
	rfs := makeRafts(t, r, names)

	cli := setupClients(t, r, rfs)
	iters := 3
	for idx := 0; idx < iters; idx++ {
		nd, _ := cli.nCommitted(int64(idx))
		if nd > 0 {
			t.Fatalf("some have commited before commiting, nd=%d", nd)
		}
		xindex := cli.one((idx+1)*100, len(names), false)
		if xindex != int64(idx) {
			t.Fatalf("got index %v but expected %v", xindex, idx)
		}
	}

	close(cli.closed)
	r.Shutdown()
	shutdownRafts(t, rfs)
}

func TestFailNoAgree(t *testing.T) {
	t.Log("fail no agree")
	r := initRouter(t)

	names := []string{"AA", "BB", "CC", "DD", "EE"}
	rfs := makeRafts(t, r, names)

	cli := setupClients(t, r, rfs)

	cli.one(10, len(names), false)

	leader := checkOneLeader(t, r, rfs)
	leaderIdx := 0
	for i, name := range names {
		if name == leader {
			leaderIdx = i
			break
		}
	}
	r.Disconnect(names[(leaderIdx+1)%len(names)])
	r.Disconnect(names[(leaderIdx+2)%len(names)])
	r.Disconnect(names[(leaderIdx+3)%len(names)])

	index, _, ok := rfs[leader].commitCommond(20)
	if ok != true {
		t.Fatalf("leader reject CommitCommond")
	}

	if index != 1 {
		t.Fatalf("expect index 1, got %v", index)
	}

	time.Sleep(2 * time.Second)

	r.Connect(names[(leaderIdx+1)%len(names)])
	r.Connect(names[(leaderIdx+2)%len(names)])
	r.Connect(names[(leaderIdx+3)%len(names)])

	leader2 := checkOneLeader(t, r, rfs)
	if leader2 == "" {
		t.Fatal("expect leader2")
	}
	index2, _, ok2 := rfs[leader2].commitCommond(30)
	if ok2 == false {
		t.Fatalf("leader2 reject CommitCommand")
	}
	if index2 < 1 || index2 > 2 {
		t.Fatalf("unexpected index %v", index2)
	}

	cli.one(1000, len(names), true)

	r.Shutdown()
	shutdownRafts(t, rfs)
	close(cli.closed)
	cli.printReplicatLogs()
}

func TestRejoin(t *testing.T) {
	t.Log("fail no agree")
	r := initRouter(t)

	names := []string{"AA", "BB", "CC"}
	rfs := makeRafts(t, r, names)
	cli := setupClients(t, r, rfs)
	defer func() {
		cli.printReplicatLogs()
	}()

	// first log at index=0 for all
	cli.one(101, len(names), true)

	// leader network failure
	leader1 := checkOneLeader(t, r, rfs)
	r.Disconnect(leader1)

	// make old leader try to agree on some entries
	rfs[leader1].commitCommond(102)
	rfs[leader1].commitCommond(103)
	rfs[leader1].commitCommond(104)

	// new leader commits, also for index=1
	cli.one(103, 2, true)

	// new leader network failure
	leader2 := checkOneLeader(t, r, rfs)
	r.Disconnect(leader2)

	// old leader connect again
	r.Connect(leader1)

	cli.one(104, 2, true)

	// all together now
	r.Connect(leader2)

	cli.one(105, len(names), true)

	close(cli.closed)
	r.Shutdown()
	shutdownRafts(t, rfs)
}

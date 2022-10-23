package raft

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ccbhj/raft_lab/rpc"
	"github.com/stretchr/testify/assert"
)

const localHost = "0.0.0.0"
const routerPort = "9090"

var _ = os.Setenv("RAFT_ENV", "TEST")

func initRouter(t *testing.T) *rpc.Router {
	os.Setenv("ROUTER_PORT", routerPort)
	r := rpc.NewRouter()
	go r.Start()
	time.Sleep(500 * time.Millisecond)
	t.Log("done init router")
	return r
}

func checkTerms(t *testing.T, rafts map[string]*Raft) bool {
	m := make(map[string]int64)
	terms := make(map[int64]struct{})
	for _, r := range rafts {
		m[r.channel.Name()] = r.currentTerm
		terms[r.currentTerm] = struct{}{}
	}
	if !assert.Len(t, terms, 1) {
		t.Logf("terms are not the same: %+v", m)
		return false
	}
	return true
}

func checkOneLeader(t *testing.T, router *rpc.Router, rafts map[string]*Raft) string {
	tab := router.GetRouterTable()
	for iters := 0; iters < 10; iters++ {
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int64][]string)
		for name, route := range tab {
			if !route.Disconnect {
				status, err := rafts[name].GetStatus(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				term, leader := status.Term, status.State == LEADER
				if leader {
					leaders[term] = append(leaders[term], name)
				}
			}
		}

		lastTermWithLeader := int64(-1)
		for term, leaders := range leaders {
			if len(leaders) > 1 {
				t.Fatalf("term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	return ""
}

func shutdownRafts(t *testing.T, rfs map[string]*Raft) {
	for _, rf := range rfs {
		rf.Shutdown()
	}
}

func makeRafts(t *testing.T, router *rpc.Router, names []string) map[string]*Raft {
	t.Logf("making %d rafts: %+v\n", len(names), names)
	routeAddr := fmt.Sprintf("%s:%s", localHost, routerPort)
	rafts := make(map[string]*Raft, len(names))
	for i, name := range names {
		os.Setenv("CHAN_PORT", fmt.Sprintf("%d", i+8080))
		rf, err := MakeRaft(name, routeAddr)
		if err != nil {
			t.Fatalf("fail to start %s at port %d: %s", name, i+8080, err)
		}
		if rf.channel == nil {
			t.Fatalf("channel is nil for %s", name)
		}
		rafts[name] = rf
		t.Logf("done making raft %s", name)
	}

	for _, rf := range rafts {
		rf.Start()
	}
	return rafts
}

func TestSimpleElection(t *testing.T) {
	t.Log("test simple election")
	r := initRouter(t)

	names := []string{"AA", "BB", "CC"}
	rfs := makeRafts(t, r, names)

	time.Sleep(2 * time.Second)

	if checkOneLeader(t, r, rfs) == "" {
		t.Fail()
	}
	if !checkTerms(t, rfs) {
		t.Fail()
	}

	r.Shutdown()
	shutdownRafts(t, rfs)
}

func TestReElection(t *testing.T) {
	t.Log("test re-election")
	r := initRouter(t)

	names := []string{"AA", "BB", "CC"}
	rfs := makeRafts(t, r, names)

	time.Sleep(2 * time.Second)

	leader1 := checkOneLeader(t, r, rfs)
	if leader1 == "" {
		t.Fatalf("lack of first leader")
	}
	if !checkTerms(t, rfs) {
		t.Fail()
	}

	// leader1 disconnect, a new leader should be elected
	t.Logf("===================== disconnect leader1 %s ====================", leader1)
	r.Disconnect(leader1)
	if l := checkOneLeader(t, r, rfs); l == "" {
		t.Fatalf("lack of second leader")
	}

	t.Logf("===================== connect leader1 %s ====================", leader1)
	r.Connect(leader1)
	leader2 := checkOneLeader(t, r, rfs)

	// disconnect leader and another peer
	// should no leader elected
	t.Logf("===================== disconnect leader2 %s ====================", leader2)
	r.Disconnect(leader2)
	var disconnect string
	for name := range rfs {
		if name != leader2 {
			disconnect = name
			t.Logf("===================== disconnect peer %s ====================", name)
			r.Disconnect(name)
			break
		}
	}
	time.Sleep(2 * time.Second)
	if l := checkOneLeader(t, r, rfs); l != "" {
		t.Fatalf("should be no leader, but got %s", l)
	}

	// if a peer connects now, a new leader should be elected
	r.Connect(disconnect)
	if checkOneLeader(t, r, rfs) == "" {
		t.Fatalf("lack of third leader")
	}

	r.Connect(leader2)
	if checkOneLeader(t, r, rfs) == "" {
		t.Fatalf("lack of third leader")
	}

	r.Shutdown()
	shutdownRafts(t, rfs)
}

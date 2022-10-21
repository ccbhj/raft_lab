package raft

import (
	"context"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/ccbhj/raft_lab/log"
	"github.com/ccbhj/raft_lab/rpc"
	"github.com/pkg/errors"
)

func init() {
	rand.Seed(time.Now().UnixMicro())
	log.InitLogger(os.Stdout, "RAFT", []string{raftIdKey, stateKey, termKey})
}

type LogEntry struct {
	Command interface{}
	Index   int64
	Term    int64
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Replay       bool
}

type RaftStatus struct {
	Name    string
	State   State
	Term    int64
	VoteFor string
}

type Raft struct {
	closed int64
	// Persister *Persister // Object to hold this peer's persisted state
	me string // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State
	currentTerm int64
	voteFor     string
	logs        []LogEntry
	applyCh     chan ApplyMsg

	voteCount int

	timer *time.Timer

	// index of the next log entry to sent to for each peer
	nextIdxes map[string]int64
	// index of the highest log entry that has been send for each peer
	matchIdxes map[string]int64
	// index of the highest log entry that has been send to applyCh
	lastApplied int64

	// index of the last log entry known to be commited
	commitIdx int64

	lastIncludedTerm  int64
	lastIncludedIndex int64

	// channel to notify persisting, buffered
	persistCh  chan persistMsg
	applyMsgCh chan sendApplyMsgMsg

	msgCh chan Msg

	channel *rpc.Channel
	closeCh chan struct{}
}

func (rf *Raft) initFollower(ctx context.Context) {
	if rf.state != FOLLOWER {
		getLogger(ctx).Info("become follower")
		rf.voteCount = 0
		rf.state = FOLLOWER
	}
	rf.voteFor = NOT_VOTE
}

// resetTimer reset rf.timer, if timeout is zero, random timeout will be used
func (rf *Raft) resetTimer(ctx context.Context, timeout time.Duration) time.Duration {
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:
		}
	}
	if timeout == 0 {
		timeout = time.Duration(rand.Int63n(GetRaftConfig().ElectionTimeoutRangeMs)+GetRaftConfig().ElectionTimeoutMinMs) * time.Millisecond
	}
	getLogger(ctx).Debug("reset timer for %d ms", timeout.Milliseconds())
	rf.timer.Reset(timeout)
	return timeout
}

func (rf *Raft) isMoreUpToDate(lastLogTerm, lastLogIdx int64) bool {
	return true
}

func (rf *Raft) prepareAppendEntriesArgs(peer string, args *AppendEntriesRequest) {
	args.LeaderID = rf.me
	args.Term = rf.currentTerm
	args.LeaderCommit = rf.commitIdx

	if len(rf.logs) > 0 {
		args.PrevLogIdx = rf.nextIdxes[peer] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIdx].Term
		args.Entries = rf.logs[rf.nextIdxes[peer]:]
	}
}

func (rf *Raft) mainLoop() {
	for {
		reqId := rf.channel.GenRequestId()
		ctx := rf.newContext(reqId)
		select {
		case <-rf.timer.C:
			switch rf.state {
			case FOLLOWER, CANDIDATE:
				rf.msgCh <- tickerMsg{ReqId: reqId}
			case LEADER:
				rf.msgCh <- sendHeartbeatMsg{ReqId: reqId}
			}
		case <-rf.closeCh:
			getLogger(ctx).Info("raft shutdown now")
			return
		case msg := <-rf.msgCh:
			switch v := msg.(type) {
			case tickerMsg:
				rf.tickerProc(v)
			case electionMsg:
				rf.electionProc(v)
			case sendHeartbeatMsg:
				rf.sendingHeartbeatProc(v)
			case getStatusMsg:
				rf.getStatusProc(v)
			case rpcArgsMsg:
				switch v.RPCType {
				case RPCRequestVote:
					rf.requestVote(v)
				case RPCAppendEntries:
					rf.appendEntries(v)
				}
			case rpcReplyMsg:
				switch v.RPCType {
				case RPCRequestVote:
					rf.voteReplyProc(v)
				case RPCAppendEntries:
					rf.heartbeatReplyProc(v)
				}
			}
		}
	}
}

func (rf *Raft) persist() {
}

func (rf *Raft) GetStatus(ctx context.Context) (RaftStatus, error) {
	ch := make(chan RaftStatus, 1)
	rf.msgCh <- getStatusMsg{ch}

	select {
	case s := <-ch:
		return s, nil
	case <-ctx.Done():
		return RaftStatus{}, ctx.Err()
	}
}

func (rf *Raft) Shutdown() {
	if atomic.CompareAndSwapInt64(&rf.closed, 0, 1) {
		close(rf.closeCh)
		rf.channel.Shutdown()
	}
}

func (rf *Raft) Start() {
	rf.resetTimer(rf.newContext(""), 0)
	go rf.mainLoop()
}

func (rf *Raft) newContext(reqId string) (ctx context.Context) {
	ctx = context.Background()
	ctx = context.WithValue(ctx, raftIdKey, rf.me)
	ctx = context.WithValue(ctx, termKey, rf.currentTerm)
	ctx = context.WithValue(ctx, stateKey, rf.state)
	if reqId == "" {
		reqId = rf.channel.GenRequestId()
	}
	ctx = context.WithValue(ctx, log.RequestIdCtxKey, reqId)
	return
}

func (rf *Raft) forEachPeers(ctx context.Context, fn func(peer string, routeTab map[string]rpc.RouteInfo)) error {

	tab, err := rf.channel.GetRouteTab()
	if err != nil {
		getLogger(ctx).Error("fail to get routeTab")
		return err
	}

	for k := range tab {
		if k == rf.me {
			continue
		}
		fn(k, tab)
	}
	return nil
}

func getLogger(ctx context.Context) *log.Logger {
	return log.GetLogger(ctx, "RAFT")
}

func Make(name string, routeAddr string) (*Raft, error) {
	rf := &Raft{
		me:                name,
		state:             FOLLOWER,
		currentTerm:       0,
		voteFor:           NOT_VOTE,
		logs:              make([]LogEntry, 0, 1<<10),
		applyCh:           make(chan ApplyMsg),
		voteCount:         0,
		timer:             time.NewTimer(0),
		nextIdxes:         make(map[string]int64),
		matchIdxes:        make(map[string]int64),
		lastApplied:       0,
		commitIdx:         0,
		lastIncludedTerm:  0,
		lastIncludedIndex: 0,
		persistCh:         make(chan persistMsg),
		applyMsgCh:        make(chan sendApplyMsgMsg),
		msgCh:             make(chan Msg, msgQueueSize),
		closeCh:           make(chan struct{}),
	}
	ch, err := rpc.NewChannel(context.Background(), name, routeAddr, rf.newMethodTable())
	if err != nil {
		return nil, errors.WithMessage(err, "fail to create channel")
	}
	if err := ch.Start(); err != nil {
		return nil, errors.WithMessage(err, "fail to start channel")
	}

	rf.channel = ch
	return rf, nil
}

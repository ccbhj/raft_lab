package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
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
	Term         int64
	CommandIndex int64
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

func (rf *Raft) getLastLogEntry() (idx, term int64) {
	l := int64(len(rf.logs))
	if l == 0 {
		return -1, 0
	}
	return rf.logs[l-1].Index, rf.logs[l-1].Term
}

func (rf *Raft) getLogEntry(i int64) *LogEntry {
	l := int64(len(rf.logs))
	if l == 0 {
		return nil
	} else if 0 <= i && i < l {
		return &rf.logs[i]
	}

	return nil
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

func (rf *Raft) isMoreUpToDate(ctx context.Context, lastTerm, lastIdx int64) bool {
	if len(rf.logs) == 0 {
		return true
	}
	// If the log send with the same term, then whichever log is longer ismore
	// up-to-date (see $5.4.1 in the paper)
	lastLogIdx, lastLogTerm := rf.getLastLogEntry()
	result := (lastLogTerm < lastTerm) ||
		(lastLogTerm == lastTerm && lastLogIdx <= lastIdx)

	getLogger(ctx).Debug("lastLogIdx=%d, lastLogTerm=%d, lastIdx=%d, lastTerm=%d", lastLogIdx, lastLogTerm, lastIdx, lastTerm)

	return result
}

// update commit index to a majority of matchIndex
// 1. count all the matchIndexes
// 2. if a count is a majority for a match index,
//    and the log at that index has the same term as rf.currentTerm,
//    update rf.commitIdx to this match index
// 3. apply logs since the commitIdx is updated
func (rf *Raft) updateCommitIdx(ctx context.Context) {
	counter := make(map[int64]int)
	logger := getLogger(ctx)

	routeTab, err := rf.channel.GetRouteTab()
	if err != nil {
		logger.Error("fail to GetRouteTab: %s", err)
		return
	}
	ct := rf.currentTerm
	for _, idx := range rf.matchIdxes {
		count, in := counter[idx]
		if !in {
			counter[idx] = 0
		}
		count++
		counter[idx] = count
		if count >= (len(routeTab)/2)+1 &&
			idx > rf.commitIdx &&
			rf.logs[idx].Term == ct {
			rf.commitIdx = idx
			break
		}
	}
	logger.Debug("matchIdxes=%+v, commitIdx is now %d", rf.matchIdxes, rf.commitIdx)

	rf.sendApplyMsg(ctx)
	rf.persist()
}

func (rf *Raft) sendApplyMsg(ctx context.Context) {
	getLogger(ctx).Debug("prepare to send logs[%d:%d]", rf.lastApplied+1, rf.commitIdx+1)
	logs := rf.logs[rf.lastApplied+1 : rf.commitIdx+1]
	dup := make([]LogEntry, len(logs))
	copy(dup, logs)
	rf.applyMsgCh <- sendApplyMsgMsg{
		ReqId:       log.GetRequestIdFromCtx(ctx),
		LastApplied: rf.lastApplied,
		CommitIdx:   rf.commitIdx,
		Logs:        dup,
	}
	rf.lastApplied = rf.commitIdx
	getLogger(ctx).Debug("lastApplied=%d, commitIdx=%d", rf.lastApplied, rf.commitIdx)
}

func (rf *Raft) mergeLogEntries(ctx context.Context, a AppendEntriesRequest) bool {
	// check the prev log term
	// reply false if log doesn't contain an entry at prevLogIdx whose term
	// matches prevLogTerm (see Figure 2 and $5.3 in paper)
	if a.PrevLogIdx >= 0 {
		lastIdx, lastTerm := rf.getLastLogEntry()
		var prevLogTerm int64
		if a.PrevLogIdx == lastIdx {
			prevLogTerm = lastTerm
		} else {
			prevLog := rf.getLogEntry(a.PrevLogIdx)
			if prevLog == nil {
				return false
			}
			prevLogTerm = prevLog.Term
		}

		if a.PrevLogTerm != prevLogTerm {
			return false
		}
	}

	// no need to append any entry
	if len(a.Entries) == 0 {
		return true
	}

	var newEntries []LogEntry
	lastLogIdx, _ := rf.getLastLogEntry()
	for i, entry := range a.Entries {
		// have no new entry from i to len(a.Entries),
		// append all the entries behind i
		if entry.Index > lastLogIdx {
			newEntries = a.Entries[i:]
			break
		}

		storeEntry := rf.getLogEntry(entry.Index)
		if storeEntry == nil {
			return false
		}

		// if an existing entry conflicts with a new one (same index but different
		// term), delete all the existing entry and all that follow it
		// (see Figure 2 and $5.3 in paper)
		if entry.Term != storeEntry.Term {
			rf.logs = rf.logs[:entry.Index]
			newEntries = a.Entries[i:]
			break
		}
	}

	// now that we have all the entries
	// override or append the logs
	if n := len(newEntries); n > 0 {
		for i, l := range newEntries {
			idx := l.Index
			if idx < int64(len(rf.logs)) {
				rf.logs[idx] = l
			} else {
				rf.logs = append(rf.logs, newEntries[i:]...)
			}
		}
	}

	return true

}

func (rf *Raft) prepareAppendEntriesArgs(peer string, args *AppendEntriesRequest) {
	args.LeaderID = rf.me
	args.Term = rf.currentTerm
	args.LeaderCommit = rf.commitIdx
	args.PrevLogTerm = 0
	args.PrevLogIdx = -1

	if len(rf.logs) > 0 {
		args.PrevLogIdx = rf.nextIdxes[peer] - 1
		if args.PrevLogIdx >= 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIdx].Term
		}
		args.Entries = rf.logs[rf.nextIdxes[peer]:]
	}
}

// daemons
func (rf *Raft) sendApplyMsgDaemon() {
	for msg := range rf.applyMsgCh {
		ctx := rf.newContext(msg.ReqId)
		getLogger(ctx).Info("apply %d logs, lastApplied=%d, commitIdx=%d",
			len(msg.Logs), msg.LastApplied, msg.CommitIdx)
		if msg.CommitIdx == msg.LastApplied {
			continue
		}

		for i := 0; i < len(msg.Logs); i++ {
			log := msg.Logs[i]
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.Index,
			}
		}
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
			case commitMsg:
				rf.commitMsgProc(v)
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
			default:
				panic(fmt.Sprintf("unhandled msg %+v", v))
			}
		}
	}
}

func (rf *Raft) persist() {
	fileName := fmt.Sprintf("%s/raft_data_%s.json",
		strings.TrimRight(raftConfig.PersistDir, "/"), rf.Name())
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(errors.WithMessagef(err, "fail to open persist file: %s", fileName))
	}
	defer file.Close()

	type PersisteContent struct {
		CurrentTerm int64      `json:"current_term"`
		VoteFor     string     `json:"vote_for"`
		CommitIdx   int64      `json:"commit_idx"`
		LastApplied int64      `json:"last_applied"`
		Logs        []LogEntry `json:"logs"`
	}

	cnt := &PersisteContent{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.voteFor,
		CommitIdx:   rf.commitIdx,
		LastApplied: rf.lastApplied,
		Logs:        rf.logs,
	}

	data, _ := json.MarshalIndent(cnt, "", " ")
	file.Write(data)
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
	go rf.sendApplyMsgDaemon()
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

func MakeRaft(name string, routeAddr string) (*Raft, error) {
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
		lastApplied:       -1,
		commitIdx:         -1,
		lastIncludedTerm:  0,
		lastIncludedIndex: 0,
		persistCh:         make(chan persistMsg),
		applyMsgCh:        make(chan sendApplyMsgMsg, msgQueueSize),
		msgCh:             make(chan Msg, msgQueueSize),
		closeCh:           make(chan struct{}),
	}
	ch, err := rpc.NewChannel(context.Background(), name, routeAddr, rf.newMethodTable())
	if err != nil {
		return nil, errors.WithMessage(err, "fail to create channel")
	}
	err = ch.Start()
	if err != nil {
		return nil, errors.WithMessage(err, "fail to start channel")
	}

	rf.channel = ch
	return rf, nil
}

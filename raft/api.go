package raft

import (
	"context"
	"errors"

	"github.com/ccbhj/raft_lab/log"
	"github.com/ccbhj/raft_lab/rpc"
)

func (r *Raft) newMethodTable() map[string]rpc.MethodInfo {
	var apis = map[string]rpc.MethodInfo{
		string(RPCRequestVote): {
			Request:  func() interface{} { return &RequestVoteRequest{} },
			Response: func() interface{} { return &RequestVoteResponse{} },
			Handler:  r.requestVoteHandler,
		},
		string(RPCAppendEntries): {
			Request:  func() interface{} { return &AppendEntriesRequest{} },
			Response: func() interface{} { return &AppendEntriesResponse{} },
			Handler:  r.appendEntriesHandler,
		},
	}

	return apis
}

func (rf *Raft) requestVoteHandler(ctx context.Context, req, resp interface{}) error {
	args := req.(*RequestVoteRequest)
	reply := resp.(*RequestVoteResponse)
	doneCh := make(chan interface{}, 1)
	rf.msgCh <- rpcArgsMsg{
		ReqId:   log.GetRequestIdFromCtx(ctx),
		RPCType: RPCRequestVote,
		Args:    *args,
		DoneCh:  doneCh,
	}
	r := (<-doneCh).(RequestVoteResponse)
	*reply = r

	return nil
}

func (rf *Raft) requestVote(msg rpcArgsMsg) {
	args := msg.Args.(RequestVoteRequest)
	reply := RequestVoteResponse{}
	ctx := rf.newContext(msg.ReqId)
	logger := getLogger(ctx)

	logger.Info("start request vote, req=%+v", args)

	defer func() {
		msg.DoneCh <- reply
		logger.Info("done request vote, resp=%+v", reply)
	}()

	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	reply.VoteGrant = false

	if args.Term < rf.currentTerm {
		//  do NOTE vote for smaller term
		return
	} else if currentTerm < args.Term {
		// we might not be a follower for now
		// turn leader or candidate into follow here
		rf.initFollower(ctx)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	if vf := rf.voteFor; (vf == args.CandidateId || vf == NOT_VOTE) && rf.isMoreUpToDate(ctx, args.LastLogTerm, args.LastLogIdx) {
		reply.VoteGrant = true
		rf.voteFor = args.CandidateId
		// set the timer randomly
		rf.resetTimer(ctx, 0)
	}
	rf.persist()
}

func (rf *Raft) appendEntriesHandler(ctx context.Context, req, resp interface{}) error {
	args := req.(*AppendEntriesRequest)
	reply := resp.(*AppendEntriesResponse)
	doneCh := make(chan interface{}, 1)
	rf.msgCh <- rpcArgsMsg{
		ReqId:   log.GetRequestIdFromCtx(ctx),
		RPCType: RPCAppendEntries,
		Args:    *args,
		DoneCh:  doneCh,
	}
	r := (<-doneCh).(AppendEntriesResponse)
	*reply = r

	return nil
}

func (rf *Raft) appendEntries(msg rpcArgsMsg) {
	args := msg.Args.(AppendEntriesRequest)
	reply := AppendEntriesResponse{}
	ctx := rf.newContext(msg.ReqId)
	logger := getLogger(ctx)

	logger.Info("start append entries, req=%+v", args)

	defer func() {
		msg.DoneCh <- reply
		logger.Info("done append entries, resp=%+v, rf.commitIdx=%d",
			reply, rf.commitIdx)
	}()

	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	reply.Success = false
	if l := len(rf.logs); l > 0 {
		reply.LastLogIdx = rf.logs[l-1].Index
	}

	if args.Term < rf.currentTerm {
		// expired request or from expired leader
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.initFollower(ctx)
	}
	reply.Success = rf.mergeLogEntries(ctx, args)
	if reply.Success && args.LeaderCommit > rf.commitIdx {
		rf.commitIdx = MinInt64(reply.LastLogIdx, args.LeaderCommit)
		rf.sendApplyMsg(ctx)
	}

	if rf.state != FOLLOWER {
		rf.initFollower(ctx)
	}

	rf.persist()
	rf.resetTimer(ctx, 0)
}

func (rf *Raft) Applied() <-chan ApplyMsg {
	return rf.applyCh
}

func (rf *Raft) Name() string {
	return rf.channel.Name()
}

func (rf *Raft) CommitCommond(cmd interface{}) (idx, term int64, isLeader bool) {
	reqId := rf.channel.GenRequestId()

	doneCh := make(chan struct {
		Err   error
		Index int64
		Term  int64
	}, 1)
	rf.msgCh <- commitMsg{
		ReqId:   reqId,
		Commnad: cmd,
		DoneCh:  doneCh,
	}

	msg := <-doneCh
	return msg.Index, msg.Term, !errors.Is(msg.Err, ErrNotLeader)
}

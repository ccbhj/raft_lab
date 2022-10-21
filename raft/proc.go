package raft

import (
	"time"

	"github.com/ccbhj/raft_lab/log"
	"github.com/ccbhj/raft_lab/rpc"
)

func (rf *Raft) getStatusProc(msg getStatusMsg) {
	status := RaftStatus{
		Name:    rf.channel.Name(),
		State:   rf.state,
		Term:    rf.currentTerm,
		VoteFor: rf.voteFor,
	}

	msg.ch <- status
}

func (rf *Raft) tickerProc(msg tickerMsg) {
	// timeout, start election
	ctx := rf.newContext(msg.ReqId)
	rf.state = CANDIDATE
	getLogger(ctx).Info("timeout, become candidate, start election")
	rf.msgCh <- electionMsg{ReqId: msg.ReqId}
}

func (rf *Raft) electionProc(msg electionMsg) {
	ctx := rf.newContext(msg.ReqId)
	logger := getLogger(ctx)
	rf.state = CANDIDATE
	rf.voteCount = 1 // vote for myself
	rf.voteFor = rf.me
	rf.currentTerm++

	args := &RequestVoteRequest{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		LastLogIdx:  0,
		LastLogTerm: 0,
	}
	rf.persist()

	logger.Info("start election, timeout=%dms, args=%+v",
		rf.resetTimer(ctx, 0).Milliseconds(), args)

	rf.forEachPeers(ctx, func(peer string, routeTab map[string]rpc.RouteInfo) {
		reply := new(RequestVoteResponse)
		if err := rf.channel.Call(ctx, routeTab, peer, string(RPCRequestVote), args, reply); err != nil {
			logger.Error("fail to send request vote request to %s: %s", peer, err)
			return
		}
		rf.msgCh <- rpcReplyMsg{
			ReqId:   log.GetRequestIdFromCtx(ctx),
			RPCType: RPCRequestVote,
			Peer:    peer,
			Args:    *args,
			Reply:   *reply,
		}
	})
}

func (rf *Raft) voteReplyProc(msg rpcReplyMsg) {
	args := msg.Args.(RequestVoteRequest)
	reply := msg.Reply.(RequestVoteResponse)
	ctx := rf.newContext(msg.ReqId)
	logger := getLogger(ctx)

	logger.Debug("handling request_vote reply %+v", reply)

	// request is out-of-date
	if args.Term != rf.currentTerm {
		return
	}

	// there is a node with larger term
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.initFollower(ctx)
		rf.persist()
		return
	}

	if reply.VoteGrant {
		rf.voteCount++
	}

	// already got a majority of vote
	// become a leader
	peers, err := rf.channel.GetRouteTab()
	if err != nil {
		getLogger(ctx).Error("fail to get peers: %s", err)
		return
	}
	if len(peers) < 2 {
		panic("not enough peers")
	}
	getLogger(ctx).Debug("total peers %d, voteCount=%d", len(peers), rf.voteCount)
	if rf.voteCount >= (len(peers)/2)+1 {
		getLogger(ctx).Info("become leader")
		rf.state = LEADER
		rf.nextIdxes[rf.me] = int64(len(rf.logs))
		rf.matchIdxes[rf.me] = -1
		for peer := range peers {
			rf.nextIdxes[peer] = rf.nextIdxes[rf.me]
			rf.matchIdxes[peer] = -1
		}
		rf.resetTimer(ctx, time.Duration(GetRaftConfig().HeartBeatIntervalMs)*time.Millisecond)
		rf.msgCh <- sendHeartbeatMsg{msg.ReqId}
	}
}

func (rf *Raft) sendingHeartbeatProc(msg sendHeartbeatMsg) {
	ctx := rf.newContext(msg.ReqId)
	rf.forEachPeers(ctx, func(peer string, routeTab map[string]rpc.RouteInfo) {
		var (
			reply AppendEntriesResponse
			args  AppendEntriesRequest
		)
		args.RequestId = msg.ReqId
		rf.prepareAppendEntriesArgs(peer, &args)
		getLogger(ctx).Debug("send %d entries to peer %s", len(args.Entries), peer)
		go func() {
			err := rf.channel.Call(ctx, routeTab, peer, string(RPCAppendEntries), &args, &reply)
			if err == nil {
				rf.msgCh <- rpcReplyMsg{ReqId: msg.ReqId, RPCType: RPCAppendEntries, Peer: peer, Args: args, Reply: reply}
				return
			}
			getLogger(ctx).Error("fail to send %d entries to peer %s: %v", len(args.Entries), peer, err)
		}()
	})
	rf.resetTimer(ctx, time.Duration(GetRaftConfig().HeartBeatIntervalMs)*time.Millisecond)
}

func (rf *Raft) heartbeatReplyProc(msg rpcReplyMsg) {
	args := msg.Args.(AppendEntriesRequest)
	reply := msg.Reply.(AppendEntriesResponse)
	ctx := rf.newContext(msg.ReqId)
	logger := getLogger(ctx)

	logger.Debug("handling append_entrie reply %+v", reply)

	// filter out expired reply
	if args.Term != rf.currentTerm {
		return
	}
	// invalidate leader
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.initFollower(ctx)
		rf.persist()
		return
	}
	if args.Term != reply.Term || args.Term != rf.currentTerm {
		return
	}
	if reply.Success {
		if logs := args.Entries; len(logs) > 0 {
			last := logs[len(logs)-1]
			rf.nextIdxes[msg.Peer] = last.Index + 1
			rf.matchIdxes[msg.Peer] = last.Index
		}
	} else {
		// rf.nextIdxes[msg.Peer] = MaxInt64(MinInt64(rf.nextIdxes[msg.Peer]-1, reply.LastLog+1), 1)
	}
}

package raft

import "time"

type RequestVoteRequest struct {
	Term        int64  `json:"term"`
	CandidateId string `json:"candidate_id"`
	LastLogIdx  int64  `json:"last_log_idx"`
	LastLogTerm int64  `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term      int64 `json:"term"`
	VoteGrant bool  `json:"vote_grant"`
}

type AppendEntriesResponse struct {
	RequestId string
	Term      int64
	Success   bool

	LastLogIdx int64
	// the index of a peer's last log entry
	// ConfilctTerm int64
	// ConfilctIdx  int64
}

type AppendEntriesRequest struct {
	RequestId string
	Term      int64
	LeaderID  string

	PrevLogIdx   int64
	PrevLogTerm  int64
	LeaderCommit int64
	Entries      []LogEntry
}

type Msg interface{}

type getStatusMsg struct {
	ch chan RaftStatus
}

type rpcArgsMsg struct {
	ReqId   string
	RPCType RPCType
	// do not use pointer
	// copy the struct
	Args   interface{}
	DoneCh chan interface{}
}

type rpcReplyMsg struct {
	ReqId   string
	RPCType RPCType
	Peer    string
	// do not use pointer
	// copy the struct
	Args  interface{}
	Reply interface{}
}

type sendApplyMsgMsg struct {
	ReqId       string
	LastApplied int64
	CommitIdx   int64
	Replay      bool
	Logs        []LogEntry
}

type persistMsg struct {
	CurrentTerm      int64
	VoteFor          int32
	Logs             []LogEntry
	Snapshot         []byte
	CommitIdx        int64
	LastApplied      int64
	LastIncludedIdx  int64
	LastIncludedTerm int64
}

type tickerMsg struct {
	ReqId string
}

type electionMsg struct {
	ReqId string
}

type sendHeartbeatMsg struct {
	ReqId string
}

type commitMsg struct {
	ReqId   string
	Commnad interface{}
	DoneCh  chan struct {
		Err   error
		Index int64
		Term  int64
	}
}

type getLogMsg struct {
	DoneCh chan []LogEntry
}

type TimeoutEvent struct {
	LastTimeout time.Time
	Timeout     time.Duration
}

package raft

const msgQueueSize = 1 << 10

// context key
const (
	raftIdKey string = "raft_id"
	termKey          = "term"
	stateKey         = "state"
)

type State string

const NOT_VOTE = ""

const (
	FOLLOWER  State = "Follower"
	CANDIDATE       = "Candidate"
	LEADER          = "Leader"
)

func (s State) String() string {
	return string(s)
}

type RPCType string

const (
	// RPC type for sendRPCWithCtx()
	RPCRequestVote   RPCType = "request_vote"
	RPCAppendEntries RPCType = "append_entries"
)

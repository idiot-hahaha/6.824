package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID            int64
	LastCommandID int64
	LastCallCount int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID            int64
	LastCommandID int64
	LastCallCount int
}

type GetReply struct {
	Err   Err
	Value string
}

// my code

type IsLeaderArgs struct {
}

type IsLeaderReply struct {
	IsLeader bool
}

type TestArgs struct {
}

type TestReply struct {
}

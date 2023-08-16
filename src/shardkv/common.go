package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongNum    = "ErrWrongNum"
	//OpPut          = "Put"
	//OpAppend       = "Append"
	//OpGet          = "Get"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID  int64
	CmdIndex int
	LastNum  int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID  int64
	CmdIndex int
	LastNum  int
}

type GetReply struct {
	Err   Err
	Value string
}

type TransmitArgs struct {
	LastNum  int
	Shard    int
	ShardNum int
}

type TransmitReply struct {
	LastNum   int
	ConfigNum int
	Data      []byte
	Err       string
}

type DeleteArgs struct {
	LastNum  int
	Shard    int
	ShardNum int
}

type DeleteReply struct {
	LastNum int
	Err     string
}

package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PutMethod    = "Put"
	AppendMethod = "Append"
	useCond      = true
	useSnapshot  = false
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation     string
	Key           string
	Value         string
	ID            int64
	LastCommandID int64
	LastCallCount int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kVMap             map[string]string
	commandReplyCount map[int64]int
	checkCond         *sync.Cond
	commitIndex       int
	//commandSet        map[int64]struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("server(%d) get GetMsg:%+v", kv.me, args)
	op := Op{
		Operation:     "Get",
		Key:           args.Key,
		Value:         "",
		ID:            args.ID,
		LastCommandID: args.LastCommandID,
		LastCallCount: args.LastCallCount,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		if useCond {
			kv.checkCond.Wait()
		} else {
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 5)
			kv.mu.Lock()
		}
		if kv.killed() {
			DPrintf("kvServer(%d) is killed", kv.me)
		}
		if _, isLeader = kv.rf.GetState(); !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if index <= kv.commitIndex {
			break
		}
		DPrintf("check command(%d),index:%d rf.GetLastLogIndexL:%d", op.ID, index, kv.rf.GetLastLogIndex())
	}
	v, ok := kv.kVMap[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return
	}
	reply.Err = OK
	reply.Value = v
	DPrintf("command(%d) complete", op.ID)
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("server(%d) get PutAppendMsg:%+v", kv.me, args)
	if _, ok := kv.commandReplyCount[args.ID]; ok {
		DPrintf("command%+v has been applied", args.Op)
		reply.Err = OK
		return
	}
	op := Op{
		Operation:     "",
		Key:           "",
		Value:         "",
		ID:            args.ID,
		LastCommandID: args.LastCommandID,
		LastCallCount: args.LastCallCount,
	}
	op.Key = args.Key
	op.Value = args.Value
	if args.Op == "Append" {
		op.Operation = AppendMethod
	} else if args.Op == "Put" {
		op.Operation = PutMethod
	} else {
		panic("args err")
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		if useCond {
			kv.checkCond.Wait()
		} else {
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 5)
			kv.mu.Lock()
		}
		if _, isLeader = kv.rf.GetState(); !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if index <= kv.commitIndex {
			break
		}
		DPrintf("check command(%d),index:%d rf.GetLastLogIndexL:%d", op.ID, index, kv.rf.GetLastLogIndex())
	}
	reply.Err = OK
	DPrintf("command(%d) complete", op.ID)
	return
}

func (kv *KVServer) PingLeader(args *TestArgs, reply *TestReply) {
	DPrintf("get ping leader msg")
	_, isLeader := kv.rf.GetState()
	if isLeader {
		DPrintf("PingLeader success")
	} else {
		DPrintf("PingLeader fail")
	}
}

func (kv *KVServer) IsLeader(args *IsLeaderArgs, reply *IsLeaderReply) {
	_, isLeader := kv.rf.GetState()
	reply.IsLeader = isLeader
	if isLeader {
		DPrintf("server(%d) is leader", kv.me)
	}
}

func (kv *KVServer) Snapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kVMap)
	e.Encode(kv.commandReplyCount)
	b := w.Bytes()
	kv.rf.Snapshot(index, b)
	DPrintf("KVServer(%d) snapshot(%d)\nkVMap:%v\ncommandSet:%v", kv.me, index, kv.kVMap, kv.commandReplyCount)
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if kv.killed() {
			kv.checkCond.Broadcast()
			return
		}
		kv.mu.Lock()
		DPrintf("applier(%d) apply msg:%+v", kv.me, m)
		kv.mu.Unlock()
		if m.SnapshotValid {
			DPrintf("applier(%d) apply snapshot %d", kv.me, m.SnapshotIndex)
			b := bytes.NewBuffer(m.Snapshot)
			d := labgob.NewDecoder(b)
			var kvMap map[string]string
			//var commandSet map[int64]struct{}
			var commandReplyCount map[int64]int
			if d.Decode(&kvMap) != nil || d.Decode(&commandReplyCount) != nil {
				panic("Decode Err")
			}
			kv.mu.Lock()
			kv.kVMap = kvMap
			kv.commandReplyCount = commandReplyCount
			DPrintf("KVServer(%d) load snapshot\nkVMap:%v\ncommandSet:%v", kv.me, kv.kVMap, kv.commandReplyCount)
			kv.commitIndex = m.SnapshotIndex
			kv.checkCond.Broadcast()
			kv.mu.Unlock()
		} else if m.CommandValid {
			op, ok := m.Command.(Op)
			if !ok {
				panic("type err")
			}
			kv.mu.Lock()
			if op.Operation == AppendMethod {
				if _, ok := kv.commandReplyCount[op.ID]; !ok {
					kv.kVMap[op.Key] = string(append([]byte(kv.kVMap[op.Key]), op.Value...))
					DPrintf("KVMap:%v", kv.kVMap)
				}
			} else if op.Operation == PutMethod {
				if _, ok := kv.commandReplyCount[op.ID]; !ok {
					kv.kVMap[op.Key] = op.Value
					DPrintf("KVMap:%v", kv.kVMap)
				}
			}
			kv.commandReplyCount[op.ID]--
			if kv.commandReplyCount[op.ID] == 0 {
				delete(kv.commandReplyCount, op.ID)
			}
			if v, ok := kv.commandReplyCount[op.LastCommandID]; ok && v < 0 {
				newCount := v + op.LastCallCount
				if newCount == 0 {
					delete(kv.commandReplyCount, op.LastCommandID)
				} else {
					kv.commandReplyCount[op.LastCommandID] = newCount
				}
			}
			DPrintf("applier(%d) m.CommandIndex:%d", kv.me, m.CommandIndex)
			if useSnapshot && m.CommandIndex%10 == 0 {
				kv.Snapshot(m.CommandIndex)
			}
			kv.commitIndex = m.CommandIndex
			kv.checkCond.Broadcast()
			kv.mu.Unlock()
		}
	}
	//kv.checkCond.Broadcast()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kVMap = map[string]string{}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.commandReplyCount = map[int64]int{}
	kv.checkCond = sync.NewCond(&kv.mu)

	// You may need initialization code here.

	go kv.applier()

	if useCond {
		go func() {
			kv.mu.Lock()
			for !kv.killed() {
				kv.mu.Unlock()
				time.Sleep(time.Millisecond * 200)
				kv.mu.Lock()
				kv.checkCond.Broadcast()
			}
			kv.checkCond.Broadcast()
			kv.mu.Unlock()
		}()
	}

	return kv
}

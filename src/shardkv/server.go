package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
	"unsafe"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const (
	SHARDS      = 10
	Put         = "Put"
	Append      = "Append"
	Get         = "Get"
	SetShard    = "SetShard"
	GetShard    = "GetShard"
	SetConfig   = "SetConfig"
	DeleteShard = "DeleteShard"
	OpSize      = int(unsafe.Sizeof(Op{}))
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	LastNum   int // ?
	ClientID  int64
	ClientCmd int

	Key   string
	Value string

	Shard    int
	ShardNum int
	Data     []byte
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32 // set by Kill()
	mck  *shardctrler.Clerk

	db         []map[string]string
	clientCmd  []map[int64]int
	shardNum   []int
	config     shardctrler.Config
	applyIndex int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.LastNum < kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if args.LastNum > kv.config.Num {
		reply.Err = ErrWrongGroup
		kv.config = kv.mck.Query(-1)
		return
	}
	shard := key2shard(args.Key)
	configNum := kv.shardNum[shard]
	if configNum != kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	op := Op{
		Operation: Get,
		Key:       args.Key,
		ClientID:  args.ClerkID,
		ClientCmd: args.CmdIndex,
		LastNum:   kv.config.Num,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//for
	for kv.clientCmd[shard][args.ClerkID] < args.CmdIndex {
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
		kv.mu.Lock()
	}
	reply.Value = kv.db[shard][args.Key]
	reply.Err = OK
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.LastNum < kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if args.LastNum > kv.config.Num {
		reply.Err = ErrWrongGroup
		kv.config = kv.mck.Query(-1)
		return
	}
	shard := key2shard(args.Key)
	configNum := kv.shardNum[shard]
	if configNum != kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	op := Op{
		Operation: Put,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClerkID,
		ClientCmd: args.CmdIndex,
		LastNum:   kv.config.Num,
	}
	if args.Op == "Append" {
		op.Operation = Append
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for kv.clientCmd[shard][args.ClerkID] < args.CmdIndex {
		DPrintf("wait CmdIndex:%d", args.CmdIndex)
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
		kv.mu.Lock()
	}
	reply.Err = OK
	return
}

func (kv *ShardKV) GetShard(args *TransmitArgs, reply *TransmitReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.LastNum = kv.config.Num
	if args.LastNum < kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if args.LastNum > kv.config.Num {
		kv.config = kv.mck.Query(-1)
	}
	if args.ShardConfigNum != kv.shardNum[args.Shard] {
		reply.Err = ErrWrongNum
		reply.ConfigNum = kv.shardNum[args.Shard]
		return
	} else {
		op := Op{
			Operation: GetShard,
			LastNum:   kv.config.Num,
		}
		idx, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		for idx > kv.applyIndex {
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 100)
			kv.mu.Lock()
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		if err := e.Encode(kv.db[args.Shard]); err != nil {
			panic(err)
		}
		if err := e.Encode(kv.clientCmd[args.Shard]); err != nil {
			panic(err)
		}
		reply.Err = OK
		reply.Data = w.Bytes()
		return
	}
}

func (kv *ShardKV) SendShard(args *TransmitArgs, reply *TransmitReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.LastNum = kv.config.Num
	if args.LastNum < kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if args.LastNum > kv.config.Num {
		kv.config = kv.mck.Query(-1)
	}
	if args.ShardConfigNum != kv.shardNum[args.Shard] {
		reply.Err = ErrWrongNum
		reply.ConfigNum = kv.shardNum[args.Shard]
		return
	} else {
		op := Op{
			Operation: GetShard,
			LastNum:   kv.config.Num,
		}
		idx, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		for idx > kv.applyIndex {
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 100)
			kv.mu.Lock()
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		if err := e.Encode(kv.db[args.Shard]); err != nil {
			panic(err)
		}
		if err := e.Encode(kv.clientCmd[args.Shard]); err != nil {
			panic(err)
		}
		reply.Err = OK
		reply.Data = w.Bytes()
		return
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) Killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.db = make([]map[string]string, SHARDS)
	for i := 0; i < SHARDS; i++ {
		kv.db[i] = make(map[string]string)
	}
	kv.clientCmd = make([]map[int64]int, SHARDS)
	for i := 0; i < SHARDS; i++ {
		kv.clientCmd[i] = make(map[int64]int)
	}
	kv.shardNum = make([]int, 10)
	kv.config.Num = 0
	kv.config.Shards = [10]int{}
	kv.applyIndex = 0
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applier()
	go kv.updateConfig()
	return kv
}
func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if kv.Killed() {
			return
		}
		if m.SnapshotValid {
			kv.snapshotInstall(&m)
		} else if m.CommandValid {
			op, ok := m.Command.(Op)
			kv.mu.Lock()
			DPrintf("server-%d-%d get cmd msg:%+v", kv.gid, kv.me, op)
			if kv.config.Num < op.LastNum {
				kv.config = kv.mck.Query(-1)
			} else if kv.config.Num > op.LastNum {

			}
			if !ok {
				panic("type err")
			}
			switch op.Operation {
			case Get:
				kv.getHandler(&op)
			case Put:
				kv.putHandler(&op)
			case Append:
				kv.appendHandler(&op)
			case SetShard:
				kv.setShardHandler(&op)
			case SetConfig:

			case GetShard:

			case DeleteShard:
				kv.deleteShardHandler(&op)
			default:
				panic("undefined operation")
			}
			kv.applyIndex = m.CommandIndex
			if kv.maxraftstate != -1 && kv.applyIndex%max(1, (kv.maxraftstate/OpSize)) == 0 {
				kv.snapshot(kv.applyIndex)
			}
			kv.mu.Unlock()
		} else {
			panic("wrong cmd type")
		}
		kv.mu.Lock()
		DPrintf("server-%d-%d shardNum:%+v config:%+v DB:%+v", kv.gid, kv.me, kv.shardNum, kv.config, kv.db)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) updateConfig() {
	for !kv.Killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			config := kv.mck.Query(-1)
			if config.Num > kv.config.Num {
				kv.config = config
			}
			for i := 0; i < SHARDS; i++ {
				if kv.config.Shards[i] == kv.gid && kv.shardNum[i] < kv.config.Num {
					//kv.getShardDataL(kv.config.Num, i)
					go kv.getShardData(kv.config.Num, i)
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 80)
	}
}

func (kv *ShardKV) getShardDataL(targetNum, shard int) {
	args := TransmitArgs{
		LastNum:        kv.config.Num,
		Shard:          shard,
		ShardConfigNum: 0,
	}
	queryNum := targetNum - 1
	for queryNum > kv.shardNum[shard] {
		args.ShardConfigNum = queryNum
		config := kv.mck.Query(queryNum)
		gid := config.Shards[shard]
		if gid == kv.gid {
			queryNum--
			continue
		}
		servers := config.Groups[gid]
		for i := 0; i < len(servers); i++ {
			srv := kv.make_end(servers[i])
			var reply TransmitReply
			kv.mu.Unlock()
			if ok := srv.Call("ShardKV.GetShard", &args, &reply); !ok {
				if i == len(servers)-1 {
					panic("all offline")
				}
				continue
			}
			kv.mu.Lock()
			if reply.Err == ErrWrongLeader {
				continue
			}
			if reply.Err == ErrWrongGroup {
				kv.config = kv.mck.Query(-1)
				return
			}
			if reply.Err == ErrWrongNum {
				if reply.ConfigNum < queryNum {
					queryNum--
				} else if reply.ConfigNum > queryNum {
					if reply.ConfigNum > targetNum {
						return
					} else if reply.ConfigNum == targetNum {
						panic("")
					} else {
						queryNum = targetNum - 1
					}
				} else {
					panic("err")
				}
				break
			}
			if reply.Err == OK {
				op := Op{
					Operation: SetShard,
					LastNum:   kv.config.Num,
					Data:      reply.Data,
					ShardNum:  targetNum,
					Shard:     shard,
				}
				_, _, isLeader := kv.rf.Start(op)
				if !isLeader {
					panic("is not leader")
				}
			}
		}
	}
	if queryNum == 0 {
		emptyDB := map[string]string{}
		emptyClientCmd := map[string]string{}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(emptyDB)
		e.Encode(emptyClientCmd)
		op := Op{
			Operation: SetShard,
			LastNum:   kv.config.Num,
			Data:      w.Bytes(),
			ShardNum:  targetNum,
			Shard:     shard,
		}
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			panic("is not leader")
		}
	} else {
		op := Op{
			Operation: SetShard,
			LastNum:   kv.config.Num,
			Data:      []byte{},
			ShardNum:  targetNum,
			Shard:     shard,
		}
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			panic("is not leader")
		}
	}
}

func (kv *ShardKV) putHandler(op *Op) {
	shard := key2shard(op.Key)
	if kv.clientCmd[shard][op.ClientID] < op.ClientCmd {
		kv.db[shard][op.Key] = op.Value
		kv.clientCmd[shard][op.ClientID] = op.ClientCmd
	}
}

func (kv *ShardKV) appendHandler(op *Op) {
	shard := key2shard(op.Key)
	if kv.clientCmd[shard][op.ClientID] < op.ClientCmd {
		kv.db[shard][op.Key] += op.Value
		kv.clientCmd[shard][op.ClientID] = op.ClientCmd
	}
}
func (kv *ShardKV) getHandler(op *Op) {
	shard := key2shard(op.Key)
	if kv.clientCmd[shard][op.ClientID] < op.ClientCmd {
		kv.clientCmd[shard][op.ClientID] = op.ClientCmd
	}
}
func (kv *ShardKV) setShardHandler(op *Op) {
	if kv.shardNum[op.Shard] >= op.ShardNum {
		return
	}
	if len(op.Data) == 0 {
		kv.shardNum[op.Shard] = op.ShardNum
		return
	}
	var shard map[string]string
	var clientCmd map[int64]int
	r := bytes.NewBuffer(op.Data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&shard); err != nil {
		panic(err)
	}
	if err := d.Decode(&clientCmd); err != nil {
		panic(err)
	}
	kv.db[op.Shard] = shard
	kv.shardNum[op.Shard] = op.ShardNum
	for k, v := range clientCmd {
		kv.clientCmd[op.Shard][k] = v
	}
}

func (kv *ShardKV) deleteShardHandler(op *Op) {
	kv.db[op.Shard] = make(map[string]string)
}

func (kv *ShardKV) snapshotInstall(m *raft.ApplyMsg) {
	r := bytes.NewBuffer(m.Snapshot)
	d := labgob.NewDecoder(r)
	var db []map[string]string
	var clientCmd []map[int64]int
	var config shardctrler.Config
	var shardNum []int
	var applyIndex int
	if err := d.Decode(&db); err != nil {
		panic(err)
	}
	if err := d.Decode(&clientCmd); err != nil {
		panic(err)
	}
	if err := d.Decode(&config); err != nil {
		panic(err)
	}
	if err := d.Decode(&shardNum); err != nil {
		panic(err)
	}
	if err := d.Decode(&applyIndex); err != nil {
		panic(err)
	}
	kv.mu.Lock()
	kv.db = db
	kv.clientCmd = clientCmd
	kv.config = config
	kv.shardNum = shardNum
	kv.applyIndex = applyIndex
	kv.mu.Unlock()
}

func (kv *ShardKV) snapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.clientCmd)
	e.Encode(kv.config)
	e.Encode(kv.shardNum)
	e.Encode(kv.applyIndex)
	b := w.Bytes()
	kv.rf.Snapshot(index, b)
}

func (kv *ShardKV) getShardData(targetNum, shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := TransmitArgs{
		LastNum:        kv.config.Num,
		Shard:          shard,
		ShardConfigNum: 0,
	}
	queryNum := targetNum - 1
	for queryNum > kv.shardNum[shard] {
		args.ShardConfigNum = queryNum
		config := kv.mck.Query(queryNum)
		gid := config.Shards[shard]
		if gid == kv.gid {
			queryNum--
			continue
		}
		servers := config.Groups[gid]
		for i := 0; i < len(servers); i++ {
			srv := kv.make_end(servers[i])
			var reply TransmitReply
			kv.mu.Unlock()
			if ok := srv.Call("ShardKV.GetShard", &args, &reply); !ok {
				//if i == len(servers)-1 {
				//	queryNum--
				//	kv.mu.Lock()
				//	break
				//}
				kv.mu.Lock()
				continue
			}
			kv.mu.Lock()
			if reply.Err == ErrWrongLeader {
				continue
			}
			if reply.Err == ErrWrongGroup {
				kv.config = kv.mck.Query(-1)
				return
			}
			if reply.Err == ErrWrongNum {
				if reply.ConfigNum < queryNum {
					queryNum--
				} else if reply.ConfigNum > queryNum {
					if reply.ConfigNum > targetNum {
						return
					} else if reply.ConfigNum == targetNum {
						panic("")
					} else {
						queryNum = targetNum - 1
					}
				} else {
					panic("err")
				}
				break
			}
			if reply.Err == OK {
				op := Op{
					Operation: SetShard,
					LastNum:   kv.config.Num,
					Data:      reply.Data,
					ShardNum:  targetNum,
					Shard:     shard,
				}
				_, _, isLeader := kv.rf.Start(op)
				if !isLeader {
					//panic("is not leader")
				}
			}
		}
	}
	if queryNum == 0 {
		emptyDB := map[string]string{}
		emptyClientCmd := map[int64]int{}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(emptyDB)
		e.Encode(emptyClientCmd)
		op := Op{
			Operation: SetShard,
			LastNum:   kv.config.Num,
			Data:      w.Bytes(),
			ShardNum:  targetNum,
			Shard:     shard,
		}
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			//panic("is not leader")
		}
	} else {
		op := Op{
			Operation: SetShard,
			LastNum:   kv.config.Num,
			Data:      []byte{},
			ShardNum:  targetNum,
			Shard:     shard,
		}
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			//panic("is not leader")
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

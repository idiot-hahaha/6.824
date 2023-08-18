package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const (
	NSHARDS     = 10
	Put         = "Put"
	Append      = "Append"
	Get         = "Get"
	SetShard    = "SetShard"
	SetConfig   = "SetConfig"
	DeleteShard = "DeleteShard"
	OpSize      = int(unsafe.Sizeof(Op{}))
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	LastNum   int
	CallerID  int64
	CmdIndex  int

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
	callerCmd  []map[int64]int
	shardNum   []int
	config     shardctrler.Config
	applyIndex int

	ID       int64
	cmdIndex int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.LastNum != kv.config.Num { // || kv.shardNum[shard] != kv.config.Num
		reply.Err = ErrWrongGroup
		return
	}
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		panic("err")
	}
	op := Op{
		Operation: Get,
		Key:       args.Key,
		CallerID:  args.ClerkID,
		CmdIndex:  args.CmdIndex,
		LastNum:   args.LastNum,
	}
	idx, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for !kv.Killed() && idx > kv.applyIndex {
		if term, isLeader := kv.rf.GetState(); !isLeader || term != startTerm {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 5)
		kv.mu.Lock()
	}
	if kv.callerCmd[shard][args.ClerkID] < args.CmdIndex || kv.shardNum[shard] != args.LastNum {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Value = kv.db[shard][args.Key]
	reply.Err = OK
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.LastNum != kv.config.Num { // || kv.shardNum[shard] != kv.config.Num
		reply.Err = ErrWrongGroup
		return
	}
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		panic("err")
	}
	op := Op{
		Operation: Put,
		Key:       args.Key,
		Value:     args.Value,
		CallerID:  args.ClerkID,
		CmdIndex:  args.CmdIndex,
		LastNum:   args.LastNum,
	}
	if args.Op == "Append" {
		op.Operation = Append
	}
	idx, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for !kv.Killed() && idx > kv.applyIndex {
		if term, isLeader := kv.rf.GetState(); !isLeader || term != startTerm {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 5)
		kv.mu.Lock()
	}
	if kv.callerCmd[shard][args.ClerkID] < args.CmdIndex {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Err = OK
	return
}

func (kv *ShardKV) GetShard(args *TransmitArgs, reply *TransmitReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.LastNum = kv.config.Num
	if args.LastNum != kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if args.ShardNum != kv.shardNum[args.Shard] {
		reply.Err = ErrWrongNum
		reply.ConfigNum = kv.shardNum[args.Shard]
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.db[args.Shard]); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.callerCmd[args.Shard]); err != nil {
		panic(err)
	}
	reply.Err = OK
	reply.Data = w.Bytes()
	return
}

func (kv *ShardKV) DeleteShard(args *DeleteArgs, reply *DeleteReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.LastNum = kv.config.Num
	if args.LastNum != kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.shardNum[args.Shard] != args.ShardNum {
		num := kv.shardNum[args.Shard]
		if num < 0 {
			num *= -1
		}
		if num < args.ShardNum {
			panic(fmt.Sprintf("err num:%d, args.ShardNum:%d", num, args.ShardNum))
		}
		reply.Err = OK
		return
	}
	op := Op{
		Operation: DeleteShard,
		Shard:     args.Shard,
		ShardNum:  args.ShardNum,
		LastNum:   args.LastNum,
	}
	idx, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for !kv.Killed() && idx >= kv.applyIndex {
		if term, isLeader := kv.rf.GetState(); !isLeader || term != startTerm {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 5)
		kv.mu.Lock()
	}
	if kv.shardNum[args.Shard] != args.ShardNum {
		num := kv.shardNum[args.Shard]
		if num < 0 {
			num *= -1
		}
		if num < args.ShardNum {
			panic("err")
		}
		reply.Err = OK
		return
	}
	reply.Err = ErrWrongGroup
	return
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
	kv.db = make([]map[string]string, NSHARDS)
	for i := 0; i < NSHARDS; i++ {
		kv.db[i] = make(map[string]string)
	}
	kv.callerCmd = make([]map[int64]int, NSHARDS)
	for i := 0; i < NSHARDS; i++ {
		kv.callerCmd[i] = make(map[int64]int)
	}
	kv.shardNum = make([]int, 10)
	kv.config.Num = 0
	kv.config.Shards = [10]int{}
	kv.applyIndex = 0
	kv.ID = nrand()
	kv.cmdIndex = 0
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.SetName(fmt.Sprintf("server-%d-%d", gid, me))

	go kv.applier()
	go kv.updateConfig()
	go kv.updateShard()

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
			case SetConfig:
				kv.setConfigHandler(&op)
			case SetShard:
				kv.setShardHandler(&op)
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

func (kv *ShardKV) updateShard() {
	for !kv.Killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			wg := sync.WaitGroup{}
			kv.mu.Lock()
			for i := 0; i < NSHARDS; i++ {
				if kv.config.Shards[i] == kv.gid && kv.config.Num > kv.shardNum[i] {
					wg.Add(1)
					go func(i int) {
						kv.getShardData(i)
						wg.Done()
					}(i)
				}
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(time.Millisecond * 80)
	}
}

func (kv *ShardKV) updateConfig() {
	for !kv.Killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			config := kv.mck.Query(-1)
			kv.mu.Lock()
			DPrintf("server-%d-%d check config", kv.gid, kv.me)
			if config.Num > kv.config.Num {
				DPrintf("server-%d-%d update config", kv.gid, kv.me)
				kv.sendConfig(config.Num)
			}
			kv.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 80)
	}
}

func (kv *ShardKV) sendConfig(configNum int) {
	op := Op{
		Operation: SetConfig,
		LastNum:   configNum,
	}
	kv.rf.Start(op)
}

func (kv *ShardKV) putHandler(op *Op) {
	if op.LastNum != kv.config.Num {
		return
	}
	shard := key2shard(op.Key)
	if kv.shardNum[shard] < kv.config.Num {
		return
	} else if kv.shardNum[shard] > kv.config.Num {
		panic("err")
	}
	if kv.callerCmd[shard][op.CallerID] < op.CmdIndex {
		kv.db[shard][op.Key] = op.Value
		kv.callerCmd[shard][op.CallerID] = op.CmdIndex
	}
}

func (kv *ShardKV) appendHandler(op *Op) {
	if op.LastNum != kv.config.Num {
		return
	}
	shard := key2shard(op.Key)
	if kv.shardNum[shard] < kv.config.Num {
		return
	} else if kv.shardNum[shard] > kv.config.Num {
		panic("err")
	}
	if kv.callerCmd[shard][op.CallerID] < op.CmdIndex {
		kv.db[shard][op.Key] += op.Value
		kv.callerCmd[shard][op.CallerID] = op.CmdIndex
	}
}
func (kv *ShardKV) getHandler(op *Op) {
	if op.LastNum != kv.config.Num {
		return
	}
	shard := key2shard(op.Key)
	if kv.shardNum[shard] < kv.config.Num {
		return
	} else if kv.shardNum[shard] > kv.config.Num {
		panic("err")
	}
	if kv.callerCmd[shard][op.CallerID] < op.CmdIndex {
		kv.callerCmd[shard][op.CallerID] = op.CmdIndex
	}
}
func (kv *ShardKV) setShardHandler(op *Op) {
	if op.LastNum != kv.config.Num {
		return
	}
	if kv.shardNum[op.Shard] >= op.ShardNum {
		return
	}
	if len(op.Data) == 0 {
		kv.shardNum[op.Shard] = op.ShardNum
		return
	}
	var shard map[string]string
	var callerCmd map[int64]int
	r := bytes.NewBuffer(op.Data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&shard); err != nil {
		panic(err)
	}
	if err := d.Decode(&callerCmd); err != nil {
		panic(err)
	}
	kv.db[op.Shard] = shard
	kv.shardNum[op.Shard] = op.ShardNum
	for k, v := range callerCmd {
		kv.callerCmd[op.Shard][k] = v
	}
}

func (kv *ShardKV) deleteShardHandler(op *Op) {
	if kv.shardNum[op.Shard] != op.ShardNum {
		return
	}
	kv.db[op.Shard] = make(map[string]string)
	kv.callerCmd[op.Shard] = make(map[int64]int)
	kv.shardNum[op.Shard] *= -1
}

func (kv *ShardKV) setConfigHandler(op *Op) {
	if kv.config.Num >= op.LastNum {
		return
	}
	kv.mu.Unlock()
	config := kv.mck.Query(op.LastNum)
	kv.mu.Lock()
	kv.config = config
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
	kv.callerCmd = clientCmd
	kv.config = config
	kv.shardNum = shardNum
	kv.applyIndex = applyIndex
	kv.mu.Unlock()
}

func (kv *ShardKV) snapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.callerCmd)
	e.Encode(kv.config)
	e.Encode(kv.shardNum)
	e.Encode(kv.applyIndex)
	b := w.Bytes()
	kv.rf.Snapshot(index, b)
}

func (kv *ShardKV) getShardData(shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Shards[shard] != kv.gid {
		return
	}
	DPrintf("server-%d-%d update shard%d, configNum:%d, shardNum:%d", kv.gid, kv.me, shard, kv.config.Num, kv.shardNum[shard])
	args := TransmitArgs{
		LastNum:  kv.config.Num,
		Shard:    shard,
		ShardNum: 0,
	}
	targetNum, queryNum := kv.config.Num, kv.config.Num-1
	for queryNum > kv.shardNum[shard] && queryNum > 0 {
		if kv.Killed() {
			return
		}
		if targetNum != kv.config.Num {
			return
		}
		args.ShardNum = queryNum
		kv.mu.Unlock()
		config := kv.mck.Query(queryNum)
		kv.mu.Lock()
		gid := config.Shards[shard]
		if gid == kv.gid {
			if kv.shardNum[shard] < 0 && (-kv.shardNum[shard]) >= queryNum {
				return
			}
			queryNum--
			continue
		}
		servers := config.Groups[gid]
		for i := 0; i < len(servers); i++ {
			srv := kv.make_end(servers[i])
			var reply TransmitReply
			kv.mu.Unlock()
			if ok := srv.Call("ShardKV.GetShard", &args, &reply); !ok {
				kv.mu.Lock()
				if i == len(servers)-1 {
					return
				}
				DPrintf("fail:server-%d-%d call getShard to server-%d-%d, args:%+v", kv.gid, kv.me, gid, i, args)
				continue
			}
			kv.mu.Lock()
			if kv.shardNum[shard] >= queryNum {
				return
			}
			DPrintf("success:server-%d-%d call getShard to server-%d-%d, args:%+v, reply:%+v", kv.gid, kv.me, gid, i, args, reply)
			if reply.Err == ErrWrongLeader {
				continue
			}
			if reply.Err == ErrWrongGroup {
				return
			}
			if reply.Err == ErrWrongNum {
				if reply.ConfigNum < queryNum {
					if reply.ConfigNum*-1 > queryNum {
						panic("err")
					}
					queryNum--
				} else if reply.ConfigNum > queryNum {
					if reply.ConfigNum > targetNum {
						return
					} else if reply.ConfigNum == targetNum {
						panic("")
					} else {
						panic("err")
					}
				} else {
					panic("err")
				}
				break
			}
			if reply.Err == OK {
				op := Op{
					Operation: SetShard,
					LastNum:   args.LastNum,
					Data:      reply.Data,
					ShardNum:  targetNum,
					Shard:     shard,
				}
				idx, startTerm, isLeader := kv.rf.Start(op)
				if !isLeader {
					return
				}
				for !kv.Killed() && kv.applyIndex < idx {
					if term, isLeader := kv.rf.GetState(); !isLeader || term != startTerm {
						reply.Err = ErrWrongLeader
						return
					}
					kv.mu.Unlock()
					time.Sleep(time.Millisecond * 5)
					kv.mu.Lock()
				}
				if kv.shardNum[op.Shard] >= op.ShardNum {
					go kv.callDeleteShard(op.Shard, queryNum)
				}
				return
			} else {
				panic("err")
			}
		}
	}
	if kv.shardNum[shard] == targetNum {
		return
	}
	op := Op{
		Operation: SetShard,
		LastNum:   targetNum,
		Data:      []byte{},
		ShardNum:  targetNum,
		Shard:     shard,
	}
	kv.rf.Start(op)
}

func (kv *ShardKV) callDeleteShard(shard int, shardNum int) {
	config := kv.mck.Query(shardNum)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := DeleteArgs{
		LastNum:  kv.config.Num,
		Shard:    shard,
		ShardNum: shardNum,
	}
	servers := config.Groups[config.Shards[shard]]
	//for {
	for i, server := range servers {
		srv := kv.make_end(server)
		var reply DeleteReply
		kv.mu.Unlock()
		ok := srv.Call("ShardKV.DeleteShard", &args, &reply)
		kv.mu.Lock()
		if !ok || reply.Err == ErrWrongLeader {
			continue
		}
		if reply.Err == OK {
			DPrintf("success:server-%d-%d call DeleteShard(shard:%d, Num:%d) to server-%d-%d", kv.gid, kv.me, args.Shard, args.ShardNum, config.Shards[shard], i)
			return
		}
	}
	//}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

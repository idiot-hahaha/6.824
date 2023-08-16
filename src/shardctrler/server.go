package shardctrler

import (
	"6.824/raft"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	Query = "Query"
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32

	configs    []Config // indexed by config num
	groupsHead *ListNode
	callerCmd  map[int64]int
	applyIndex int
}

type ListNode struct {
	next *ListNode
	pre  *ListNode
	gid  int
}

type Op struct {
	// Your data here.
	Operation string
	ID        int64
	CmdIndex  int
	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Move
	GID   int
	Shard int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		Operation: Join,
		ID:        args.ID,
		CmdIndex:  args.CmdIndex,
		Servers:   args.Servers,
		GIDs:      []int{},
	}
	for k, _ := range op.Servers {
		op.GIDs = append(op.GIDs, k)
	}
	idx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	for !sc.Killed() && idx > sc.applyIndex {
		sc.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
		sc.mu.Lock()
	}
	if sc.callerCmd[args.ID] < args.CmdIndex {
		reply.Err = ErrUnknown
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		Operation: Leave,
		ID:        args.ID,
		CmdIndex:  args.CmdIndex,
		GIDs:      args.GIDs,
	}
	idx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	for !sc.Killed() && idx > sc.applyIndex {
		sc.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
		sc.mu.Lock()
	}
	if sc.callerCmd[args.ID] < args.CmdIndex {
		reply.Err = ErrUnknown
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		Operation: Move,
		ID:        args.ID,
		CmdIndex:  args.CmdIndex,
		GID:       args.GID,
		Shard:     args.Shard,
	}
	idx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	for !sc.Killed() && idx > sc.applyIndex {
		sc.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
		sc.mu.Lock()
	}
	if sc.callerCmd[args.ID] < args.CmdIndex {
		reply.Err = ErrUnknown
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		Operation: Query,
		ID:        args.ID,
		CmdIndex:  args.CmdIndex,
	}
	idx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	for !sc.Killed() && sc.applyIndex < idx {
		sc.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
		sc.mu.Lock()
	}
	if sc.callerCmd[args.ID] < args.CmdIndex {
		reply.Err = ErrUnknown
		return
	}
	reply.Err = OK
	if args.Num == -1 {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) Killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.callerCmd = make(map[int64]int)
	sc.configs = []Config{{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}}
	sc.applyIndex = 0
	sc.groupsHead = nil
	sc.groupsHead = &ListNode{
		next: nil,
		pre:  nil,
		gid:  0,
	}
	sc.groupsHead.pre = sc.groupsHead
	sc.groupsHead.next = sc.groupsHead
	sc.rf.Name = fmt.Sprintf("shardCtrler-%d", me)
	go sc.applier()
	DPrintf("shardCtrler-%d start", sc.me)
	return sc
}

func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		if sc.Killed() {
			return
		}
		if m.SnapshotValid {
			sc.installSnapshot(&m.Snapshot)
		} else if m.CommandValid {
			op, ok := m.Command.(Op)
			if !ok {
				panic("err type")
			}
			sc.mu.Lock()
			switch op.Operation {
			case Query:
				sc.queryHandler(&op)
			case Join:
				sc.joinHandler(&op)
			case Leave:
				sc.leaveHandler(&op)
			case Move:
				sc.moveHandler(&op)
			default:
				panic("undefined operation")
			}
			sc.applyIndex = m.CommandIndex
			DPrintf("server-%d apply %d op:%+v", sc.me, sc.applyIndex, op)
			//if (sc.applyIndex+1)%10 == 0 {
			//	sc.snapshot(sc.applyIndex)
			//}
			sc.mu.Unlock()
		} else {
			panic("err cmd")
		}
	}
}
func (sc *ShardCtrler) queryHandler(op *Op) {
	if sc.callerCmd[op.ID] >= op.CmdIndex {
		return
	}
	sc.callerCmd[op.ID] = op.CmdIndex
}
func (sc *ShardCtrler) joinHandler(op *Op) {
	if sc.callerCmd[op.ID] >= op.CmdIndex {
		return
	}
	sc.callerCmd[op.ID] = op.CmdIndex
	sc.configs = append(sc.configs, Config{
		Num:    sc.configs[len(sc.configs)-1].Num + 1,
		Shards: [10]int{},
		Groups: make(map[int][]string, len(sc.configs[len(sc.configs)-1].Groups)),
	})
	newConfig := &sc.configs[len(sc.configs)-1]
	for k, v := range sc.configs[len(sc.configs)-2].Groups {
		newConfig.Groups[k] = v
	}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = sc.configs[len(sc.configs)-2].Shards[i]
	}
	for _, gid := range op.GIDs {
		if servers, ok := newConfig.Groups[gid]; ok {
			servers = append(servers, op.Servers[gid]...)
			continue
		}
		newConfig.Groups[gid] = op.Servers[gid]
		count := NShards / len(newConfig.Groups)
		if count == 10 {
			for i := 0; i < NShards; i++ {
				newConfig.Shards[i] = gid
			}
			sc.groupsHead.gid = gid
			continue
		}
		for i := 0; i < count; i++ {
			oldGid := sc.groupsHead.gid
			for j := 0; j < NShards; j++ {
				if newConfig.Shards[j] == oldGid {
					newConfig.Shards[j] = gid
					break
				}
			}
			sc.groupsHead = sc.groupsHead.next
		}
		tail := &ListNode{
			next: sc.groupsHead,
			pre:  sc.groupsHead.pre,
			gid:  gid,
		}
		sc.groupsHead.pre.next = tail
		sc.groupsHead.pre = tail
	}
	//sc.configs[len(sc.configs)-1].Shards = newConfig.Shards
}
func (sc *ShardCtrler) leaveHandler(op *Op) {
	if sc.callerCmd[op.ID] >= op.CmdIndex {
		return
	}
	sc.callerCmd[op.ID] = op.CmdIndex
	sc.configs = append(sc.configs, Config{
		Num:    sc.configs[len(sc.configs)-1].Num + 1,
		Shards: [10]int{},
		Groups: make(map[int][]string, len(sc.configs[len(sc.configs)-1].Groups)),
	})
	newConfig := &sc.configs[len(sc.configs)-1]
	for k, v := range sc.configs[len(sc.configs)-2].Groups {
		newConfig.Groups[k] = v
	}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = sc.configs[len(sc.configs)-2].Shards[i]
	}
	deleteGroups := make(map[int]struct{})
	for _, gid := range op.GIDs {
		deleteGroups[gid] = struct{}{}
		delete(newConfig.Groups, gid)
	}
	for i, _ := range newConfig.Shards {
		if _, ok := deleteGroups[newConfig.Shards[i]]; ok {
			newConfig.Shards[i] = 0
		}
	}
	ptr := sc.groupsHead.next
	for ptr != sc.groupsHead {
		if _, ok := deleteGroups[ptr.gid]; ok {
			ptr.next.pre = ptr.pre
			ptr.pre.next = ptr.next
		}
		ptr = ptr.next
	}
	if _, ok := deleteGroups[ptr.gid]; ok {
		if ptr.next == ptr {
			ptr.gid = 0
		} else {
			ptr.next.pre = ptr.pre
			ptr.pre.next = ptr.next
			sc.groupsHead = sc.groupsHead.next
		}
	}
	for i := 0; i < NShards; i++ {
		if newConfig.Shards[i] == 0 {
			sc.groupsHead = sc.groupsHead.pre
			newConfig.Shards[i] = sc.groupsHead.gid
		}
	}
	sc.configs[len(sc.configs)-1].Shards = newConfig.Shards
}
func (sc *ShardCtrler) moveHandler(op *Op) {
	if sc.callerCmd[op.ID] >= op.CmdIndex {
		return
	}
	sc.callerCmd[op.ID] = op.CmdIndex
	sc.configs = append(sc.configs, Config{
		Num:    sc.configs[len(sc.configs)-1].Num + 1,
		Shards: [10]int{},
		Groups: make(map[int][]string, len(sc.configs[len(sc.configs)-1].Groups)),
	})
	newConfig := &sc.configs[len(sc.configs)-1]
	for k, v := range sc.configs[len(sc.configs)-2].Groups {
		newConfig.Groups[k] = v
	}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = sc.configs[len(sc.configs)-2].Shards[i]
	}
	newConfig.Shards[op.Shard] = op.GID
	return
	if newConfig.Shards[op.Shard] == op.GID {
		return
	}
	gid := newConfig.Shards[op.Shard]
	newConfig.Shards[op.Shard] = op.GID
	for i := 0; i < NShards; i++ {
		if newConfig.Shards[i] == op.GID {
			newConfig.Shards[i] = gid
			break
		}
	}
}
func (sc *ShardCtrler) installSnapshot(snapshot *[]byte) {
	var applyIndex int
	var configs []Config
	var callerCmd map[int64]int
	var listSlice []int
	r := bytes.NewBuffer(*snapshot)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&applyIndex); err != nil {
		panic(err)
	}
	if err := d.Decode(&configs); err != nil {
		panic(err)
	}
	if err := d.Decode(&callerCmd); err != nil {
		panic(err)
	}
	if err := d.Decode(&listSlice); err != nil {
		panic(err)
	}
	sc.mu.Lock()
	sc.applyIndex = applyIndex
	sc.configs = configs
	sc.callerCmd = callerCmd
	sc.groupsHead = &ListNode{
		next: nil,
		pre:  nil,
		gid:  listSlice[0],
	}
	tail := sc.groupsHead
	for i := 1; i < len(listSlice); i++ {
		pre := tail
		tail = &ListNode{
			next: nil,
			pre:  pre,
			gid:  listSlice[i],
		}
		pre.next = tail
	}
	tail.next = sc.groupsHead
	sc.groupsHead.pre = tail
	sc.mu.Unlock()
}
func (sc *ShardCtrler) snapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	listSlice := []int{sc.groupsHead.gid}
	for ptr := sc.groupsHead.next; ptr != sc.groupsHead; ptr = ptr.next {
		listSlice = append(listSlice, ptr.gid)
	}
	if err := e.Encode(sc.applyIndex); err != nil {
		panic(err)
	}
	if err := e.Encode(sc.configs); err != nil {
		panic(err)
	}
	if err := e.Encode(sc.callerCmd); err != nil {
		panic(err)
	}
	if err := e.Encode(listSlice); err != nil {
		panic(err)
	}
	sc.rf.Snapshot(index, w.Bytes())
}

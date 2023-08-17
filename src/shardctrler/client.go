package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id       int64
	cmdIndex int
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = nrand()
	ck.cmdIndex = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.cmdIndex++
	args.CmdIndex = ck.cmdIndex
	ck.mu.Unlock()
	args.ID = ck.id
	args.Num = num
	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply
			DPrintf("client try call Query to server-%d,args:%+v", i, args)
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			DPrintf("client call Query to server-%d,args:%+v, reply:%+v", i, args, reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.cmdIndex++
	args.CmdIndex = ck.cmdIndex
	ck.mu.Unlock()
	args.ID = ck.id
	args.Servers = servers

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			DPrintf("client call Join to server-%d,args:%+v, reply:%+v", i, args, reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.cmdIndex++
	args.CmdIndex = ck.cmdIndex
	ck.mu.Unlock()
	args.ID = ck.id
	args.GIDs = gids

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			DPrintf("client call Leave to server-%d,args:%+v, reply:%+v", i, args, reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.cmdIndex++
	args.CmdIndex = ck.cmdIndex
	ck.mu.Unlock()
	args.ID = ck.id
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			DPrintf("client call Move to server-%d,args:%+v, reply:%+v", i, args, reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

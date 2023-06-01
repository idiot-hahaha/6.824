package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIndex   int
	lastIndex     int
	ID            int64
	lastCommandID int64
	lastCallCount int
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
	// You'll have to add code here.
	ck.leaderIndex = -1
	ck.ID = nrand()
	ck.lastCommandID = 0
	ck.lastCallCount = 0
	DPrintf("MakeClerk(%d)", ck.ID)
	return ck
}

func (ck *Clerk) PingLeader() {
	ck.checkLeader()
	args := &TestArgs{}
	reply := &TestReply{}
	ok := ck.servers[ck.leaderIndex].Call("KVServer.PingTest", args, reply)
	for !ok {
		DPrintf("time out, try again")
		ok = ck.servers[ck.leaderIndex].Call("KVServer.PingTest", args, reply)
	}
	DPrintf("ping success")
}

func (ck *Clerk) checkLeader() {
	for ck.leaderIndex == -1 {
		DPrintf("client(%d) check leader", ck.ID)
		time.Sleep(time.Millisecond * 10)

		// need to be changed to concurrent
		for i := range ck.servers {
			args := &IsLeaderArgs{}
			reply := &IsLeaderReply{
				IsLeader: false,
			}
			if ok := ck.servers[i].Call("KVServer.IsLeader", args, reply); !ok {
				DPrintf("call IsLeader to server(%d) fail", i)
				continue
			}
			if reply.IsLeader == true {
				ck.leaderIndex = i
				DPrintf("leader Index:%d", i)
				break
			}
		}
	}
	return
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := &GetArgs{
		Key:           key,
		ID:            nrand(),
		LastCommandID: ck.lastCommandID,
		LastCallCount: ck.lastCallCount,
	}
	ck.lastCommandID = args.ID
	ck.lastCallCount = 1
	reply := &GetReply{
		Err:   "",
		Value: "",
	}
	DPrintf("client(%d) send getMsg to server(%d), msg:%+v", ck.ID, ck.leaderIndex, args)
	for {
		DPrintf("client(%d):Get(%s)", ck.ID, key)
		if ck.leaderIndex == -1 {
			ck.checkLeader()
		}
		ok := ck.servers[ck.leaderIndex].Call("KVServer.Get", args, reply)
		for !ok {
			ck.lastCallCount++
			DPrintf("call Get fail, try again")
			if ck.lastCallCount%10 == 0 {
				reply.Err = ErrWrongLeader
				break
			}
			ok = ck.servers[ck.leaderIndex].Call("KVServer.Get", args, reply)
		}
		if reply.Err == OK {
			DPrintf("call Get success")
		} else if reply.Err == ErrWrongLeader {
			ck.lastCallCount--
			DPrintf("ErrWrongLeader")
			ck.leaderIndex = -1
			continue
		} else {
			//panic(reply.Err)
			return ""
		}
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:           key,
		Value:         value,
		Op:            op,
		ID:            nrand(),
		LastCommandID: ck.lastCommandID,
		LastCallCount: ck.lastCallCount,
	}
	ck.lastCommandID = args.ID
	ck.lastCallCount = 1
	reply := &PutAppendReply{
		Err: "",
	}
	for {
		if ck.leaderIndex == -1 {
			ck.checkLeader()
		}
		DPrintf("client(%d) send PutAppendMsg to server(%d), msg:%+v", ck.ID, ck.leaderIndex, args)
		ok := ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", args, reply)
		for !ok {
			ck.lastCallCount++
			if ck.lastCallCount%10 == 0 {
				reply.Err = ErrWrongLeader
				break
			}
			DPrintf("PutAppend fail, try again")
			ok = ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", args, reply)
		}
		if reply.Err == OK {
			DPrintf("PutAppend(%d) success", args.ID)
		} else if reply.Err == ErrWrongLeader {
			ck.lastCallCount--
			DPrintf("client(%d) get reply:ErrWrongLeader", ck.ID)
			ck.leaderIndex = -1
			continue
		} else {
			panic(reply.Err)
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("client(%d):Put(%s, %s)", ck.ID, key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("client(%d):Append(%s, %s)", ck.ID, key, value)
	ck.PutAppend(key, value, "Append")
}

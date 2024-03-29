package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	//mu       sync.Mutex
	clerkID  int64
	cmdIndex int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.

	ck.clerkID = nrand()
	ck.cmdIndex = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	//ck.mu.Lock()
	ck.cmdIndex++
	args := GetArgs{}
	args.Key = key
	args.ClerkID = ck.clerkID
	args.CmdIndex = ck.cmdIndex
	for {
		args.LastNum = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			//ck.mu.Unlock()
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				//DPrintf("client call Get args:%+v", args)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok {
					DPrintf("CALL %s Get success args:%+v, reply:%+v", servers[si], args, reply)
				} else {
					DPrintf("CALL %s Get fail args:%+v", servers[si], args)
				}
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
				if ok && reply.Err == ErrWrongLeader {
					continue
				}
				if ok {
					panic("undefined err")
				}
				if !ok {
					continue
				}
			}
		} else {
			//ck.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		//ck.mu.Lock()
		ck.config = ck.sm.Query(-1)
		DPrintf("ask controler for the latest configuration. new config:%+v", ck.config)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//ck.mu.Lock()
	ck.cmdIndex++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClerkID:  ck.clerkID,
		CmdIndex: ck.cmdIndex,
	}
	for {
		args.LastNum = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			//ck.mu.Unlock()
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				//DPrintf("client call PutAppend args:%+v", args)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok {
					DPrintf("CALL %s PutAppend success args:%+v, reply:%+v", servers[si], args, reply)
				} else {
					DPrintf("CALL %s PutAppend fail args:%+v", servers[si], args)
				}
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				if ok && reply.Err == ErrWrongLeader {
					continue
				}
				if !ok {
					continue
				}
			}
		} else {
			//ck.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		//ck.mu.Lock()
		ck.config = ck.sm.Query(-1)
		DPrintf("ask controler for the latest configuration. new config:%+v", ck.config)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

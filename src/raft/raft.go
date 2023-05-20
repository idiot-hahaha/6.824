package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state       int
	applyCond   *sync.Cond
	leaderAlive bool

	currentTerm int
	voteFor     int
	log         Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg

	//2D
	installSnapshot bool
}

const (
	follower = iota
	candidate
	leader
)
const (
	heartBeatIntervalTime = time.Millisecond * 100
)

func (rf *Raft) getLastLogIndexL() int {
	return rf.log.getLastLogIndex()
}

func (rf *Raft) getLastLogTermL() int {
	return rf.log.getLastLogTerm()
}

func (rf *Raft) getLogTermByIndexL(index int) int {
	return rf.log.getLogTermByIndex(index)
}

func (rf *Raft) getCommandByIndexL(index int) interface{} {
	return rf.log.getCommandByIndex(index)
}

func (rf *Raft) getLogIndexStart() int {
	return rf.log.StartIndex
}

func (rf *Raft) sliceLogL(start, end int) Log {
	return rf.log.sliceLog(start, end)
}

func (rf *Raft) sliceEntriesL(start, end int) []Entry {
	return rf.log.sliceEntries(start, end)
}

func (rf *Raft) sliceEntriesHeadL(end int) []Entry {
	return rf.log.sliceEntriesHead(end)
}

func (rf *Raft) sliceEntriesTailL(start int) []Entry {
	return rf.log.sliceEntriesTail(start)
}

func (rf *Raft) appendLogL(entries []Entry) Log {
	return rf.log.appendLog(entries)
}

func (rf *Raft) sliceLogHeadL(end int) Log {
	return rf.log.sliceLogHead(end)
}

func (rf *Raft) appendLog(args *AppendEntriesArgs) {
	for i := 0; i < len(args.Entries); i++ {
		if i+args.PrevLogIndex+1 > rf.getLastLogIndexL() || rf.getLogTermByIndexL(i+args.PrevLogIndex+1) != args.Entries[i].Term {
			rf.log = rf.sliceLogHeadL(i + args.PrevLogIndex + 1)
			rf.log = rf.appendLogL(args.Entries[i:])
			rf.persist()
			break
		}
	}
	if args.LeaderCommit > rf.lastApplied {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndexL())
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) applyEntry() {
	rf.mu.Lock()
	if rf.installSnapshot {
		rf.applySnapshotL()
	}
	for rf.killed() == false {
		rf.applyCond.Wait()
		for rf.lastApplied < rf.commitIndex || rf.lastApplied < rf.getLogIndexStart() {
			if rf.installSnapshot {
				rf.applySnapshotL()
				continue
			}
			if rf.lastApplied < rf.getLogIndexStart() {
				rf.installSnapshot = true
				continue
			}
			DPrintf("server(%d) apply entry, lastApplied:%d log:%+v", rf.me, rf.lastApplied, rf.log)
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.getCommandByIndexL(rf.lastApplied + 1),
				CommandIndex:  rf.lastApplied + 1,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied = msg.CommandIndex
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) applySnapshotL() {
	DPrintf("server(%d) apply snapshot, lastApplied:%d log:%+v", rf.me, rf.lastApplied, rf.log)
	msg := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotTerm:  rf.getLogTermByIndexL(rf.getLogIndexStart()),
		SnapshotIndex: rf.getLogIndexStart(),
	}
	rf.installSnapshot = false
	rf.mu.Unlock()
	rf.applyCh <- msg
	rf.mu.Lock()
	rf.lastApplied = msg.SnapshotIndex
	rf.commitIndex = msg.SnapshotIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.voteFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var voteFor int
	var currentTerm int
	var log Log

	if d.Decode(&voteFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil {
		DPrintf("Decode err")
	} else {
		rf.voteFor = voteFor
		rf.currentTerm = currentTerm
		rf.log = log
		rf.lastApplied = rf.getLogIndexStart()
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = rf.log.sliceLogTail(index + 1)
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
	DPrintf("server(%d) snapshot index:%d, new log:%+v", rf.me, index, rf.log)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server(%d) get RequestVoteArgs(%+v) from candidate(%d)", rf.me, *args, args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.changeStateL(follower)
		rf.currentTerm = args.Term
		DPrintf("server(%d) become follower", rf.me)
		rf.persist()
	}
	reply.Term = rf.currentTerm
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && !rf.moreNewLog(args) {
		DPrintf("args(%+v) is more new than term:%d index:%d", args, rf.getLastLogTermL(), rf.getLastLogIndexL())
		reply.VoteGranted = true
		rf.leaderAlive = true
		rf.voteFor = args.CandidateId
		rf.persist()
		return
	} else {
		reply.VoteGranted = false
		return
	}
}

func (rf *Raft) moreNewLog(args *RequestVoteArgs) bool {
	lastLogTerm := rf.getLastLogTermL()
	if lastLogTerm > args.LastLogTerm {
		return true
	}
	if lastLogTerm < args.LastLogTerm {
		return false
	}
	return rf.getLastLogIndexL() > args.LastLogIndex
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	ConflictTerm     int
	ConflictMinIndex int
	ConflictMaxIndex int
	Term             int
	Success          bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server(%d) get AppendEntriesMsg, args:%+v, server.lastIndex: %d, lastTerm:%d", rf.me, args, rf.getLastLogIndexL(), rf.getLastLogTermL())
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.leaderAlive = true
	if rf.state == candidate {
		rf.changeStateL(follower)
		rf.currentTerm = args.Term
		rf.persist()
		DPrintf("candidate(%d) become follower of term%d because get heartBeat from leader(%d)", rf.me, rf.currentTerm, args.LeaderId)
	}
	if args.Term > rf.currentTerm {
		rf.changeStateL(follower)
		rf.currentTerm = args.Term
		rf.persist()
		DPrintf("server(%d) become follower of term%d because term of appendArgs(%+v) is more new", rf.me, rf.currentTerm, args)
	}

	//
	if args.PrevLogIndex > rf.getLastLogIndexL() {
		reply.Success = false
		reply.ConflictTerm = rf.getLastLogTermL()
		conflictIndex := rf.getLastLogIndexL()
		for conflictIndex > rf.getLogIndexStart() && rf.getLogTermByIndexL(conflictIndex) == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictMinIndex = conflictIndex + 1
		reply.ConflictMaxIndex = rf.getLastLogIndexL()
		return
	}

	// 2D
	if args.PrevLogIndex+len(args.Entries) <= rf.getLogIndexStart() {
		reply.Success = true
		return
	}

	if args.PrevLogIndex < rf.getLogIndexStart() {
		PrevLogIndex := rf.getLogIndexStart()
		PrevLogTerm := args.Entries[PrevLogIndex-args.PrevLogIndex-1].Term
		args.Entries = args.Entries[PrevLogIndex-args.PrevLogIndex:]
		args.PrevLogIndex = PrevLogIndex
		args.PrevLogTerm = PrevLogTerm
	}
	if rf.getLogTermByIndexL(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.getLogTermByIndexL(args.PrevLogIndex)
		rf.log = rf.sliceLogHeadL(args.PrevLogIndex)
		rf.persist()
		conflictIndex := args.PrevLogIndex - 1
		for conflictIndex > rf.getLogIndexStart() && rf.getLogTermByIndexL(conflictIndex) == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictMinIndex = conflictIndex + 1
		reply.ConflictMaxIndex = args.PrevLogIndex - 1
		return
	}
	reply.Success = true
	// update logs
	rf.appendLog(args)
	DPrintf("server(%d) new log:%+v", rf.me, rf.log)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server(%d) get InstallSnapshotRPC from leader(%d)", rf.me, args.LeaderId)
	DPrintf("args:%+v", args)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.leaderAlive = true
	if args.Term > rf.currentTerm {
		rf.changeStateL(follower)
		rf.currentTerm = args.Term
	}
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
	if rf.getLogIndexStart() < args.LastIncludedIndex && args.LastIncludedIndex < rf.getLastLogIndexL() {
		rf.log = rf.log.sliceLogTail(args.LastIncludedIndex + 1)
	} else {
		rf.log = makeEmptyLog()
		rf.log.StartIndex = args.LastIncludedIndex
	}
	rf.log.Entries[0].Index = args.LastIncludedIndex
	rf.log.Entries[0].Term = args.LastIncludedTerm
	rf.persist()
	DPrintf("install snapshot ,new log:%+v", rf.log)
	rf.installSnapshot = true
	rf.applyCond.Broadcast()
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return index, term, isLeader
	}
	isLeader = true
	entries := []Entry{
		{
			Command: command,
			Index:   rf.getLastLogIndexL() + 1,
			Term:    rf.currentTerm,
		},
	}
	rf.log = rf.appendLogL(entries)
	rf.persist()
	index = rf.getLastLogIndexL()
	term = rf.currentTerm
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	DPrintf("=====Start!=====  cmd:%v(index:%d)", command, index)
	rf.updateLogOfOthersL()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	time.Sleep(time.Duration(120+rand.Intn(50)) * time.Millisecond)
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state == leader {
			rf.leaderAlive = true
		}
		if rf.leaderAlive == false {
			rf.changeStateL(candidate)
			DPrintf("server(%d) become candidate of term%d because of time out!", rf.me, rf.currentTerm)
		}
		rf.leaderAlive = false
		rf.mu.Unlock()
		time.Sleep(time.Duration(rand.Intn(150)+200) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = -1
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.leaderAlive = false
	rf.state = follower
	rf.log = makeEmptyLog()
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.installSnapshot = rf.persister.ReadSnapshot() != nil && len(rf.persister.ReadSnapshot()) != 0
	DPrintf("server(%d) start! log:%+v", me, rf.log)
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyEntry()

	go func() {
		for !rf.killed() {
			time.Sleep(50 * time.Millisecond)
			DPrintf("server(%d) is alive", me)
		}
	}()

	go func() {
		for !rf.killed() {
			time.Sleep(50 * time.Millisecond)
			rf.mu.Lock()
			DPrintf("server(%d) is not dead lock", me)
			rf.mu.Unlock()
		}
	}()

	return rf
}

func (rf *Raft) changeStateL(target int) {
	if target == follower {
		rf.state = follower
		rf.voteFor = -1
		rf.persist()
		rf.nextIndex = []int{}
		rf.matchIndex = []int{}
		return
	}
	if target == candidate {
		if rf.state == leader {
			panic("change state from leader to candidate!")
			return
		}
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.persist()
		rf.state = candidate
		voteCount := 1
		// send RequestVoteMsg to all others
		//DPrintf("candidate send RequestVoteMsg to all others")
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.getLastLogIndexL(),
			LastLogTerm:  rf.getLastLogTermL(),
		}
		majorCount := len(rf.peers)/2 + 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				reply := &RequestVoteReply{
					Term:        0,
					VoteGranted: false,
				}
				//DPrintf("candidate(%d) send RequestVoteMsg to server(%d)", args.CandidateId, server)
				if ok := rf.sendRequestVote(server, args, reply); !ok {
					//DPrintf("candidate(%d) send RequestVoteMsg to server(%d) failed!", args.CandidateId, server)
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("candidate(%d) get reply(%+v) from server(%d)", rf.me, reply, server)
				if rf.state != candidate || reply.Term != rf.currentTerm {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.changeStateL(follower)
					rf.currentTerm = reply.Term
					rf.persist()
					return
				}
				if reply.VoteGranted {
					voteCount++
					DPrintf("candidate(%d) get vote from server(%d)", rf.me, server)
					if voteCount == majorCount {
						rf.changeStateL(leader)
						DPrintf("candidate(%d) become leader of term%d", rf.me, rf.currentTerm)
					}
				}
			}(i)
		}
		return
	}
	if target == leader {
		if rf.state == follower {
			panic("change state from follower to leader!")
			return
		}
		if rf.state == leader {
			panic("change state from leader to leader!")
			return
		}
		rf.state = leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = rf.getLastLogIndexL()
		lastLogIndex := rf.getLastLogIndexL()
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.updateLogOfOthersL()
		go rf.heartBeat()
		return
	}
}

func (rf *Raft) updateLogOfOthersL() {
	DPrintf("lastIndex:%d, index0:%d, nextIndex:%v, matchIndex:%v", rf.getLastLogIndexL(), rf.getLogIndexStart(), rf.nextIndex, rf.matchIndex)
	lastIndex := rf.getLastLogIndexL()
	for i, _ := range rf.peers {
		if i != rf.me && rf.nextIndex[i] <= lastIndex {
			if rf.nextIndex[i] <= rf.getLogIndexStart() {
				DPrintf("leader(%d) update snapshot of server(%d)", rf.me, i)
				go rf.updateInstallSnapshot(i)
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.getLogTermByIndexL(rf.nextIndex[i] - 1),
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			args.Entries = make([]Entry, len(rf.sliceEntriesTailL(rf.nextIndex[i])))
			copy(args.Entries, rf.sliceEntriesTailL(rf.nextIndex[i]))
			reply := &AppendEntriesReply{
				ConflictTerm:     0,
				ConflictMinIndex: 0,
				ConflictMaxIndex: 0,
				Term:             0,
				Success:          false,
			}
			go func(server int) {
				DPrintf("leader(%d) update log of server(%d)", args.LeaderId, server)
				if ok := rf.sendAppendEntries(server, args, reply); !ok {
					return
				}
				rf.mu.Lock()
				DPrintf("leader(%d) get appendEntriesReply:%+v from server(%d)", rf.me, reply, server)
				if rf.killed() || rf.state != leader || args.Term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				if reply.Success == false && reply.Term > rf.currentTerm {
					rf.changeStateL(follower)
					rf.currentTerm = reply.Term
					rf.persist()
					DPrintf("leader(%d) become follower of term%d because AppendEntriesReply(%+v) of server(%d)", rf.me, rf.currentTerm, reply, server)
					rf.mu.Unlock()
					return
				}
				if reply.Success == false {
					//rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
					rf.catchUpQuicklyL(server, args, reply)
					rf.mu.Unlock()
					//rf.catchUpQuickly(server, args, reply)
					return
				}
				rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				DPrintf("matchIndex[%d]:%d nextIndex[%d]:%d", server, rf.matchIndex[server], server, rf.nextIndex[server])
				rf.leaderCommitL()
				rf.mu.Unlock()
				return
				//}
			}(i)
		}
	}
}

func (rf *Raft) heartBeatOnceL() {
	DPrintf("lastIndex:%d, index0:%d, nextIndex:%v, matchIndex:%v", rf.getLastLogIndexL(), rf.getLogIndexStart(), rf.nextIndex, rf.matchIndex)
	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.nextIndex[i] <= rf.getLogIndexStart() {
				DPrintf("leader(%d) update snapshot of server(%d), nextIndex:%d, IndexStart:%d", rf.me, i, rf.nextIndex[i], rf.getLogIndexStart())
				go rf.updateInstallSnapshot(i)
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.getLogTermByIndexL(rf.nextIndex[i] - 1),
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			args.Entries = make([]Entry, len(rf.sliceEntriesTailL(rf.nextIndex[i])))
			copy(args.Entries, rf.sliceEntriesTailL(rf.nextIndex[i]))
			reply := &AppendEntriesReply{
				ConflictTerm:     0,
				ConflictMinIndex: 0,
				ConflictMaxIndex: 0,
				Term:             0,
				Success:          false,
			}
			go func(server int) {
				DPrintf("leader(%d) update log of server(%d)", args.LeaderId, server)
				if ok := rf.sendAppendEntries(server, args, reply); !ok {
					return
				}
				rf.mu.Lock()
				DPrintf("leader(%d) get appendEntriesReply:%+v from server(%d)", rf.me, reply, server)
				if rf.killed() || rf.state != leader || args.Term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				if reply.Success == false && reply.Term > rf.currentTerm {
					rf.changeStateL(follower)
					rf.currentTerm = reply.Term
					rf.persist()
					DPrintf("leader(%d) become follower of term%d because AppendEntriesReply(%+v) of server(%d)", rf.me, rf.currentTerm, reply, server)
					rf.mu.Unlock()
					return
				}
				if reply.Success == false {
					//rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
					rf.catchUpQuicklyL(server, args, reply)
					rf.mu.Unlock()
					//rf.catchUpQuickly(server, args, reply)
					return
				}
				rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				DPrintf("matchIndex[%d]:%d nextIndex[%d]:%d", server, rf.matchIndex[server], server, rf.nextIndex[server])
				rf.leaderCommitL()
				rf.mu.Unlock()
				return
				//}
			}(i)
		}
	}
}

func (rf *Raft) heartBeat() {
	time.Sleep(heartBeatIntervalTime)
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != leader {
			DPrintf("server(%d) heartBeat stop, killed: %v, state: %d", rf.me, rf.killed(), rf.state)
			rf.mu.Unlock()
			return
		}
		DPrintf("leader(%d) beat heart", rf.me)
		rf.heartBeatOnceL()
		rf.mu.Unlock()
		time.Sleep(heartBeatIntervalTime)
	}
}

func (rf *Raft) leaderCommitL() {
	if rf.lastApplied < rf.getLogIndexStart() {
		rf.installSnapshot = true
		rf.applyCond.Broadcast()
		return
	}
	N := rf.lastApplied + 1
	for N <= rf.getLastLogIndexL() {
		count := 0
		for _, v := range rf.matchIndex {
			if v >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			if rf.getLogTermByIndexL(N) == rf.currentTerm {
				rf.commitIndex = N
				rf.applyCond.Broadcast()
			}
			N++
		} else {
			break
		}
	}
}

func (rf *Raft) updateInstallSnapshot(server int) {
	rf.mu.Lock()
	DPrintf("leader(%d) send snapshot to server(%d)", rf.me, server)
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.getLogIndexStart(),
		LastIncludedTerm:  rf.getLogTermByIndexL(rf.getLogIndexStart()),
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	reply := &InstallSnapshotReply{
		Term: 0,
	}
	rf.mu.Unlock()
	if ok := rf.sendInstallSnapshot(server, args, reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.changeStateL(follower)
		rf.currentTerm = reply.Term
		return
	}
	if rf.killed() || rf.state != leader || args.Term != rf.currentTerm {
		return
	}

	// success
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1
}

func (rf *Raft) catchUpQuickly(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	for rf.state == leader {
		DPrintf("catchUpQuicklyL for server(%d)", server)
		//if reply.ConflictMaxIndex < rf.getLogIndexStart() {
		//	rf.mu.Unlock()
		//	rf.updateInstallSnapshot(server)
		//	return
		//}
		matchIndex := min(reply.ConflictMaxIndex, rf.getLastLogIndexL())
		for ; reply.ConflictMinIndex < matchIndex; matchIndex-- {
			if matchIndex < rf.getLogIndexStart() {
				rf.mu.Unlock()
				rf.updateInstallSnapshot(server)
				return
			}
			if rf.getLogTermByIndexL(matchIndex) == reply.ConflictTerm {
				break
			}
		}
		rf.nextIndex[server] = matchIndex + 1
		args = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.getLogTermByIndexL(rf.nextIndex[server] - 1),
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		args.Entries = make([]Entry, len(rf.sliceEntriesTailL(rf.nextIndex[server])))
		copy(args.Entries, rf.sliceEntriesTailL(rf.nextIndex[server]))
		reply = &AppendEntriesReply{
			ConflictTerm:     0,
			ConflictMinIndex: 0,
			ConflictMaxIndex: 0,
			Term:             0,
			Success:          false,
		}
		rf.mu.Unlock()
		if ok := rf.sendAppendEntries(server, args, reply); !ok {
			return
		}
		rf.mu.Lock()
		if rf.killed() || rf.state != leader || args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.changeStateL(follower)
			rf.currentTerm = reply.Term
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			DPrintf("matchIndex[%d]:%d nextIndex[%d]:%d", server, rf.matchIndex[server], server, rf.nextIndex[server])
			rf.leaderCommitL()
			rf.mu.Unlock()
			return
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) catchUpQuicklyL(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for rf.state == leader {
		DPrintf("catchUpQuicklyL for server(%d)", server)
		DPrintf("ConflictMaxIndex:%d, ConflictMinIndex:%d, getLogIndexStart:%d, getLastLogIndexL:%d", reply.ConflictMaxIndex, reply.ConflictMinIndex, rf.getLogIndexStart(), rf.getLastLogIndexL())
		matchIndex := min(reply.ConflictMaxIndex, rf.getLastLogIndexL())
		//if matchIndex < rf.getLogIndexStart() {
		//	go rf.updateInstallSnapshot(server)
		//	return
		//}
		for ; reply.ConflictMinIndex < matchIndex; matchIndex-- {
			if matchIndex < rf.getLogIndexStart() {
				go rf.updateInstallSnapshot(server)
				return
			}
			if rf.getLogTermByIndexL(matchIndex) == reply.ConflictTerm {
				break
			}
		}
		if matchIndex < rf.getLogIndexStart() {
			go rf.updateInstallSnapshot(server)
			return
		}
		rf.nextIndex[server] = matchIndex + 1
		args = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.getLogTermByIndexL(rf.nextIndex[server] - 1),
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		args.Entries = make([]Entry, len(rf.sliceEntriesTailL(rf.nextIndex[server])))
		copy(args.Entries, rf.sliceEntriesTailL(rf.nextIndex[server]))
		reply = &AppendEntriesReply{
			ConflictTerm:     0,
			ConflictMinIndex: 0,
			ConflictMaxIndex: 0,
			Term:             0,
			Success:          false,
		}
		rf.mu.Unlock()
		if ok := rf.sendAppendEntries(server, args, reply); !ok {
			rf.mu.Lock()
			return
		}
		rf.mu.Lock()
		if rf.killed() || rf.state != leader || args.Term != rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.changeStateL(follower)
			rf.currentTerm = reply.Term
			return
		}
		if reply.Success {
			rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			DPrintf("matchIndex[%d]:%d nextIndex[%d]:%d", server, rf.matchIndex[server], server, rf.nextIndex[server])
			rf.leaderCommitL()
			return
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

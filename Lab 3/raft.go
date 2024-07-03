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
	"6.5840/labgob"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	VotedForNil = -1
	Follower    = 0
	Candidate   = 1
	Leader      = 2
)

// DPrintf("[Start]:\t\t\t ID %d term %d State %s\t || \t ", rf.me, rf.currentTerm, state2name(rf.state))

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	ApplyCh   chan ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// some other varieties
	state    int
	leaderID int

	nonLeaderCond *sync.Cond
	leaderCond    *sync.Cond
	applyCond     *sync.Cond

	heartbeatPeriodTime int
	electionTimeoutTime int

	lastReceiveTime int64
	lastSendTime    int64

	electionTimeoutChan chan struct{}
	heartbeatPeriodChan chan struct{}

	snapshot []byte
}

func state2name(state int) string {
	var name string
	switch state {
	case 0:
		name = "Follower"
	case 1:
		name = "Candidate"
	case 2:
		name = "Leader"
	default:
		name = "UnknownState"
	}
	return name
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isLeader bool
	// Your code here (3A).

	term = rf.currentTerm
	isLeader = rf.state == Leader

	return term, isLeader
}

// getLastLogIndex return the last log index from log or snapshot;
// do not use mutex, protected by the caller;
//func (rf *Raft) getLastLogIndex() int {
//	lastLogIndex := len(rf.log) - 1
//	if lastLogIndex > 0 {
//		return rf.log[lastLogIndex].Index
//	}
//	return rf.snapshot.SnapshotIndex
//}

// getLogIndex return current entry index in log;
// return -1 if it is not existed;return -2 error;
// do not use mutex, protected by the caller;
func (rf *Raft) getLogIndex(index int) int {
	// DPrintf("[getLogIndex]:\t\t ID %d term %d State %s\t || \t get log index %d from log %v", rf.me, rf.currentTerm, state2name(rf.state), index, rf.log)
	if index == 0 {
		return 0
	}

	if rf.log[0].Index == index {
		return 0
	}

	if rf.log[0].Index > index {
		// index in snapshot
		return -1
	}

	// binary search, O(log n)
	l, r := 1, len(rf.log)-1

	for l <= r {
		m := (r-l)>>1 + l
		if rf.log[m].Index == index {
			return m
		} else if rf.log[m].Index > index {
			r = m - 1
		} else {
			l = m + 1
		}
	}
	// error
	return -2
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	defer func() {
		DPrintf("[persist]:\t\t\t ID %d term %d State %s\t || \t "+
			"After persist : commitIndex %d, lastApplied %d, lastIncludeIndex %d,current log %v",
			rf.me, rf.currentTerm, state2name(rf.state), rf.commitIndex, rf.lastApplied, rf.log[0].Index, rf.log)
	}()
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)

	raftState := w.Bytes()

	rf.persister.Save(raftState, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	defer func() {
		DPrintf("[readPersist]:\t\t\t ID %d term %d State %s\t || \t "+
			"After readPersist : commitIndex %d, lastApplied %d,lastIncludeIndex %d,current log %v",
			rf.me, rf.currentTerm, state2name(rf.state), rf.commitIndex, rf.lastApplied, rf.log[0].Index, rf.log)
	}()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []Entry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatalf("decode persist error!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.commitIndex = rf.log[0].Index
		rf.lastApplied = rf.log[0].Index
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		DPrintf("[Snapshot]:\t\t\t ID %d term %d State %s\t || \t "+
			"After Snapshot : commitIndex %d, lastApplied %d,index %d,lastIncludeIndex %d,current log %v",
			rf.me, rf.currentTerm, state2name(rf.state), rf.commitIndex, rf.lastApplied, index, rf.log[0].Index, rf.log)
	}()

	if index <= rf.log[0].Index || index > rf.lastApplied {
		// already snapshot
		// or have not commited index
		return
	}

	cIndex := rf.getLogIndex(index)

	if cIndex < 0 {
		return
	}

	newLogLen := len(rf.log) - cIndex
	newLog := make([]Entry, newLogLen)

	for i := 0; i < newLogLen; i++ {
		newLog[i] = rf.log[cIndex+i]
	}

	rf.log = newLog
	rf.snapshot = snapshot

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)

	raftState := w.Bytes()

	rf.persister.Save(raftState, snapshot)
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderID         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm

		rf.mu.Unlock()

		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = VotedForNil

		rf.switchTo(Follower)

		rf.persist()
	}

	if rf.log[len(rf.log)-1].Index >= args.LastIncludeIndex {
		reply.Term = rf.currentTerm

		rf.mu.Unlock()

		return
	}

	index := rf.getLogIndex(args.LastIncludeIndex)
	for index > 0 && rf.log[index].Term != args.LastIncludeTerm {
		index--
	}

	if index >= 0 && rf.log[index].Index == args.LastIncludeIndex {
		rf.log = rf.log[index:]
	} else {
		rf.log = []Entry{{Index: args.LastIncludeIndex, Term: args.LastIncludeTerm}}
	}

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.log[0].Term,
		SnapshotIndex: rf.log[0].Index,
	}

	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludeIndex
	rf.lastApplied = args.LastIncludeIndex

	// fmt.Printf("*****apply msg %v install snapshot\n", msg)

	reply.Term = rf.currentTerm

	DPrintf("[InstallSnapshot]:\t\t ID %d term %d State %s\t || \t "+
		"After InstallSnapshot : commitIndex %d, lastApplied %d,lastIncludeIndex %d,current log %v",
		rf.me, rf.currentTerm, state2name(rf.state), rf.commitIndex, rf.lastApplied, rf.log[0].Index, rf.log)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)

	raftState := w.Bytes()

	rf.persister.Save(raftState, rf.snapshot)

	rf.mu.Unlock()

	rf.ApplyCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		DPrintf("[AppendEntries]:\t\t ID %d term %d State %s\t || \t "+
			"After AppendEntries : commitIndex %d, lastApplied %d,lastIncludeIndex %d,current log %v",
			rf.me, rf.currentTerm, state2name(rf.state), rf.commitIndex, rf.lastApplied, rf.log[0].Index, rf.log)
		if reply.Success == false {
			DPrintf("[AppendEntries]:\t\t ID %d term %d State %s\t || \t "+
				"After AppendEntries : reply %v",
				rf.me, rf.currentTerm, state2name(rf.state), reply)
		}
	}()

	if rf.currentTerm > args.Term {
		DPrintf("[AppendEntries]:\t\t ID %d term %d State %s\t || \t follower's term %d larger than leader's term %d,return",
			rf.me, rf.currentTerm, state2name(rf.state), rf.currentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm

		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = VotedForNil

		rf.switchTo(Follower)

		rf.persist()
	}

	if rf.log[0].Index > args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm

		reply.ConflictIndex = rf.log[len(rf.log)-1].Index
		reply.ConflictTerm = rf.log[len(rf.log)-1].Term

		return
	}

	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	lastIndex := rf.log[len(rf.log)-1].Index
	if lastIndex < args.PrevLogIndex {
		DPrintf("[AppendEntries]:\t\t ID %d term %d State %s\t || \t follower's log %v don't contain prev log index %d",
			rf.me, rf.currentTerm, state2name(rf.state), rf.log, args.PrevLogIndex)

		reply.Success = false
		reply.Term = rf.currentTerm
		// If a follower does not have prevLogIndex in its log,
		// it should return with conflictIndex = len(log) and conflictTerm = None.
		reply.ConflictTerm = 0
		reply.ConflictIndex = lastIndex + 1

		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	index := rf.getLogIndex(args.PrevLogIndex)

	if index == -2 {
		DPrintf("[AppendEntries]:\t\t ID %d term %d State %s\t || \t prevlogindex %d, index error,current log %v",
			rf.me, rf.currentTerm, state2name(rf.state), args.PrevLogIndex, rf.log)
		log.Fatalf("append entries error!\n")
	}

	if index != -1 && rf.log[index].Term != args.PrevLogTerm {
		DPrintf("[AppendEntries]:\t\t ID %d term %d State %s\t || \t same index %d prevlogindex %d but different term %d with leader's term %d",
			rf.me, rf.currentTerm, state2name(rf.state), index, args.PrevLogIndex, rf.log[index].Term, args.PrevLogTerm)
		// optimization
		reply.Success = false
		reply.Term = rf.currentTerm
		// If a follower does have prevLogIndex in its log,
		// but the term does not match, it should return conflictTerm = log[prevLogIndex].Term,
		// and then search its log for the first index whose entry has term equal to conflictTerm.
		reply.ConflictTerm = rf.log[index].Term
		reply.ConflictIndex = rf.log[index].Index

		for i := index - 1; i >= 0; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				break
			}
			reply.ConflictIndex--
		}

		rf.log = rf.log[:index]

		return
	}

	// redundant judgement？
	if index == 0 && rf.log[0].Term != args.PrevLogTerm {
		DPrintf("[AppendEntries]:\t\t ID %d term %d State %s\t || \t index==0, but rf.log[0].Term %d != args.PrevLogTerm %d",
			rf.me, rf.currentTerm, state2name(rf.state), rf.log[0].Term, args.PrevLogTerm)
		log.Fatalf("snapshot term not math, append entries term error!")
	}

	if index == -1 {
		// already snapshot
		idx := 0
		for i := range args.Entries {
			if args.PrevLogIndex > rf.log[0].Index {
				idx = i
				break
			}
			args.PrevLogIndex++
		}
		if idx == 0 {
			// fully snapshot
			DPrintf("[AppendEntries]:\t\t ID %d term %d State %s\t || \t fully snapshot, current log %v, args.Entries %v",
				rf.me, rf.currentTerm, state2name(rf.state), rf.log, args.Entries)
			reply.Success = true
			reply.Term = rf.currentTerm

			return
		}

		DPrintf("[AppendEntries]:\t\t ID %d term %d State %s\t || \t index==-1, idx %d, args.PrevLogIndex %d",
			rf.me, rf.currentTerm, state2name(rf.state), idx, args.PrevLogIndex)

		args.Entries = args.Entries[idx:]
		index = 0
	}

	end := len(rf.log) - 1

	isMatch := true
	nextIndex := index + 1

	for i := 0; isMatch && i < len(args.Entries); i++ {
		if end < nextIndex+i {
			isMatch = false
		} else if rf.log[nextIndex+i].Term != args.Entries[i].Term {
			isMatch = false
		}
	}

	if !isMatch {
		DPrintf("[AppendEntries]:\t\t ID %d term %d State %s\t || \t append from nextIndex %d,current log %v, args.Entries %v",
			rf.me, rf.currentTerm, state2name(rf.state), nextIndex, rf.log, args.Entries)

		rf.log = append(rf.log[:nextIndex], args.Entries...)
	}

	end = len(rf.log) - 1

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > rf.log[end].Index {
			rf.commitIndex = rf.log[end].Index
		}
		rf.applyCond.Broadcast()
	}

	rf.resetTimer()
	rf.switchTo(Follower)

	rf.persist()

	rf.leaderID = args.LeaderID

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		DPrintf("[RequestVote]:\t\t\t ID %d term %d State %s\t || \t "+
			"After RequestVote, vote result %v,voted for %d, commitIndex %d, lastApplied %d,lastIncludeIndex %d, current log %v",
			rf.me, rf.currentTerm, state2name(rf.state), reply.VoteGranted, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.log[0].Index, rf.log)
	}()

	// if server has bigger term, return immediately
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		return
	}

	// update server's term, change state to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = VotedForNil

		rf.switchTo(Follower)

		rf.persist()
	}

	// vote for one server in one term
	if rf.votedFor != VotedForNil && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		return
	}

	// log check
	// If the logs have last entries with different terms,
	// then the log with the later term is more up-to-date.
	lastLogIndex := rf.log[len(rf.log)-1].Index
	cIndex := rf.getLogIndex(lastLogIndex)
	if rf.log[cIndex].Term > args.LastLogTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		return
	}

	// If the logs end with the same term,
	// then whichever log is longer is more up-to-date.
	if rf.log[cIndex].Term == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		return
	}

	rf.resetTimer()
	rf.switchTo(Follower)

	rf.votedFor = args.CandidateID

	rf.persist()

	reply.VoteGranted = true
	reply.Term = rf.currentTerm
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	isLeader = rf.state == Leader

	if !isLeader {
		return index, term, isLeader
	}

	index = rf.log[len(rf.log)-1].Index + 1
	term = rf.currentTerm

	entry := Entry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, entry)

	DPrintf("[Start]:\t\t\t ID %d term %d State %s\t || \t start log consistency with index %d", rf.me, rf.currentTerm, state2name(rf.state), index)

	go rf.broadcastAppendEntries(index, term, rf.commitIndex)

	rf.persist()

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

func (rf *Raft) installSnapshot(server int) bool {
	rf.mu.Lock()

	args := &InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LeaderID:         rf.me,
		LastIncludeIndex: rf.log[0].Index,
		LastIncludeTerm:  rf.log[0].Term,
		Data:             rf.snapshot,
	}
	reply := &InstallSnapshotReply{}

	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(server, args, reply)

	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != args.Term {
		return true
	}

	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.votedFor = VotedForNil

		rf.switchTo(Follower)

		rf.persist()

		return true
	}

	rf.nextIndex[server] = rf.log[0].Index + 1
	rf.matchIndex[server] = rf.log[0].Index

	return true
}

func (rf *Raft) getNextIndex(reply *AppendEntriesReply, nextIndex int) int {
	// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
	// If it does not find an entry with that term, it should set nextIndex = conflictIndex.
	if reply.Term == 0 {
		return reply.ConflictIndex
	}

	cIndex := rf.getLogIndex(reply.ConflictIndex)

	if cIndex < 0 {
		return reply.ConflictIndex
	}

	if rf.log[cIndex].Term < reply.ConflictTerm {
		return reply.ConflictIndex
	}

	i := cIndex
	for i > 0 {
		if rf.log[i].Term == reply.ConflictTerm {
			break
		}
		i--
	}

	if i == 0 {
		if rf.log[0].Term == reply.ConflictTerm {
			return rf.log[0].Index + 1
		}
		return cIndex
	}

	// If it finds an entry in its log with that term,
	// it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
	for rf.log[i].Index < nextIndex {
		if rf.log[i].Term != reply.ConflictTerm {
			break
		}
		i++
	}

	return rf.log[i].Index
}

func (rf *Raft) broadcastAppendEntries(index int, term int, commitIndex int) {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()

		return
	}

	wg := &sync.WaitGroup{}

	rf.lastSendTime = time.Now().UnixNano()

	nReplica := 1
	require := len(rf.peers)>>1 + 1

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)

		go func(server int) {
			defer wg.Done()

		retry:

			rf.mu.Lock()

			if rf.currentTerm != term || rf.state != Leader {
				rf.mu.Unlock()

				return
			}

			// TODO: delete?
			if index < rf.commitIndex {
				rf.mu.Unlock()

				return
			}

			nextIndex := rf.nextIndex[server]
			prevLogIndex := nextIndex - 1
			cIndex := rf.getLogIndex(prevLogIndex)
			eIndex := rf.getLogIndex(index)

			if cIndex == -2 || eIndex == -2 {
				DPrintf("[broadcastAppendEntries]:\t ID %d term %d State %s\t || \t index error prevLogIndex %d,index %d,cIndex %d,eIndex %d,current log %v",
					rf.me, rf.currentTerm, state2name(rf.state), prevLogIndex, index, cIndex, eIndex, rf.log)

				log.Fatalf("broadcastAppendEntries index error!")
			}

			prevLogTerm := 0
			// make sure prev log index >= log[0].index
			if cIndex >= 0 {
				prevLogTerm = rf.log[cIndex].Term
			} else {
				// install snapshot
				DPrintf("[broadcastAppendEntries]:\t ID %d term %d State %s\t || \t "+
					"server %d need installsnapshot, commitIndex %d, lastApplied %d,lastIncludeIndex %d,prevLogIndex %d, current log %v",
					rf.me, rf.currentTerm, state2name(rf.state), server, rf.commitIndex, rf.lastApplied, rf.log[0].Index, prevLogIndex, rf.log)
				rf.mu.Unlock()

				ok := rf.installSnapshot(server)

				if !ok {
					goto retry
				}

				rf.mu.Lock()

				if rf.state != Leader {
					rf.mu.Unlock()

					return
				}

				index = rf.log[len(rf.log)-1].Index

				rf.mu.Unlock()

				goto retry
			}

			entries := make([]Entry, 0)

			if nextIndex < index+1 {
				if cIndex >= 0 {
					entries = rf.log[cIndex+1 : eIndex+1]
				} else {
					entries = rf.log[1:]
				}
			}

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			reply := &AppendEntriesReply{}

			DPrintf("[broadcastAppendEntries]:\t ID %d term %d State %s\t || \t send entries to server %d cindex %d,index %d,nextIndex %d，args %v",
				rf.me, rf.currentTerm, state2name(rf.state), server, cIndex, index, nextIndex, args)

			rf.mu.Unlock()

			ok := rf.sendAppendEntries(server, args, reply)

			if !ok {
				return
			}

			rf.mu.Lock()

			// consistency check
			if rf.currentTerm != term || rf.state != Leader {
				rf.mu.Unlock()

				return
			}

			if reply.Success == false {
				if rf.currentTerm < reply.Term {
					DPrintf("[broadcastAppendEntries]:\t ID %d term %d State %s\t || \t follower %d 's term %d bigger than leader",
						rf.me, rf.currentTerm, state2name(rf.state), server, reply.Term)

					rf.currentTerm = reply.Term
					rf.votedFor = VotedForNil

					rf.switchTo(Follower)

					rf.persist()

					rf.mu.Unlock()

					return
				} else {
					rf.nextIndex[server] = rf.getNextIndex(reply, nextIndex)

					DPrintf("[broadcastAppendEntries]:\t ID %d term %d State %s\t || \t prevLogIndex %d or prevLogTerm %d check false, retry, nextIndex %d",
						rf.me, rf.currentTerm, state2name(rf.state), args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[server])

					rf.mu.Unlock()

					goto retry
				}
			} else {
				nReplica++

				if rf.nextIndex[server] < index+1 {
					rf.nextIndex[server] = index + 1
					rf.matchIndex[server] = index
				}

				if rf.state == Leader && nReplica >= require {
					if index > rf.commitIndex && term == rf.currentTerm {
						rf.commitIndex = index

						rf.applyCond.Broadcast()

						go rf.broadcastHeartBeat()
					}
				}

				DPrintf("[broadcastAppendEntries]:\t ID %d term %d State %s\t || \t follower %d receive index %d,current nextIndex %d",
					rf.me, rf.currentTerm, state2name(rf.state), server, index, rf.nextIndex[server])

				rf.mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) broadcastHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	// for consistency check
	index := rf.log[len(rf.log)-1].Index
	term := rf.currentTerm
	commitIndex := rf.commitIndex

	go rf.broadcastAppendEntries(index, term, commitIndex)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.currentTerm++
	rf.votedFor = rf.me

	rf.resetTimer()
	rf.switchTo(Candidate)

	rf.persist()

	nVote := 1 // vote for self

	DPrintf("[startElection]:\t\t ID %d term %d State %s\t || \t "+
		"start election, commitIndex %d, lastApplied %d,lastIncludeIndex %d, current log %v",
		rf.me, rf.currentTerm, state2name(rf.state), rf.commitIndex, rf.lastApplied, rf.log[0].Index, rf.log)

	rf.mu.Unlock()

	go func(nVote *int, rf *Raft) {
		wg := &sync.WaitGroup{}

		rf.mu.Lock()

		term := rf.currentTerm
		lastLogIndex := rf.log[len(rf.log)-1].Index
		cIndex := rf.getLogIndex(lastLogIndex)
		lastLogTerm := rf.log[cIndex].Term
		require := len(rf.peers)>>1 + 1

		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			wg.Add(1)

			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}

			go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
				defer wg.Done()

				ok := rf.sendRequestVote(server, args, reply)

				// RPC failed
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// consistency check
				if rf.currentTerm != term {
					return
				}

				if reply.VoteGranted == false {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = VotedForNil

						rf.switchTo(Follower)

						rf.persist()
					}
				} else {
					*nVote++

					if rf.state == Candidate && *nVote >= require {
						rf.leaderID = rf.me

						rf.switchTo(Leader)

						for i := range rf.peers {
							rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
							rf.matchIndex[i] = 0
						}

						DPrintf("[startElection]:\t\t ID %d term %d State %s\t || \t "+
							"win election,next index %v,current log %v",
							rf.me, rf.currentTerm, state2name(rf.state), rf.nextIndex, rf.log)

						go rf.broadcastHeartBeat()

						rf.persist()
					}
				}
			}(i, args, reply)
		}
		wg.Wait()
	}(&nVote, rf)
}

func (rf *Raft) resetTimer() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	rf.electionTimeoutTime = 5*rf.heartbeatPeriodTime + (r.Int() % rf.heartbeatPeriodTime)

	rf.lastReceiveTime = time.Now().UnixNano()
}

func (rf *Raft) switchTo(newState int) {
	oldState := rf.state
	rf.state = newState

	if oldState == Leader && newState == Follower {
		rf.nonLeaderCond.Broadcast()
	}
	if oldState == Candidate && newState == Leader {
		rf.leaderCond.Broadcast()
	}
}

func (rf *Raft) electionTimeoutTicker() {
	for rf.killed() == false {
		if _, isLeader := rf.GetState(); isLeader {
			rf.mu.Lock()
			rf.nonLeaderCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			elapsedTime := time.Now().UnixNano() - rf.lastReceiveTime

			if int(elapsedTime/int64(time.Millisecond)) >= rf.electionTimeoutTime {
				rf.mu.Unlock()
				rf.electionTimeoutChan <- struct{}{}
			} else {
				rf.mu.Unlock()
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) heartbeatPeriodTicker() {
	for rf.killed() == false {
		if _, isLeader := rf.GetState(); isLeader == false {
			rf.mu.Lock()
			rf.leaderCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			elapsedTime := time.Now().UnixNano() - rf.lastSendTime

			if int(elapsedTime/int64(time.Millisecond)) >= rf.heartbeatPeriodTime {
				rf.mu.Unlock()
				rf.heartbeatPeriodChan <- struct{}{}
			} else {
				rf.mu.Unlock()
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) applyEntries() {
	for rf.killed() == false {
		rf.mu.Lock()

		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied

		rf.mu.Unlock()

		if commitIndex == lastApplied {
			rf.mu.Lock()
			rf.applyCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			DPrintf("[applyEntries]:\t\t ID %d term %d State %s\t || \t apply entries from %d to %d, current log %v",
				rf.me, rf.currentTerm, state2name(rf.state), lastApplied+1, commitIndex, rf.log)
			rf.mu.Unlock()
			for i := lastApplied + 1; i <= commitIndex; i++ {
				rf.mu.Lock()

				if rf.commitIndex == rf.lastApplied {
					rf.mu.Unlock()

					break
				}

				index := rf.getLogIndex(i)
				if index <= 0 {
					DPrintf("[applyEntries]:\t\t ID %d term %d State %s\t || \t "+
						"index error! currentIndex %d index %d,current log %v,current lastApplied %d,current commitIndex %d,current i %d",
						rf.me, rf.currentTerm, state2name(rf.state), i, index, rf.log, rf.lastApplied, rf.commitIndex, i)
					log.Fatalf("apply entries error!")
				}

				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[index].Command,
					CommandIndex: rf.log[index].Index,
				}
				rf.lastApplied++

				rf.mu.Unlock()

				rf.ApplyCh <- msg
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) eventLoop() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimeoutChan:
			go rf.startElection()

		case <-rf.heartbeatPeriodChan:
			go rf.broadcastHeartBeat()
		}
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
	rf.ApplyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = VotedForNil
	rf.log = make([]Entry, 1) // index start from 1

	rf.commitIndex = 0
	rf.lastApplied = 0

	size := len(rf.peers)
	rf.nextIndex = make([]int, size)
	rf.matchIndex = make([]int, size)

	rf.state = Follower
	rf.leaderID = -1

	rf.nonLeaderCond = sync.NewCond(&rf.mu)
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.heartbeatPeriodTime = 120 // 120ms
	rf.resetTimer()

	rf.electionTimeoutChan = make(chan struct{})
	rf.heartbeatPeriodChan = make(chan struct{})

	rf.snapshot = make([]byte, 0)
	// initialize from state persisted before a crash
	rf.mu.Lock()
	// fmt.Printf("server %d start\n", rf.me)
	rf.readPersist(rf.persister.ReadRaftState())
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.eventLoop()
	go rf.heartbeatPeriodTicker()
	go rf.electionTimeoutTicker()
	go rf.applyEntries()

	return rf
}

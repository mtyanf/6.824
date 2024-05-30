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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	FOLLOWER         = 1
	LEADER           = 2
	CANDIDATE        = 3
	NOTVOTE          = -1
	HEARTBEATTIMEOUT = 300
	ELECTIONTIMEOUT  = 50
)

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

type Log struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []Log
	identity    int8
	leaderID    int

	// volatile state on all servers.
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// some channels
	heartbeat chan struct{}
	vote      chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.identity == LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()

	if reply.Term > args.Term {
		reply.Success = false
		return
	}

	// heartbeat
	if len(args.Entries) == 0 {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.leaderID = args.LeaderID
		rf.identity = FOLLOWER
		rf.mu.Unlock()

		rf.heartbeat <- struct{}{}
		reply.Success = true
		return
	}
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
	reply.Term = rf.currentTerm
	rf.mu.Unlock()

	if reply.Term > args.Term {
		reply.VoteGranted = false
		return
	}

	rf.mu.Lock()
	if rf.currentTerm == args.Term && rf.votedFor != NOTVOTE && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	// TODO: log check

	reply.VoteGranted = true
	rf.currentTerm = args.Term
	rf.identity = FOLLOWER
	rf.votedFor = args.CandidateID
	rf.mu.Unlock()

	rf.vote <- struct{}{}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		if _, isLeader := rf.GetState(); isLeader {
			// send heartbeat
			rf.sendHeartbeat()

			//// pause for a random amount of time between 50 and 350
			//// milliseconds.
			ms := 100 + (rand.Int63() % 50)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := HEARTBEATTIMEOUT + (rand.Int63() % HEARTBEATTIMEOUT)
			electionTimer := time.After(time.Duration(ms) * time.Millisecond)

			select {
			// time out and then start an election
			case <-electionTimer:
				rf.startElection()
			// get heart beat and reset the timer
			case <-rf.heartbeat:

			// vote for some candidate and reset the timer
			case <-rf.vote:

			}
		}

		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.identity = CANDIDATE
	rf.votedFor = rf.me
	term := rf.currentTerm
	candidateID := rf.me
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()

	ms := ELECTIONTIMEOUT * (rand.Int63() % ELECTIONTIMEOUT)
	electionTimer := time.After(time.Duration(ms) * time.Millisecond)
	vote := make(chan struct{}, len(rf.peers)-1)
	stopElection := make(chan struct{}, len(rf.peers)-1)

	currentVotes := 1
	requireVotes := len(rf.peers)>>1 + 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}

			rf.sendRequestVote(server, args, reply)

			if reply.Term > term {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.identity = FOLLOWER
				rf.votedFor = NOTVOTE
				rf.mu.Unlock()
				stopElection <- struct{}{}
			}

			if reply.VoteGranted {
				vote <- struct{}{}
			}
		}(i)
	}

	for currentVotes < requireVotes {
		select {
		case <-electionTimer:
			rf.mu.Lock()
			rf.identity = FOLLOWER
			rf.votedFor = NOTVOTE
			rf.mu.Unlock()
			return
		case <-stopElection:
			return
		case <-rf.heartbeat:
			return
		case <-rf.vote:
			return
		case <-vote:
			currentVotes++
		}
	}

	// win an election

	rf.mu.Lock()
	rf.identity = LEADER
	rf.mu.Unlock()
	// send heartbeat to stop other election and
	// reset other server's election timer
	rf.sendHeartbeat()
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	leaderID := rf.me
	prevLogIndex := len(rf.log) - 1
	prevLogTerm := rf.log[len(rf.log)-1].Term
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				// no entries
				LeaderCommit: leaderCommit,
			}
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(server, args, reply)

			if reply.Term > term {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.identity = FOLLOWER
				rf.votedFor = NOTVOTE
				rf.mu.Unlock()
			}
		}(i)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = NOTVOTE
	rf.log = make([]Log, 1) // first index is 1
	rf.identity = FOLLOWER
	rf.leaderID = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.heartbeat = make(chan struct{})
	rf.vote = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

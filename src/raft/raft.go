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

	"6.824/labrpc"
)

// import "bytes"
// import "6.824/labgob"

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

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	votedFor        int
	state           State
	lastContact     time.Time
	electionTimeout time.Duration

	log           []LogEntry
	commitedIndex int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	isLeader := rf.state == Leader

	return currentTerm, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2C).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2C).

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// Reject older terms
	if args.Term < rf.currentTerm {
		return
	}

	// Update term if higher
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
rf.lastContact = time.Now()
		reply.Term = rf.currentTerm
	}

	// Get last log index and term of receiver
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	// Reject if candidate's log is not at least as up-to-date as receiver's log
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}

	// Grant vote if not voted or same candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTimeout = randomElectionTimeout()
rf.lastContact = time.Now()
	}
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	// Reject old terms
	if args.Term < rf.currentTerm {
		return
	}

	// If leader's term is greater, update currentTerm and convert to follower
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	// Reject if log doesn't contain an entry at prevLogIndex
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	// Reject if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		// Find the first index with ConflictTerm
		reply.ConflictIndex = args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				break
			}
			reply.ConflictIndex = i
		}
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for i, newEntry := range args.Entries {
		// Log index where this entry should go
		logIndex := args.PrevLogIndex + 1 + i

		// If the index is beyond the end of the log, break the loop
		if logIndex >= len(rf.log) {
			break
		}

		// If terms differ, truncate the log at this index
		if rf.log[logIndex].Term != newEntry.Term {
			rf.log = rf.log[:logIndex]
			break
		}
	}

	// Append any new entries not already in the log
	startIndex := args.PrevLogIndex + 1

	for i, newEntry := range args.Entries {
		logIndex := startIndex + i

		if logIndex >= len(rf.log) {
			rf.log = append(rf.log, newEntry)
		}
	}

	if args.LeaderCommit > rf.commitedIndex {
		rf.commitedIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	// reset contact time
	rf.lastContact = time.Now()

	reply.Success = true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		elapsed := time.Since(rf.lastContact)
		timeout := rf.electionTimeout
		isLeader := rf.state == Leader
		rf.mu.Unlock()

		if isLeader {
			// if i'm leader, send heartbeats
			rf.sendHeartbeats()
		} else if elapsed >= timeout {
			// if timeout elapsed, start election
			rf.startElection()

			// reset election timeout to a new random value
			rf.mu.Lock()
			rf.electionTimeout = randomElectionTimeout()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	// become candidate, vote for self, reset timer
	// start new term
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastContact = time.Now()
	rf.electionTimeout = randomElectionTimeout()

	term := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	votes := int32(1)
	total := len(rf.peers)

	// send RequestVote RPCs to all other servers
	for i := range rf.peers {
		if i == me {
			continue
		}

		go rf.requestVoteFromPeer(i, term, me, &votes, total)
	}
}

func (rf *Raft) requestVoteFromPeer(server int, term int, me int, votes *int32, total int) {
	args := RequestVoteArgs{Term: term, CandidateId: me}
	var reply RequestVoteReply

	ok := rf.sendRequestVote(server, &args, &reply)

	// If RPC failed, do nothing
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If peer term is greater, update currentTerm and convert to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		return
	}

	// If still candidate and vote granted, increment votes
	if rf.state == Candidate && reply.VoteGranted {
		// If got majority, become leader
		if atomic.AddInt32(votes, 1) > int32(total/2) {
			rf.state = Leader
			rf.lastContact = time.Now()
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
	rf.mu.Unlock()

	// send AppendEntries RPCs (heartbeats) to all other servers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.sendHeartbeatToPeer(i, &args)
	}
}

func (rf *Raft) sendHeartbeatToPeer(server int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, args, &reply)

	// If RPC failed, do nothing
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if peer term is greater, update currentTerm and convert to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.lastContact = time.Now()
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.lastContact = time.Now()

	// initialize random election timeout between 400-600ms
	rf.electionTimeout = randomElectionTimeout()

	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0, Command: nil} // Dummy entry

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func randomElectionTimeout() time.Duration {
	return time.Duration(400+rand.Intn(200)) * time.Millisecond
}

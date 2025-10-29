package raft

import "time"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
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
		rf.persist()
	}

	// Get last log index and term of receiver
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()

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
		rf.persist()
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
	Term          int
	Success       bool
	ConflictTerm  int // Term of the conflicting entry
	ConflictIndex int // First index of that term
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
		rf.persist()
	}

	// If leader's PrevLogIndex is before our snapshot, tell leader we need a snapshot install
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// leader is too far behind; instruct it about our snapshot boundary
		reply.ConflictIndex = rf.lastIncludedIndex
		reply.ConflictTerm = rf.lastIncludedTerm
		return
	}

	// If leader's PrevLogIndex is beyond our last log index, tell leader where our log ends
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	// Now PrevLogIndex is within our stored suffix: check term match
	prevSliceIdx := rf.sliceIndex(args.PrevLogIndex)
	if rf.log[prevSliceIdx].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[prevSliceIdx].Term

		// find first index (global) where that ConflictTerm appears
		// find slice index of first entry in that term
		firstSlice := prevSliceIdx
		for firstSlice > 0 && rf.log[firstSlice-1].Term == reply.ConflictTerm {
			firstSlice--
		}
		reply.ConflictIndex = rf.lastIncludedIndex + firstSlice
		return
	}

	// At this point PrevLogIndex matches. Merge entries:
	// startSlice is index in rf.log corresponding to args.PrevLogIndex+1
	startGlobal := args.PrevLogIndex + 1
	startSlice := rf.sliceIndex(startGlobal)

	// Iterate through incoming entries and reconcile with existing log
	for i := 0; i < len(args.Entries); i++ {
		si := startSlice + i
		if si < len(rf.log) {
			// If conflict, truncate and append the rest
			if rf.log[si].Term != args.Entries[i].Term {
				rf.log = rf.log[:si]
				// append remaining new entries
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			// follower is missing this entry; append remaining entries
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// Update commit index (global indexes)
	if args.LeaderCommit > rf.committedIndex {
		rf.committedIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}

	// reset contact time
	rf.lastContact = time.Now()

	reply.Success = true
	reply.Term = rf.currentTerm
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// If term is older, ignore
	if args.Term < rf.currentTerm {
		return
	}

	// If term is newer, update currentTerm and convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}
	rf.lastContact = time.Now()

	// if i already committed at least up to LastIncludedIndex, ignore
	if args.LastIncludedIndex <= rf.committedIndex {
		return
	}

	// Send snapshot to applyCh
	snap := make([]byte, len(args.Data))
	copy(snap, args.Data)
	lastIdx := args.LastIncludedIndex
	lastTerm := args.LastIncludedTerm

	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snap,
		SnapshotTerm:  lastTerm,
		SnapshotIndex: lastIdx,
	}
	rf.mu.Lock()

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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

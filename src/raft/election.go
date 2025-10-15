package raft

import (
	"sync/atomic"
	"time"
)

// startElection initiates a new leader election
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

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	votes := int32(1)
	total := len(rf.peers)

	// send RequestVote RPCs to all other servers
	for i := range rf.peers {
		if i == me {
			continue
		}

		args := RequestVoteArgs{Term: term, CandidateId: me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}

		go rf.requestVoteFromPeer(i, &votes, total, &args)
	}
}

// requestVoteFromPeer sends a RequestVote RPC to a peer and handles the response
func (rf *Raft) requestVoteFromPeer(server int, votes *int32, total int, args *RequestVoteArgs) {

	var reply RequestVoteReply

	ok := rf.sendRequestVote(server, args, &reply)

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

			// Initialize nextIndex and matchIndex for each server
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
		}

	}
}

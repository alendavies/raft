package raft

import "time"

// sendEntries sends AppendEntries RPCs (heartbeats or log entries) to all peers
func (rf *Raft) sendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

		for i := range rf.peers {
		if i == rf.me {
			continue
		}

		ni := rf.nextIndex[i]

		if ni <= rf.lastIncludedIndex {
			continue
		}

		prevLogIndex := ni - 1

		var prevLogTerm int
		if prevLogIndex == rf.lastIncludedIndex {
			prevLogTerm = rf.lastIncludedTerm
		} else {
			si := rf.sliceIndex(prevLogIndex)
			if si < 0 || si >= len(rf.log) {
				continue
			}
			prevLogTerm = rf.log[si].Term
		}

		start := rf.sliceIndex(ni)
		if start < 0 {
			start = 0
		}
		if start > len(rf.log) {
			start = len(rf.log)
		}
		entries := make([]LogEntry, len(rf.log[start:]))
		copy(entries, rf.log[start:])

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.committedIndex,
		}
		go rf.sendEntriesToPeer(i, &args)
	}
}

// sendEntriesToPeer sends AppendEntries RPC to a specific peer and handles the response
func (rf *Raft) sendEntriesToPeer(server int, args *AppendEntriesArgs) {
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
		rf.persist()
		return
	}

	// If not leader or term changed, ignore
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		// Follower accepted the entries
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex    // Follower has entries up to this index
			rf.nextIndex[server] = newMatchIndex + 1 // Next entry to send is after this
		}

		// Try to advance commitIndex
		rf.updateCommitIndex()
	} else {
		// Follower rejected, use conflict information to backup faster
		if reply.ConflictTerm == -1 {
			// Follower's log is too short (doesn't have PrevLogIndex)
			// Jump back to the end of follower's log
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			// Follower has a conflicting entry at PrevLogIndex with term ConflictTerm
			// Search backwards in leader's log for the last entry with that term
			lastIndexOfTerm := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					lastIndexOfTerm = rf.lastIncludedIndex + i // convert to global
					break
				}
			}

			if lastIndexOfTerm != -1 {
				// Leader also has entries with term ConflictTerm
				// Start sending from right after leader's last entry with that term
				rf.nextIndex[server] = lastIndexOfTerm + 1
			} else {
				// Leader doesn't have any entries with term ConflictTerm
				// Jump back to where that term starts in follower's log
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}

		// never let nextIndex go below 1
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}
}

// updateCommitIndex updates the commit index based on matchIndex of followers
func (rf *Raft) updateCommitIndex() {
	// Find the highest index N such that N > commitIndex
	for n := rf.lastLogIndex(); n > max(rf.committedIndex, rf.lastIncludedIndex); n-- {
		if rf.log[rf.sliceIndex(n)].Term != rf.currentTerm {
			continue
		}

		// Count how many servers have matchIndex >= N
		count := 1 // include self
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		// If a majority have matchIndex >= N, update commitIndex
		if count > len(rf.peers)/2 {
			rf.committedIndex = n
			break
		}
	}
}

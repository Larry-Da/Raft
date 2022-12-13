package raft

type RequestVoteArgs struct {
	CId        int
	CTerm      int
	CLogLength int
	CLogTerm   int
}

type RequestVoteReply struct {
	VoterId int
	Term    int
	Granted bool
}

func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}
	if rf.currentRole == Leader {
		return
		// 兜底逻辑，理论上Leader应该已经cancel election timer，不会走到这里来，不过有可能出现timer channel没消费的情况
	}
	rf.lock("election")
	rf.DebugWithType(ElectionLogType, "start election")
	rf.currentTerm += 1
	rf.currentRole = Candidate
	rf.votedFor = rf.me
	rf.votesReceived = make([]int, 1)
	rf.votesReceived[0] = rf.me

	lastTerm := 0
	if len(rf.logs) > 0 {
		lastTerm = rf.logs[len(rf.logs)-1].Term
	} else {
		lastTerm = rf.snapshotTerm
	}

	rf.resetElectionTimer()
	rf.persist()
	rf.unlock("election")

	for i, _ := range rf.peers {
		args := RequestVoteArgs{rf.me, rf.currentTerm, rf.getLogLength(), lastTerm}
		reply := RequestVoteReply{}
		if i != rf.me {
			go func(argsInner RequestVoteArgs, replyInner RequestVoteReply, idx int) {
				rf.sendRequestVote(idx, &argsInner, &replyInner)
				//fmt.Print(reply)
				rf.receiveRequestVoteReply(&replyInner)
			}(args, reply, i)
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if !rf.killed() {
		rf.lock("request vote")
		rf.DebugWithType(ElectionLogType, "args.CTerm:%v rf.currentTerm:%v", args.CTerm, rf.currentTerm)
		if args.CTerm > rf.currentTerm {
			rf.currentTerm = args.CTerm
			rf.currentRole = Follower
			rf.votedFor = -1
			rf.resetElectionTimer() // 回到follower
			rf.cancelReplicateTimer()
			rf.persist()
		}

		lastTerm := 0
		if len(rf.logs) > 0 {
			lastTerm = rf.logs[len(rf.logs)-1].Term
		} else {
			lastTerm = rf.snapshotTerm
		}

		rf.DebugWithType(ElectionLogType, "rf.logLength %d, lasTerm: %d", rf.getLogLength(), lastTerm)
		logOk := (args.CLogTerm > lastTerm) || (args.CLogTerm == lastTerm && args.CLogLength >= rf.getLogLength())
		rf.DebugWithType(ElectionLogType, "rf.votedFor: %v  args.CId: %v", rf.votedFor, args.CId)
		votedForOk := rf.votedFor == args.CId || rf.votedFor == -1
		rf.DebugWithType(ElectionLogType, "logOk:%v, votedForOk:%v, args.Cterm: %v, rf.currentTerm:%v", logOk, votedForOk, args.CTerm, rf.currentTerm)
		if args.CTerm == rf.currentTerm && logOk && votedForOk {
			rf.votedFor = args.CId
			reply.VoterId = rf.me
			reply.Term = rf.currentTerm
			reply.Granted = true
		} else {
			reply.VoterId = rf.me
			reply.Term = rf.currentTerm
			reply.Granted = false
		}

		rf.unlock("request vote")
	}
}

func (rf *Raft) receiveRequestVoteReply(reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	if rf.currentRole == Candidate && reply.Term == rf.currentTerm && reply.Granted {
		votedAlready := false
		for _, val := range rf.votesReceived {
			if val == reply.VoterId {
				votedAlready = true
				break
			}
		}
		if !votedAlready {
			rf.votesReceived = append(rf.votesReceived, reply.VoterId)
		}
		if len(rf.votesReceived) >= ((len(rf.peers)+1)+1)/2 {
			rf.currentLeader = rf.me
			rf.currentRole = Leader
			rf.Info("election success")
			rf.cancelElectionTimer()

			for i := range rf.peers {
				if i != rf.me {
					rf.sentLength[i] = rf.getLogLength()
					rf.ackedLength[i] = 0
				}
			}
			rf.replicateLog()
			rf.resetReplicateTimer()
		}

	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentRole = Follower
		rf.votedFor = -1
		rf.resetElectionTimer()
		rf.cancelReplicateTimer()
		rf.persist()
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.DebugWithType(ElectionLogType, "RequestVote to %d %+v", server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.DebugWithType(ElectionLogType, "RequestVoteReply from %d %+v", server, reply)
	return ok
}

package raft

type LogRequestArgs struct {
	LeaderId     int
	Term         int
	PrefixLen    int
	PrefixTerm   int
	LeaderCommit int
	Suffix       []logEntry
}

type LogResponse struct {
	Follower int
	Term     int
	Ack      int
	Success  bool
}

type BroadcastResponse struct {
	Index int
	Term  int
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Info("command issued: %v", command)
	index := -1
	term := -1
	isLeader := true
	if rf.currentRole == Leader {
		index, term = rf.broadcastMessage(command)
		isLeader = index != -1
	}
	//else {
	//	if rf.currentLeader != -1 {
	//		reply := &BroadcastResponse{}
	//		rf.DebugWithType(ReplicateLogType, "BroadcastMessageRequest %v to leader %d", command, rf.currentLeader)
	//		rf.peers[rf.currentLeader].Call("Raft.BroadcastMessageRequest", command, reply)
	//		index = reply.Index
	//		term = reply.Term
	//	}
	//}

	if index != -1 {
		index += 1
	}
	return index, term, isLeader
}

func (rf *Raft) broadcastMessage(command interface{}) (int, int) {
	rf.lock("start broadcast message")
	if rf.currentRole != Leader {
		rf.unlock("follower/candidate can't broadcast message")
		return -1, -1
	}
	defer func() {
		rf.persist()
		rf.unlock("end broadcast message by leader")
		//rf.replicateLog()
	}()
	rf.logs = append(rf.logs, logEntry{command, rf.currentTerm})
	rf.ackedLength[rf.me] = rf.getLogLength()
	return rf.getLogLength() - 1, rf.currentTerm
}

func (rf *Raft) BroadcastMessageRequest(command interface{}, reply *BroadcastResponse) {
	index, term := rf.broadcastMessage(command)
	reply.Index = index
	reply.Term = term
	rf.DebugWithType(ReplicateLogType, "BroadcastMessageReply: index %v term %v", reply.Index, reply.Term)
}

func (rf *Raft) sendReplicateLog(server int, args *LogRequestArgs, reply *LogResponse) bool {
	termSuffix := convertTermSuffix(args.Suffix)
	debugArgs := *args
	debugArgs.Suffix = make([]logEntry, 0)

	rf.DebugWithType(ReplicateLogType, "ReplicateLogRequest to %d %+v, suffix:%v", server, debugArgs, termSuffix)
	ok := rf.peers[server].Call("Raft.ReceiveLogRequest", args, reply)
	rf.DebugWithType(ReplicateLogType, "ReplicateLogReply from %d %+v", server, reply)
	return ok
}

func (rf *Raft) replicateLogForFollower(followerId int) {
	rf.lock("replicate log")
	rf.DebugWithType(ReplicateLogType, "sentLength %v", rf.sentLength)

	prefixLen := rf.sentLength[followerId]
	prefixTerm := rf.getTermByLogIndex(prefixLen - 1)
	if prefixTerm == -1 {
		rf.unlock("replicate log")
		rf.InstallSnapshot(followerId)
		return
	}
	//rf.Debug("rf.logs %v", rf.logs)
	suffix := rf.logs[rf.getRealIdxByLogIndex(prefixLen):]
	//rf.Debug("suffix: %v, %d", suffix, rf.getRealIdxByLogIndex(prefixLen))

	args := &LogRequestArgs{rf.me, rf.currentTerm, prefixLen, prefixTerm, rf.commitLength, suffix}
	reply := &LogResponse{}
	rf.unlock("replicate log")
	go func(followerIdInner int, argsInner *LogRequestArgs, replyInner *LogResponse) {
		rf.sendReplicateLog(followerIdInner, argsInner, replyInner)
		rf.receiveLogResponse(replyInner)
	}(followerId, args, reply)
}
func (rf *Raft) replicateLog() {
	if rf.killed() {
		return
	}
	for followerId := range rf.peers {
		if followerId != rf.me {
			rf.replicateLogForFollower(followerId)
		}
	}
	rf.resetReplicateTimer()
}

func (rf *Raft) ReceiveLogRequest(args *LogRequestArgs, reply *LogResponse) {
	if rf.killed() {
		return
	}
	rf.lock("ReceiveLogRequest")
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
		rf.cancelReplicateTimer()
		rf.persist()
	}

	if args.Term == rf.currentTerm {
		rf.currentRole = Follower
		rf.currentLeader = args.LeaderId
		rf.resetElectionTimer()
		rf.cancelReplicateTimer()
	}

	logLen := rf.getLogLength()
	prefixTerm := 0
	if logLen >= args.PrefixLen {
		prefixTerm = rf.getTermByLogIndex(args.PrefixLen - 1)
	}

	rf.DebugWithType(ReplicateLogType, "logLen:%d prefixTerm:%d", logLen, prefixTerm)
	logOk := (logLen >= args.PrefixLen) && (args.PrefixLen == 0 || prefixTerm == args.PrefixTerm)
	if args.Term == rf.currentTerm && logOk {
		rf.appendEntries(args.PrefixLen, args.LeaderCommit, args.Suffix)
		ack := args.PrefixLen + len(args.Suffix)
		reply.Follower = rf.me
		reply.Term = rf.currentTerm
		reply.Success = true
		reply.Ack = ack
	} else {

		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Ack = 0
		reply.Follower = rf.me
	}

	rf.unlock("ReceiveLogRequest")
}

func (rf *Raft) receiveLogResponse(reply *LogResponse) {
	rf.lock("receiveLogResponse")
	resend := false
	rf.DebugWithType(ReplicateLogType, "receive ack: %+v", reply)
	if reply.Term == rf.currentTerm && rf.currentRole == Leader {
		if reply.Success && reply.Ack >= rf.ackedLength[reply.Follower] {
			rf.sentLength[reply.Follower] = reply.Ack
			rf.ackedLength[reply.Follower] = reply.Ack
			rf.commitLogEntries()
		} else if rf.sentLength[reply.Follower] > 0 {
			rf.sentLength[reply.Follower] -= 1
			resend = true
		}
	} else if reply.Term > rf.currentTerm {
		rf.currentRole = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
		rf.cancelReplicateTimer()
	}
	rf.persist()
	rf.unlock("receiveLogResponse")
	if resend {
		rf.replicateLogForFollower(reply.Follower)
	}
}

func (rf *Raft) appendEntries(prefixLen int, leaderCommit int, suffix []logEntry) {
	termSuffix := convertTermSuffix(suffix)
	rf.DebugWithType(ReplicateLogType, "AppendEntries prefixLen:%v, leaderCommit: %v, suffix: %v", prefixLen, leaderCommit, termSuffix)
	logLen := rf.getLogLength()

	if len(suffix) > 0 && logLen > prefixLen {
		index := 0
		if logLen > prefixLen+len(suffix) {
			index = prefixLen + len(suffix) - 1
		} else {
			index = logLen - 1
		}

		if rf.getTermByLogIndex(index) != suffix[index-prefixLen].Term {
			rf.logs = rf.logs[:rf.getRealIdxByLogIndex(prefixLen)]
		}
	}
	rf.Debug("rf.logs before append %v", rf.logs)
	logLen = rf.getLogLength()
	if prefixLen+len(suffix) > logLen {
		for i := logLen - prefixLen; i <= len(suffix)-1; i++ {
			//rf.DebugWithType(ReplicateLogType, "prefixLen:%d, rf.logs: %v, suffix: %v", prefixLen, rf.logs, suffix)
			rf.logs = append(rf.logs, suffix[i])
		}
	}
	rf.Debug("rf.logs after append %v", rf.logs)

	rf.Debug("rf.commitLength %v", rf.commitLength)
	if leaderCommit > rf.commitLength {
		for i := rf.commitLength; i <= leaderCommit-1; i++ {
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.getLogByIndex(i).Message,
				CommandIndex:  i + 1,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			rf.applyMsgQueue <- msg
		}
		rf.commitLength = leaderCommit
	}
	rf.persist()
}

func (rf *Raft) commitLogEntries() {
	//define acks(x) = |{n in nodes | ackedLength[n] >= x}| // acks(x)返回的是在所有的nodes里，有多少个node的ackedLength是大于x的

	minAcks := ((len(rf.peers) + 1) + 1) / 2

	logLen := rf.getLogLength()
	maxReady := -1
	for i := 0; i < logLen; i++ {
		// 检验rf.logs[i] 是否超过了半数的承认
		ackInI := 0
		for j := 0; j < len(rf.ackedLength); j++ {
			if rf.ackedLength[j] > i {
				ackInI++
			}
		}
		if ackInI >= minAcks {
			maxReady = i
		} else {
			break
		}
	}
	maxReady += 1
	rf.Info("commitLength: %d", maxReady)
	if maxReady != 0 && maxReady > rf.commitLength && rf.getTermByLogIndex(maxReady-1) == rf.currentTerm {
		for i := rf.commitLength; i <= maxReady-1; i++ {
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.getLogByIndex(i).Message,
				CommandIndex:  i + 1,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			rf.applyMsgQueue <- msg
		}
		rf.commitLength = maxReady
	}
}

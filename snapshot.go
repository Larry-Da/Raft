package raft

type InstallSnapshotArgs struct {
	Term         int
	LeaderId     int
	SnapshotLen  int
	SnapshotTerm int
	Data         []byte
}

type InstallSnapshotReply struct {
	Term       int
	FollowerId int
	Success    bool
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.lock("application snapshot")
	defer rf.unlock("application snapshot")

	snapshotLength := index
	rf.DebugWithType(LogChangeLogType, "Snapshot start at %d, rf.commitLength: %d", index, rf.commitLength)
	if snapshotLength <= rf.snapshotLen {
		// 已经有快照，不需要了
		return
	}

	if snapshotLength > rf.commitLength {
		panic("snapshot uncommitted logEntry")
	}

	lastLog := rf.getLogByIndex(snapshotLength - 1)
	rf.logs = rf.logs[rf.getRealIdxByLogIndex(snapshotLength):]
	rf.snapshotLen = snapshotLength
	rf.snapshotTerm = lastLog.Term

	rf.persister.SaveStateAndSnapshot(rf.serializeRaftState(), snapshot)
	// Your code here (2D).

}

func (rf *Raft) getLogByIndex(logIndex int) logEntry {
	idx := rf.getRealIdxByLogIndex(logIndex)
	if idx < 0 {
		rf.Fatal("get log Error")
		return logEntry{}
	} else {
		return rf.logs[idx]
	}
}

func (rf *Raft) getRealIdxByLogIndex(logIndex int) int {
	idx := logIndex - rf.snapshotLen
	return idx
}

func (rf *Raft) getLogIndexByRealId(realId int) int {
	idx := realId + rf.snapshotLen
	return idx
}

func (rf *Raft) getTermByLogIndex(logIndex int) int {
	idx := rf.getRealIdxByLogIndex(logIndex)
	if idx >= 0 {
		if len(rf.logs) > 0 {
			return rf.logs[idx].Term
		} else {
			return rf.snapshotTerm
		}
	} else if idx == -1 {
		return rf.snapshotTerm
	} else {
		return -1
	}
}
func (rf *Raft) getLogLength() int {
	return rf.getLogIndexByRealId(len(rf.logs))
}

func (rf *Raft) InstallSnapshot(followerId int) {
	rf.lock("install snapshot")
	args := InstallSnapshotArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		SnapshotLen:  rf.snapshotLen,
		SnapshotTerm: rf.snapshotTerm,
		Data:         rf.persister.ReadSnapshot(),
	}

	reply := InstallSnapshotReply{}
	rf.unlock("install snapshot")
	go func() {
		rf.peers[followerId].Call("Raft.ReceiveInstallSnapshot", &args, &reply)
		rf.receiveSnapshotResponse(&args, &reply)
	}()
}

func (rf *Raft) ReceiveInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("ReceiveInstallSnapshot")
	defer rf.unlock("ReceiveInstallSnapshot")
	debugArgs := *args
	debugArgs.Data = make([]byte, 0)
	rf.Info("ReceiveInstallSnapshot: %+v", debugArgs)
	reply.Term = rf.currentTerm
	reply.FollowerId = rf.me
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
		rf.cancelReplicateTimer()
		defer rf.persist()
	}

	if rf.snapshotLen >= args.SnapshotLen {
		return
	}
	// success
	reply.Success = true
	startLen := args.SnapshotLen - rf.snapshotLen
	if len(rf.logs) > startLen {
		rf.logs = rf.logs[startLen:]
	} else {
		rf.logs = make([]logEntry, 0)
	}

	rf.snapshotTerm = args.SnapshotTerm
	rf.snapshotLen = args.SnapshotLen
	rf.persister.SaveStateAndSnapshot(rf.serializeRaftState(), args.Data)

	msg := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.snapshotTerm,
		SnapshotIndex: rf.snapshotLen,
	}
	rf.applyMsgQueue <- msg
	rf.commitLength = rf.snapshotLen
	rf.persist()
}

func (rf *Raft) receiveSnapshotResponse(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("receiveSnapshotResponse")
	defer rf.unlock("receiveSnapshotResponse")

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentRole = Follower
		rf.votedFor = -1
		rf.resetElectionTimer()
		rf.cancelReplicateTimer()
		rf.persist()
	}
	if rf.currentTerm != args.Term || rf.currentRole != Leader {
		return
	}
	// success
	if args.SnapshotLen > rf.ackedLength[reply.FollowerId] {
		rf.ackedLength[reply.FollowerId] = args.SnapshotLen
	}
	if args.SnapshotLen > rf.sentLength[reply.FollowerId] {
		rf.sentLength[reply.FollowerId] = args.SnapshotLen
	}
	rf.persist()

}

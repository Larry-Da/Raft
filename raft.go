package raft

//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"github.com/sirupsen/logrus"
	"os"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"github.com/Larry-Da/Raft/labrpc"
)

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

type Role int

const (
	Leader = iota
	Candidate
	Follower
)

type logEntry struct {
	Message interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// states related to election
	currentTerm   int
	votedFor      int
	currentRole   Role
	currentLeader int
	votesReceived []int

	// states related to logs
	logs         []logEntry
	commitLength int
	sentLength   []int
	ackedLength  []int
	applyCh      chan ApplyMsg

	// timers
	lastHeardFromLeader time.Time
	electionTimer       *time.Timer
	replicateTimer      *time.Timer

	// snapshot
	snapshotLen  int
	snapshotTerm int

	// applyMsgQueue
	applyMsgQueue chan ApplyMsg

	// logrus
	logger *logrus.Logger
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.lock("Get state")
	rf.DebugWithType(ElectionLogType, "Check State")
	term = rf.currentTerm
	isleader = rf.currentRole == Leader
	rf.unlock("Get state")
	return term, isleader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.applyMsgQueue = make(chan ApplyMsg, 100)
	// logger

	rf.logger = logrus.New()
	//file := "./" + "raft_log" + ".txt"
	//logFile, _ := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	//mw := io.MultiWriter(os.Stdout, logFile)
	//rf.logger.SetOutput(mw)
	rf.logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
	})
	rf.logger.SetOutput(os.Stdout)
	rf.logger.SetLevel(logrus.InfoLevel)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.currentRole = Follower
	rf.currentLeader = -1
	// rf.votesReceived

	rf.snapshotLen = 0
	rf.snapshotTerm = 0

	// rf.log
	rf.commitLength = 0
	rf.sentLength = make([]int, len(peers))
	rf.ackedLength = make([]int, len(peers))

	rf.lastHeardFromLeader = time.Now()
	rf.electionTimer = time.NewTimer(randElectionTimeout())
	rf.replicateTimer = time.NewTimer(ReplicateInterval)
	rf.cancelReplicateTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()

	// election and replicate goroutine
	go func() {
		for {
			select {
			case <-rf.replicateTimer.C:
				rf.DebugWithType(ReplicateLogType, "Replicate periodically")
				rf.replicateLog()
				rf.resetReplicateTimer()
			case <-rf.electionTimer.C:
				rf.Info("Timeout election")
				rf.DebugWithType(ElectionLogType, "election timeout")
				rf.startElection()
				rf.Info("election end")
			}
		}
	}()

	go func() {
		for !rf.killed() {
			msg := <-rf.applyMsgQueue
			applyCh <- msg
			rf.DebugWithType(LogChangeLogType, "Deliver message: %+v", msg)
		}
	}() // this goroutine is used for deliver messages, in order not to block the leader

	return rf
}

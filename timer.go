package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// Election Timer

func (rf *Raft) cancelElectionTimer() {
	ok := rf.electionTimer.Stop()
	rf.DebugWithType(ElectionLogType, "election timer stop %v", ok)
	if ok {
		//fmt.Printf("-------")
		//fmt.Println(<-rf.electionTimer.C)
		//fmt.Printf("+++++++")
	} else {
		select {
		case t := <-rf.electionTimer.C:
			fmt.Println(t)
		default:
		}
		//<-rf.electionTimer.C
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
	rf.DebugWithType(ElectionLogType, "reset election timer")
}

func randElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}

// Replicate Timer
func (rf *Raft) cancelReplicateTimer() {
	ok := rf.replicateTimer.Stop()
	rf.DebugWithType(ElectionLogType, "replicate timer stop %v", ok)
	if ok {
	} else {
		select {
		case <-rf.replicateTimer.C:
		default:
		}
		//<-rf.replicateTimer.C
	}
}

func (rf *Raft) resetReplicateTimer() {
	rf.DebugWithType(ElectionLogType, "Reset replicate timer")
	rf.replicateTimer.Stop()
	rf.replicateTimer.Reset(ReplicateInterval)
}

package raft

func (rf *Raft) lock(msg string) {
	rf.Trace("%s wait lock %d", msg, rf.me)
	rf.mu.Lock()
	rf.Trace("%s acquire lock %d", msg, rf.me)
}

func (rf *Raft) unlock(msg string) {
	rf.mu.Unlock()
	rf.Trace("%s release lock %d", msg, rf.me)
}

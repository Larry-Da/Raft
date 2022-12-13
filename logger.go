package raft

import "github.com/sirupsen/logrus"

type LogType int

const (
	ElectionLogType = iota
	ReplicateLogType
	LogChangeLogType
	OtherType
)

func roleToString(role Role) string {
	if role == Leader {
		return "leader"
	} else if role == Follower {
		return "Follower"
	} else {
		return "Candidate"
	}
}

func (rf *Raft) DebugWithType(logType LogType, format string, args ...interface{}) {
	if logType != ElectionLogType {
		rf.Debug(format, args...)
	}
}

func (rf *Raft) InfoWithType(logType LogType, format string, args ...interface{}) {
	if logType == ElectionLogType {
		rf.Info(format, args...)
	}
}

func (rf *Raft) Debug(format string, args ...interface{}) {
	rf.logger.WithFields(logrus.Fields{
		"nodeId": rf.me,
		"term":   rf.currentTerm,
		"role":   roleToString(rf.currentRole),
	}).Debugf(format, args...)
}

func (rf *Raft) Info(format string, args ...interface{}) {
	rf.logger.WithFields(logrus.Fields{
		"nodeId": rf.me,
		"term":   rf.currentTerm,
		"role":   roleToString(rf.currentRole),
	}).Infof(format, args...)
}

func (rf *Raft) Trace(format string, args ...interface{}) {
	rf.logger.WithFields(logrus.Fields{
		"nodeId": rf.me,
		"term":   rf.currentTerm,
		"role":   roleToString(rf.currentRole),
	}).Tracef(format, args...)
}

func (rf *Raft) Warn(format string, args ...interface{}) {
	rf.logger.WithFields(logrus.Fields{
		"nodeId": rf.me,
		"term":   rf.currentTerm,
		"role":   roleToString(rf.currentRole),
	}).Warnf(format, args...)
}

func (rf *Raft) Fatal(format string, args ...interface{}) {
	rf.logger.WithFields(logrus.Fields{
		"nodeId": rf.me,
		"term":   rf.currentTerm,
		"role":   roleToString(rf.currentRole),
	}).Fatalf(format, args...)
}

func (rf *Raft) Error(format string, args ...interface{}) {
	rf.logger.WithFields(logrus.Fields{
		"nodeId": rf.me,
		"term":   rf.currentTerm,
		"role":   roleToString(rf.currentRole),
	}).Errorf(format, args...)
}

func convertTermSuffix(suffix []logEntry) []int {
	termSuffix := make([]int, len(suffix))
	for i := range suffix {
		termSuffix[i] = suffix[i].Term
	}
	return termSuffix
}

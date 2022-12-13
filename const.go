package raft

import "time"

const (
	ElectionTimeout   = time.Millisecond * 300
	ReplicateInterval = time.Millisecond * 150
)

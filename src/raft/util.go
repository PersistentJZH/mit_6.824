package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

const (
	StateFollower  = "Follower"
	StateCandidate = "Candidate"
	StateLeader    = "Leader"
)

const (
	HeartbeatTimeout = 100
	ElectionTimeout  = 1000
)

func GetHeartbeatDuration() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func GetElectionDuration() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(ElectionTimeout+rand.Intn(ElectionTimeout)) * time.Millisecond
}

package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandTerm  int
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []Entry
	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex      []int
	matchIndex     []int
	applyCond      *sync.Cond   // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state          string
	applyCh        chan ApplyMsg
	electionTicker *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("{Node %v} restores persisted state failed", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
	// there will always be at least one entry in rf.logs
	rf.lastApplied, rf.commitIndex = rf.logs[0].Index, rf.logs[0].Index
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) RequestVote(args *RequestVoteRequest, reply *RequestVoteResponse) {
	// common logic: all servers need
	if args.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.electionTicker.Reset(GetElectionDuration())
		return
	}
}

// used by RequestVote Handler to judge which log is newer
func (rf *Raft) isLogUpToDate(term, index int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

func (rf *Raft) ChangeState(state string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == state {
		return
	}
	DPrintf("Term %d: {Node %d} changes state from %s to %s ", rf.currentTerm, rf.me, rf.state, state)

	rf.state = state
	switch state {
	case StateFollower:
		rf.heartbeatTimer.Stop()
		rf.electionTicker.Reset(GetElectionDuration())
	case StateCandidate:
	case StateLeader:
		rf.electionTicker.Stop()
		rf.heartbeatTimer.Reset(GetHeartbeatDuration())
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteRequest, reply *RequestVoteResponse) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, response *AppendEntriesResponse) bool {
	return rf.peers[server].Call("Raft.AppendEntries", request, response)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader {
		fmt.Println("跳过", command, rf.me)
		return -1, -1, false
	}

	newEntry := rf.appendNewEntry(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newEntry, rf.currentTerm)
	// append entry to followers
	rf.BroadcastAppendEntries(false)
	return newEntry.Index, newEntry.Term, true
}

func (rf *Raft) appendNewEntry(command interface{}) Entry {
	lastLog := rf.getLastLog()
	newLog := Entry{lastLog.Index + 1, rf.currentTerm, command}
	rf.logs = append(rf.logs, newLog)
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLog.Index, newLog.Index+1
	return newLog
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) StartElection() {
	// 1.generate election request
	lastLog := rf.getLastLog()
	request := &RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	grantedVotes := 1
	rf.votedFor = rf.me

	// 3.seek for vote
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteResponse)
			if rf.sendRequestVote(peer, request, response) {

				if rf.currentTerm == request.Term && rf.state == StateCandidate {
					if response.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							rf.ChangeState(StateLeader)
						}

					} else if response.Term > rf.currentTerm {
						rf.ChangeState(StateFollower)
						rf.currentTerm, rf.votedFor = response.Term, -1
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) {

	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	rf.ChangeState(StateFollower)
	rf.electionTicker.Reset(GetElectionDuration())

	if request.PrevLogIndex < rf.getFirstLog().Index {
		response.Term, response.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		fmt.Println("receive AppendEntries 1")
		response.Term, response.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex {
			response.ConflictTerm, response.ConflictIndex = -1, lastIndex+1
		} else {
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term
			index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm {
				index--
			}
			response.ConflictIndex = index
		}
		return
	}
	fmt.Println("receive AppendEntries")

	firstIndex := rf.getFirstLog().Index
	for index, entry := range request.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...)
			break
		}
	}

	rf.calFollowerCommitIndex(request.LeaderCommit)

	fmt.Println("return AppendEntries")

	response.Term, response.Success = rf.currentTerm, true
}

// func (rf *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) {
//
// 	// common logic: all servers need
// 	if request.Term > rf.currentTerm {
// 		rf.ChangeState(StateFollower)
// 		rf.currentTerm, rf.votedFor = request.Term, -1
// 	}
//
// 	if request.Term < rf.currentTerm {
// 		response.Term, response.Success = rf.currentTerm, false
// 		return
// 	}
// 	rf.ChangeState(StateFollower)
//
// 	// if heart beat request
// 	if request.Entries == nil || len(request.Entries) == 0 {
// 		rf.calFollowerCommitIndex(request.LeaderCommit)
// 		response.Term, response.Success = rf.currentTerm, true
// 		return
// 	}
// 	// if append entry request
// 	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
// 		// not enough
// 		// prevIndex > lastIndex || rf.logs[index-rf.getFirstLog().Index].Term != term
// 		response.Term, response.Success = rf.currentTerm, false
// 		lastIndex := rf.getLastLog().Index
// 		if lastIndex < request.PrevLogIndex {
// 			response.ConflictTerm, response.ConflictIndex = -1, lastIndex+1
// 		} else {
// 			firstIndex := rf.getFirstLog().Index
// 			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term
// 			index := request.PrevLogIndex - 1
// 			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm {
// 				index--
// 			}
// 			response.ConflictIndex = index
// 		}
// 		return
// 	}
//
// 	// if match
// 	firstIndex := rf.getFirstLog().Index
// 	for index, entry := range request.Entries {
// 		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
// 			rf.logs = append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...)
// 			break
// 		}
// 	}
//
// 	rf.calFollowerCommitIndex(request.LeaderCommit)
//
// 	response.Term, response.Success = rf.currentTerm, true
// }

func (rf *Raft) sendHeartBeatAppendEntries(peer int) {

	if rf.state != StateLeader {

		return
	}

	request := &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	response := new(AppendEntriesResponse)
	DPrintf("Term:%d, [Node %v] send heart beat to [Node %v]", rf.currentTerm, rf.me, peer)
	if rf.sendAppendEntries(peer, request, response) {
		if response.Term > rf.currentTerm {
			rf.ChangeState(StateFollower)
			rf.currentTerm, rf.votedFor = response.Term, -1
		}
	}

}
func (rf *Raft) appendEntry(peer int) {
	fmt.Println("send appendEntry")
	if rf.state != StateLeader {
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1

	if prevLogIndex < rf.getFirstLog().Index {
		fmt.Println("snapshot")
		// snapshot
	} else {
		// just entries can catch up
		firstIndex := rf.getFirstLog().Index
		entries := make([]Entry, len(rf.logs[prevLogIndex+1-firstIndex:]))
		copy(entries, rf.logs[prevLogIndex+1-firstIndex:])
		request := &AppendEntriesRequest{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.logs[prevLogIndex-firstIndex].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		response := new(AppendEntriesResponse)
		if rf.sendAppendEntries(peer, request, response) {
			fmt.Println("handle appendEntry")
			rf.handleAppendEntriesResponse(peer, request, response)

		}
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) {
	fmt.Println("handle appendEntry in")

	if rf.state == StateLeader && rf.currentTerm == request.Term {
		if response.Success {
			rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.calLeaderCommitIndex()
		} else {
			if response.Term > rf.currentTerm {
				rf.ChangeState(StateFollower)
				rf.currentTerm, rf.votedFor = response.Term, -1
				rf.persist()
			} else if response.Term == rf.currentTerm {
				rf.nextIndex[peer] = response.ConflictIndex
				if response.ConflictTerm != -1 {
					firstIndex := rf.getFirstLog().Index
					for i := request.PrevLogIndex; i >= firstIndex; i-- {
						if rf.logs[i-firstIndex].Term == response.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling AppendEntriesResponse %v for AppendEntriesRequest %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), response, request)
}

// func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) {
// 	// common logic: all servers need
// 	if response.Term > rf.currentTerm {
// 		rf.ChangeState(StateFollower)
// 		rf.currentTerm, rf.votedFor = response.Term, -1
// 		return
// 	}
//
// 	if rf.state != StateLeader || rf.currentTerm != request.Term {
// 		return
// 	}
// 	// success = true -> no conflicts
// 	if response.Success {
// 		rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
// 		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
// 		// how to cal LeaderCommitIndex from []matchIndex
// 		// its a simple logic
// 		// case 1
// 		// []matchIndex = [3,5,3,1,3] => sort => [1,3,3,3,4]  => LeaderCommitIndex = 3
// 		// case 2
// 		// ... => sort => [1,1,3,3,3,4,4,4,4]  => LeaderCommitIndex = 3
// 		rf.calLeaderCommitIndex()
// 	} else {
// 		rf.nextIndex[peer] = response.ConflictIndex
// 		if response.ConflictTerm != -1 {
// 			firstIndex := rf.getFirstLog().Index
// 			for i := request.PrevLogIndex; i >= firstIndex; i-- {
// 				if rf.logs[i-firstIndex].Term == response.ConflictTerm {
// 					rf.nextIndex[peer] = i + 1
// 					break
// 				}
// 			}
// 		}
// 	}
//
// 	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling AppendEntriesResponse %v for AppendEntriesRequest %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), response, request)
// }

func (rf *Raft) calLeaderCommitIndex() {
	n := len(rf.matchIndex)
	srt := make([]int, n)
	copy(srt, rf.matchIndex)
	insertionSort(srt)
	newCommitIndex := srt[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		// only allow commit current term's log
		if rf.matchLog(rf.currentTerm, newCommitIndex) {
			fmt.Println("leader commit")
			DPrintf("{Node %d} advance commitIndex from %d to %d with matchIndex %v in term %d", rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()

		} else {
			DPrintf("{Node %d} can not advance commitIndex from %d because the term of newCommitIndex %d is not equal to currentTerm %d", rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
		}
	}
}

func (rf *Raft) calFollowerCommitIndex(leaderCommit int) {
	newCommitIndex := Min(leaderCommit, rf.getLastLog().Index)
	fmt.Println("cal follower commit", newCommitIndex, rf.commitIndex)
	fmt.Println(newCommitIndex, rf.commitIndex)

	if newCommitIndex > rf.commitIndex {
		if rf.matchLog(rf.currentTerm, newCommitIndex) {
			fmt.Println("follower commit")
			DPrintf("{Node %d} advance commitIndex from %d to %d with leaderCommit %d in term %d", rf.me, rf.commitIndex, newCommitIndex, leaderCommit, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}

	}
}

func (rf *Raft) matchLog(term, index int) bool {
	return index <= rf.getLastLog().Index && rf.logs[index-rf.getFirstLog().Index].Term == term
}

func (rf *Raft) needReplicating(peer int) bool {

	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) sendNormalAppendEntries(peer int) {
	for rf.killed() == false {
		for !rf.needReplicating(peer) {
			return
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.appendEntry(peer)
	}
}

// BroadcastAppendEntries mainly contains two functions:
// append entry
// heartbeat
func (rf *Raft) BroadcastAppendEntries(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			go rf.sendHeartBeatAppendEntries(peer)
		} else {
			// go rf.sendNormalAppendEntries(peer)
			rf.replicatorCond[peer].Signal()

		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		for {
			select {
			case <-rf.electionTicker.C:
				rf.ChangeState(StateCandidate)
				rf.currentTerm += 1
				DPrintf("Term:%d, {Node %v} start request vote in ticker", rf.currentTerm, rf.me)
				rf.StartElection()
				rf.electionTicker.Reset(GetElectionDuration())

			case <-rf.heartbeatTimer.C:
				if rf.state == StateLeader {
					rf.BroadcastAppendEntries(true)
					rf.heartbeatTimer.Reset(GetHeartbeatDuration())
				}

			}
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		replicatorCond: make([]*sync.Cond, len(peers)),

		// 广播时间（broadcastTime） << 选举超时时间（electionTimeout） << 平均故障间隔时间（MTBF
		electionTicker: time.NewTimer(GetElectionDuration()),
		heartbeatTimer: time.NewTimer(GetHeartbeatDuration()),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	lastLog := rf.getLastLog()

	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}

	go rf.ticker()

	go rf.applier()

	return rf
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// if there is no need to replicate entries for this peer, just release CPU and wait other goroutine's signal if service adds new Command
		// if this peer needs replicating entries, this goroutine will call replicateOneRound(peer) multiple times until this peer catches up, and then wait
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.appendEntry(peer)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			fmt.Printf("applier %v, server= %v \n", entry, rf.me)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

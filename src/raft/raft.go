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
	//  "bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//  "6.5840/labgob"
	"6.5840/labrpc"
)

type State int

const (
	STATE_LEADER State = iota
	STATE_CANDIDATE
	STATE_FOLLOWER
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
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// volatile state on all servers
	commitIndex           int      // index of highest log entry known to be committed
	lastApplied           int      // index of highest log entry applied to state machine
	state                 State    // TODO: paper do not mention this, is it necessary or not?
	stateChangeToFollower chan int // channel contains term when follower state is changed to follower
	stateMutex            sync.Mutex
	leaderCtx             context.Context
	leaderCancelFunc      context.CancelFunc

	// volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	return rf.currentTerm, rf.state == STATE_LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 1. Reply false if candidate's term < currentTerm (§5.1)
	rf.stateMutex.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.stateMutex.Unlock()
		reply.VoteGranted = false
		return
	}

	// 2. change to follower if candidate's term > currentTerm (not necessarily grant vote)
	if args.Term > rf.currentTerm {
		rf.stateMutex.Unlock()
		rf.stateChangeToFollower <- args.Term
		rf.stateMutex.Lock()
	}

	// 3. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
	// (I think the paper is not very clear about the condition, following is my idea)
	// case 1: args.Term == currentTerm, vote if not voted + log is up-to-date
	// case 2: args.Term > currentTerm, vote if log is up-to-date
	var lastLog *LogEntry
	if len(rf.log) > 0 {
		lastLog = &rf.log[len(rf.log)-1]
	} else {
		lastLog = nil
	}
	logUpToDate := lastLog == nil || args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= len(rf.log)-1)
	voteDecision := logUpToDate
	if args.Term == rf.currentTerm {
		voteDecision = voteDecision && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	}

	if voteDecision {
		rf.votedFor = args.CandidateId
		rf.stateMutex.Unlock()
		reply.VoteGranted = true
	} else {
		rf.stateMutex.Unlock()
		reply.VoteGranted = false
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC argttuments in args.
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// do this have tiemout?
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// what if args.term < rf.currentTerm?
	// 1. Reply false if term < currentTerm (§5.1)
	now := time.Now()
	defer func() {
		Pf("[%d]log=%v\n", rf.me, rf.log)
		Pf("[%d]AppendEntries Takes=%v ms\n", rf.me, time.Since(now).Milliseconds())
	}()
	rf.stateMutex.Lock()
	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	if args.Term < currentTerm {
		// I think the semantic of "false" when using as heartbeat message is different from "false" when using as append entries message
		reply.Success = false
		rf.stateMutex.Unlock()
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = -1
	if rf.leaderCancelFunc != nil {
		rf.leaderCancelFunc()
	}
	rf.stateMutex.Unlock()
	rf.stateChangeToFollower <- args.Term

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// case 1: first log, prevLogIndex == 0, should return true
	// case 2: prevLogIndex > 0, but prevLogIndex > len(rf.log), should return false
	// case 3: prevLogIndex >= 0, and prevLogIndex <= len(rf.log), but prevLogTerm != rf.log[prevLogIndex-1].Term, should return false
	// CONSISTENCY CHECK, need to be done BOTH for heartbeat message and append entries message!!
	rf.stateMutex.Lock()
	if args.PrevLogIndex > 0 && (args.PrevLogIndex > len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex-1].Term) {
		reply.Success = false
		rf.stateMutex.Unlock()
		return
	}
	if len(args.Entries) > 0 {
		// 3 and 4 are exclusive logic for AppendEntries Messages
		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		if args.PrevLogIndex < len(rf.log) {
			rf.log = rf.log[:args.PrevLogIndex]
		}

		// 4. Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries...)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// what is commitIndex used for? - to commit log entries to follower state machine -> change application's state
	// fmt.Println("leaderCommit=", args.LeaderCommit, "commitIndex=", rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		lastCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		// TODO: think! when will there be multiple entries to be committed?
		// TODO: will this block?
		for idx := lastCommitIndex + 1; idx <= rf.commitIndex; idx++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: idx,
				Command:      rf.log[idx-1].Command,
			}
			Pf("[%d]follower committed index %d, args=%+v, rf=%+v\n", rf.me, idx, args, rf)
		}
	}
	rf.stateMutex.Unlock()
	reply.Success = true

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
	// Your code here (2B).
	// 1. if not leader, return false
	rf.stateMutex.Lock()
	state := rf.state
	if state != STATE_LEADER {
		rf.stateMutex.Unlock()
		return -1, -1, false
	}

	// 2. if leader, append command to log, then send AppendEntries RPCs to all other servers in a seperate goroutine
	Pf("[%d]leader append command %v, nextIndex=%v\n", rf.me, command, rf.nextIndex)
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.matchIndex[rf.me] = len(rf.log)
	rf.lastApplied = len(rf.log) - 1
	index := len(rf.log)
	term := rf.currentTerm
	rf.stateMutex.Unlock()
	go rf.replicateEntry(command)
	return index, term, true
}

func (rf *Raft) replicateEntry(command interface{}) {
	defer func() {
		Pf("[%d]log=%v\n", rf.me, rf.log)
	}()

	select {
	case <-rf.leaderCtx.Done():
		Pf("[%d] Replication: state is not leader anymore, return\n", rf.me)
		return
	default:
		var wg sync.WaitGroup
		ch := make(chan AppendEntriesReply)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)

			go func(i int) {
				defer wg.Done()
				select {
				case <-rf.leaderCtx.Done():
					Pf("[%d] Replication: state is not leader anymore, return\n", rf.me)
					return
				default:
					rf.replicateEntrySingle(i, ch)
				}
			}(i)
		}
		go func() {
			wg.Wait()
			close(ch)
		}()
		received := 0
		// gather response, until majority of servers append entries successfully
		for received < len(rf.peers)-1 {
			_ = <-ch
			received++

			// update commitIndex, but first check if the node is still a leader (done outside of the loop)
			if rf.leaderCtx.Err() != nil {
				Pf("[%d] Replication: state is not leader anymore, return\n", rf.me)
				return
			}
			rf.stateMutex.Lock()
			lastCommitIndex := rf.commitIndex
			Pf("[%d] leader commit index=%v, matchindex=%v, rf=%+v\n", rf.me, rf.commitIndex, rf.matchIndex, rf)
			rf.commitIndex = findCommitIndex(rf.matchIndex, rf.log, rf.currentTerm, rf.commitIndex)
			// one time may commit multiple entries
			for idx := lastCommitIndex + 1; idx <= rf.commitIndex; idx++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					CommandIndex: idx,
					Command:      rf.log[idx-1].Command,
				}
				Pf("[%d] leader committed index %d\n", rf.me, idx)
			}
			// not safe for holding a lock while sending msg on channel (possibly blocking!)
			rf.stateMutex.Unlock()
		}
	}
}

func (rf *Raft) replicateEntrySingle(i int, ch chan AppendEntriesReply) {
	var args AppendEntriesArgs
	rf.stateMutex.Lock()
	currentTerm := rf.currentTerm
	args.Term = currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[i] - 1
	if args.PrevLogIndex > 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
	} else {
		args.PrevLogTerm = 0
	}
	args.Entries = rf.log[rf.nextIndex[i]-1:]
	args.LeaderCommit = rf.commitIndex
	rf.stateMutex.Unlock()

	var reply AppendEntriesReply
	ok := false
	// infiinte loop, mentioned by paper
	// TODO: will this cause problme in a real system?
	for !ok {
		// Pf("[%d] Replication1: sendAppendEntries to %d, req=%+v\n", rf.me, i, args)
		if rf.leaderCtx.Err() != nil {
			Pf("[%d] Replication: state is not leader anymore, return\n", rf.me)
			return
		}
		ok = rf.sendAppendEntries(i, &args, &reply)
		// Pf("[%d] Replication1: sendAppendEntries to %d ok, req=%+v, reply=%+v\n", rf.me, i, args, reply)
	}

	// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
	for !reply.Success {
		if reply.Term > currentTerm {
			Pf("[%d] Replication: find higher term: %v from [%v], self term is %v\n, change to follower, cancelfunc=%v\n", rf.me, reply.Term, i, currentTerm, rf.leaderCancelFunc == nil)
			rf.leaderCancelFunc()
			rf.stateChangeToFollower <- reply.Term
			// In practice I should cancel all goroutines and rpc call, but this lab's rpc framework does not support context cancellation
			return
		}
		rf.stateMutex.Lock()
		rf.nextIndex[i]--
		args.PrevLogIndex = rf.nextIndex[i] - 1
		if args.PrevLogIndex > 0 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
		} else {
			args.PrevLogTerm = 0
		}
		if rf.nextIndex[i] <= 0 {
			Pf("[%d]pre panic nextindex[%d] <= 0\n", rf.me, i)
			panic("nextIndex[i] <= 0")
		}
		args.Entries = rf.log[rf.nextIndex[i]-1:]
		rf.stateMutex.Unlock()

		// Pf("[%d] Replication2: sendAppendEntries to %d, req=%+v\n", rf.me, i, args)
		if rf.leaderCtx.Err() != nil {
			Pf("[%d] Replication: state is not leader anymore, return\n", rf.me)
			return
		}
		ok := rf.sendAppendEntries(i, &args, &reply)
		// Pf("[%d] Replication2: sendA/ppendEntries to %d ok, args=%+v, reply=%+v\n", rf.me, i, args, reply)
		for !ok {
			ok = rf.sendAppendEntries(i, &args, &reply)
		}
	}

	// If successful AppendEntries RPC: update nextIndex and matchIndex for follower (§5.3)
	// Pf("[%d] Replication: sendAppendEntries to %d ok, req=%+v, reply=%+v\n", rf.me, i, args, reply)
	rf.stateMutex.Lock()
	rf.nextIndex[i] = len(rf.log) + 1
	rf.matchIndex[i] = len(rf.log)
	rf.stateMutex.Unlock()
	ch <- reply
}

func findCommitIndex(matchIndexes []int, log []LogEntry, currentTerm int, commitIndex int) int {
	// Sort matchIndexes in descending order
	matchIndexesCopy := make([]int, len(matchIndexes))
	copy(matchIndexesCopy, matchIndexes)
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndexesCopy)))

	for _, N := range matchIndexesCopy {
		if N <= commitIndex {
			// Since the array is sorted, no further N will satisfy N > commitIndex
			break
		}

		// Check if log[N].term == currentTerm
		if log[N-1].Term == currentTerm {
			// Count how many matchIndexes are greater than or equal to N
			count := 0
			for _, matchIndex := range matchIndexesCopy {
				if matchIndex >= N {
					count++
				}
			}

			// Check if a majority is achieved
			if count > len(matchIndexesCopy)/2 {
				return N // Found a valid N
			}
		}
	}

	return commitIndex // No valid N found, return the original commitIndex
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.stateMutex.Lock()
		state := rf.state
		rf.stateMutex.Unlock()
		// Your code here (2A)
		if state == STATE_LEADER {
			rf.leaderLoop()
			// what's the interval between heartbeats?
			ms := 50 + (rand.Int63() % 50)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else if state == STATE_FOLLOWER {
			rf.followerLoop()
		} else if state == STATE_CANDIDATE {
			rf.candidateLoop()
		}
	}
}

func (rf *Raft) leaderLoop() {
	// 1. leader state: send heartbeats to all peers through AppendEntries RPC
	Pf("[%d]become leader\n", rf.me)
loop:
	for {
		var wg sync.WaitGroup
		ch := make(chan AppendEntriesReply, len(rf.peers)-1)
		rf.stateMutex.Lock()
		currentTerm := rf.currentTerm
		leaderCommit := rf.commitIndex
		prevLogIndex := len(rf.log)
		var prevLogTerm int
		if prevLogIndex > 0 {
			prevLogTerm = rf.log[prevLogIndex-1].Term
		} else {
			prevLogTerm = 0
		}

		rf.stateMutex.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				var args AppendEntriesArgs
				args.Term = currentTerm
				args.LeaderId = rf.me
				// What other information should be included?
				// I think only leaderCommit is needed in heartbeats --- NO! prevLogIndex and prevLogTerm are also needed for consistency check!!
				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = prevLogTerm
				args.LeaderCommit = leaderCommit
				var reply AppendEntriesReply
				// Pf("[%d]is leader, try to send heartbeats to %d\n", rf.me, i)
				rf.sendAppendEntries(i, &args, &reply)
				// Pf("[%d]is leader, send heartbeats to %d ok %v\n", rf.me, i, time.Now().UnixMilli())
				ch <- reply

			}(i)
		}
		go func() {
			wg.Wait()
			close(ch)
		}()
		timingCh := make(chan interface{})
		go func() {
			// intervals of heartbeats should be a constant, copilot tells me
			ms := 50
			time.Sleep(time.Duration(ms) * time.Millisecond)
			timingCh <- nil
		}()
		for {
			select {
			case reply := <-ch:
				// leader do not care how many followers replied heartbeats successfully
				// leader must go ahead and send next round of heartbeats
				// In real system, context should be used to cancel goroutines
				// This lab's rpc framework does not support context cancellation
				if reply.Term > rf.currentTerm {
					// CHANGE STATE: leader -> follower
					Pf("[%d]leader's term is stale, change to follower\n", rf.me)
					rf.stateMutex.Lock()
					rf.state = STATE_FOLLOWER
					rf.leaderCancelFunc()
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.stateMutex.Unlock()
					return
				}
			case newTerm := <-rf.stateChangeToFollower:
				// CHANGE STATE: leader -> follower
				// Pf("[%d]receive AppendEntries RPC from leader, reset election timeout\n", rf.me)
				rf.stateMutex.Lock()
				rf.state = STATE_FOLLOWER
				// may be not necessary
				rf.leaderCancelFunc()
				rf.currentTerm = newTerm
				rf.votedFor = -1
				rf.stateMutex.Unlock()
				return
			case <-timingCh:
				continue loop
			}
		}
	}
}

func (rf *Raft) followerLoop() {
	Pf("[%d]become follower\n", rf.me)
	// 2. follower state: respond to RPCs from candidates and leaders, wait for election timeout
loop:
	for {
		ms := 100 + (rand.Int63() % 300)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			// CHANGE STATE: follower -> candidate
			Pf("[%d]election timeout, change state to candidate, start a new election %v\n", rf.me, time.Now().UnixMilli())
			rf.stateMutex.Lock()
			rf.state = STATE_CANDIDATE
			rf.votedFor = -1
			rf.stateMutex.Unlock()
			break loop
		case newTerm := <-rf.stateChangeToFollower:
			// remain as a follower
			rf.stateMutex.Lock()
			rf.currentTerm = newTerm
			rf.state = STATE_FOLLOWER
			rf.stateMutex.Unlock()
			// Pf("[%d]receive AppendEntries RPC from leader, reset election timeout\n", rf.me)
		}
	}
}

func (rf *Raft) candidateLoop() {
	Pf("[%d]become candidate\n", rf.me)
	// 3. candidate state: start an election and wait for votes
loop:
	for {
		rf.stateMutex.Lock()
		rf.currentTerm++
		rf.votedFor = rf.me
		currentTerm := rf.currentTerm
		lastLogIndex := len(rf.log)
		var lastLogTerm int
		if lastLogIndex > 0 {
			lastLogTerm = rf.log[lastLogIndex-1].Term
		} else {
			lastLogTerm = 0
		}
		rf.stateMutex.Unlock()
		var wg sync.WaitGroup
		ch := make(chan RequestVoteReply, len(rf.peers)-1)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(i, lastLogIndex, lastLogTerm int) {
				defer wg.Done()
				var args RequestVoteArgs
				args.Term = currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = lastLogIndex
				args.LastLogTerm = lastLogTerm
				var reply RequestVoteReply
				rf.sendRequestVote(i, &args, &reply)
				ch <- reply
			}(i, lastLogIndex, lastLogTerm)
		}
		go func() {
			wg.Wait()
			close(ch)
		}()
		votes := 1
		ms := 100 + (rand.Int63() % 300)
		received := 0
		for received < len(rf.peers)-1 {
			select {
			// receive supporter
			case reply := <-ch:
				received++
				if reply.Term > rf.currentTerm {
					// CHANGE STATE: candidate -> follower
					Pf("[%d]candidate's term is stale, change to follower\n", rf.me)
					rf.stateMutex.Lock()
					rf.state = STATE_FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.stateMutex.Unlock()
					return
				} else if reply.VoteGranted {
					votes++
					if votes >= (len(rf.peers)+1)/2 {
						// CHANGE STATE: candidate -> leader
						Pf("[%d]win the election, get %d votes(including self), total %d peers, become leader\n", rf.me, votes, len(rf.peers))
						rf.stateMutex.Lock()
						rf.state = STATE_LEADER
						rf.votedFor = -1
						rf.nextIndex = make([]int, len(rf.peers))
						// nextIndex should be initialized to leader last log index + 1
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								// not really matter
								continue
							}
							rf.nextIndex[i] = len(rf.log) + 1
						}
						rf.matchIndex = make([]int, len(rf.peers))
						rf.leaderCtx, rf.leaderCancelFunc = context.WithCancel(context.Background())
						rf.stateMutex.Unlock()
						return
					}
				}
			// receive other leader's authority
			case newTerm := <-rf.stateChangeToFollower:
				// CHANGE STATE: candidate -> follower
				// Pf("[%d]receive AppendEntries RPC from leader, reset election timeout\n", rf.me)
				// TODO: Can newTerm be smaller than currentTerm?
				rf.stateMutex.Lock()
				rf.state = STATE_FOLLOWER
				rf.votedFor = -1
				rf.currentTerm = newTerm
				rf.stateMutex.Unlock()
				return
			// election timeout, start election again
			case <-time.After(time.Duration(ms) * time.Millisecond):
				Pf("[%d]election timeout, start a new election\n", rf.me)
				continue loop
			}
		}
		// election lose, sleep a while and begin next election
		time.Sleep(time.Duration(ms) * time.Millisecond)
		Pf("[%d]lose the election, get %d votes, start a new election\n", rf.me, votes)
	}
}

func Pf(format string, a ...interface{}) (n int, err error) {
	if os.Getenv("QUIET") == "1" {
		return 0, nil // Mute the output
	}
	return fmt.Printf(format, a...) // Proceed with normal Pf
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// TODO initialize all the fields
	// TODO move persistant state to persister
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = STATE_FOLLOWER
	rf.stateChangeToFollower = make(chan int)
	// TODO read from persister
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0 // this stupid variable is 1-indexed
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

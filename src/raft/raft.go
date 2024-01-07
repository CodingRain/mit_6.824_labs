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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//  "6.5840/labgob"
	"6.5840/labgob"
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

type RaftPersistentState struct {
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	state       State      // TODO: paper do not mention this, is it necessary or not?

	// for 2D
	snapshot             []byte
	snapshotPrevLogIndex int
	snapshotPrevLogTerm  int
	indexOffset          int // index offset introduced by snapshot
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
	RaftPersistentState

	// volatile state on all servers
	commitIndex           int      // index of highest log entry known to be committed
	lastApplied           int32    // index of highest log entry applied to state machine
	stateChangeToFollower chan int // channel contains term when follower state is changed to follower
	stateMutex            sync.Mutex
	replicationMutexes    []sync.Mutex
	leaderCtx             context.Context
	leaderCancelFunc      context.CancelFunc
	stopCtx               context.Context
	stopFunc              context.CancelFunc

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
// TODO should be called with a lock? YES!
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (2C).
	// Example:
	// Pf("[%d]persist begin\n", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.state)
	e.Encode(rf.log)
	e.Encode(rf.indexOffset)
	e.Encode(rf.snapshotPrevLogIndex)
	e.Encode(rf.snapshotPrevLogTerm)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, snapshot)
	// Pf("[%d]persist ok\n", rf.me)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.state = STATE_FOLLOWER
		rf.log = make([]LogEntry, 0)
		rf.snapshot = nil
		rf.indexOffset = 0
		rf.snapshotPrevLogIndex = 0
		rf.snapshotPrevLogTerm = 0
		return
	}
	// Your code he re (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var state State
	var log []LogEntry
	var indexOffset int
	var snapshotPrevLogIndex, snapshotPrevLogTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&indexOffset) != nil ||
		d.Decode(&snapshotPrevLogIndex) != nil ||
		d.Decode(&snapshotPrevLogTerm) != nil {
		Pf("[%d]readPersist error\n", rf.me)
		os.Exit(1)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.state = state
		rf.log = log
		rf.indexOffset = indexOffset
		rf.snapshotPrevLogIndex = snapshotPrevLogIndex
		rf.snapshotPrevLogTerm = snapshotPrevLogTerm
		rf.snapshot = rf.persister.ReadSnapshot()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// TODO(2D): extra lock? effect on repliction
	rf.stateMutex.Lock()
	Pf("[%d]Snapshot begin, index=%d, log=%v, nextIndex=%v, size=%v, snapshotsize=%v\n", rf.me, index, rf.log, rf.nextIndex, rf.persister.RaftStateSize(), len(snapshot))

	// 1. disgard old entries, record prevLogIndex and prevLogTerm
	rf.snapshotPrevLogIndex, rf.snapshotPrevLogTerm = index, rf.log[rf.toLogIndex(index)-1].Term
	moveLen := index - rf.indexOffset
	newlog := make([]LogEntry, len(rf.log)-moveLen)
	copy(newlog, rf.log[moveLen:])
	rf.log = newlog
	rf.indexOffset = index

	// 2. record snapshot
	rf.snapshot = snapshot
	rf.persist(snapshot)

	Pf("[%d]Snapshot ok, index=%d, size=%v, log=%v\n", rf.me, index, rf.persister.RaftStateSize(), rf.log)
	rf.stateMutex.Unlock()
}

func (rf *Raft) toRaftIndex(logIndex int) int {
	return logIndex + rf.indexOffset
}

func (rf *Raft) toLogIndex(raftIndex int) int {
	return raftIndex - rf.indexOffset
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	if len(rf.log) == 0 {
		return rf.snapshotPrevLogIndex, rf.snapshotPrevLogTerm
	} else {
		return rf.toRaftIndex(len(rf.log)), rf.log[len(rf.log)-1].Term
	}
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
		if rf.leaderCancelFunc != nil {
			rf.leaderCancelFunc()
		}
		rf.stateChangeToFollower <- args.Term
		rf.stateMutex.Lock()
	}

	// 3. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
	// (I think the paper is not very clear about the condition, following is my idea)
	// case 1: args.Term == currentTerm, vote if not voted + log is up-to-date
	// case 2: args.Term > currentTerm, vote if log is up-to-date
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	// if current node has no log, no snapshot, then everybody must be up-to-date because lastLogIndex==lastLogTerm==0
	logUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	voteDecision := logUpToDate
	if args.Term == rf.currentTerm {
		voteDecision = voteDecision && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	}

	if voteDecision {
		rf.votedFor = args.CandidateId
		rf.persist(rf.snapshot)
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	rf.stateMutex.Unlock()
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
	// now := time.Now()
	// defer func() {
	// 	Pf("[%d]RequestVote to %d takes %v ms\n", rf.me, server, time.Since(now).Milliseconds())
	// }()
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
	// following fields are for log catchup optimization
	XTerm  int // term of conflicting entry (if any)
	XIndex int // first index it stores for term XTerm (if any)
	XLen   int // log length
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// now := time.Now()
	// defer func() {
	// 	Pf("[%d]AppendEntries to %d takes %v ms\n", rf.me, server, time.Since(now).Milliseconds())
	// }()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// what if args.term < rf.currentTerm?
	// 1. Reply false if term < currentTerm (§5.1)
	// now := time.Now()
	Pf("[%d]AppendEntries begin, args=%+v\n", rf.me, args)
	defer func() {
		rf.stateMutex.Lock()
		Pf("[%d]log=%v\n", rf.me, rf.log)
		rf.stateMutex.Unlock()
		// Pf("[%d]AppendEntries Takes=%v ms\n", rf.me, time.Since(now).Milliseconds())
	}()
	rf.stateMutex.Lock()
	reply.Term = rf.currentTerm
	reply.XLen = rf.toRaftIndex(len(rf.log))
	if args.Term < rf.currentTerm {
		// I think the semantic of "false" when using as heartbeat message is different from "false" when using as append entries message
		reply.Success = false
		rf.stateMutex.Unlock()
		return
	}
	needPersist := rf.currentTerm != args.Term || rf.votedFor != args.LeaderId
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
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
	// reject early to prevent follower from committing log entries that do not match leader's log
	rf.stateMutex.Lock()
	Pf("[%d]AppendEntries second lock get, args=%+v\n", rf.me, args)
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	Pf("[%d]AppendEntries second lock a b=%v %v, log=%v, indexOffset=%v, args=%+v\n", rf.me, lastLogIndex, lastLogTerm, rf.log, rf.indexOffset, args)
	//	[0]AppendEntries second lock a b=23 2, log=[{2 9029442931369670119} {2 8143823129118666657} {2 3721377319778763778} {2 1136064281742652622}], indexOffset=19, args=&{Term:2 LeaderId:2 PrevLogIndex:15 PrevLogTerm:2 Entries:[{Term:2 Command:3943402387094716873} {Term:2 Command:2580652933922943691} {Term:2 Command:5182391468845783217} {Term:2 Command:6050647212476567862} {Term:2 Command:9029442931369670119} {Term:2 Command:8143823129118666657} {Term:2 Command:3721377319778763778} {Term:2 Command:1136064281742652622} {Term:2 Command:2778196411272763712} {Term:2 Command:8740606257208721837} {Term:2 Command:3731926840722974793} {Term:2 Command:4287162169356798897} {Term:2 Command:4377142596258880108} {Term:2 Command:8090021872376030887}] LeaderCommit:15}

	if args.PrevLogIndex > rf.snapshotPrevLogIndex && (args.PrevLogIndex > lastLogIndex || args.PrevLogTerm != rf.log[rf.toLogIndex(args.PrevLogIndex)-1].Term) {
		// optimized log catchup
		logIndex := rf.toLogIndex(args.PrevLogIndex)
		if logIndex-1 < len(rf.log) {
			if logIndex-1 < 0 {
				Pf("pre panic logIndex -1 < 0\n")
				panic("logIndex - 1< 0\n")
			}
			reply.XTerm = rf.log[logIndex-1].Term
			for i := logIndex - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					reply.XIndex = rf.toRaftIndex(i + 1)
				} else {
					break
				}
			}
		}
		reply.Success = false
		if needPersist {
			rf.persist(rf.snapshot)
		}
		rf.stateMutex.Unlock()
		return
	}
	msgs := make([]ApplyMsg, 0)
	if len(args.Entries) > 0 {
		// 3 and 4 are exclusive logic for AppendEntries Messages
		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		if rf.toLogIndex(args.PrevLogIndex) < 0 {
			// This is a special case, the AppendEntries RPC must be out of date, just ignore it and reply with success
			// Pf("[%d]pre panic rf.toLogIndex(args.PrevLogIndex) < 0, rf=%+v, args=%+v\n", rf.me, rf, args)
			// panic("rf.toLogIndex(args.PrevLogIndex) < 0\n")
			Pf("[%d]AppendEntries special case\n", rf.me)
			reply.Success = true
			rf.stateMutex.Unlock()
			return
		}
		if args.PrevLogIndex < lastLogIndex {
			rf.log = rf.log[:rf.toLogIndex(args.PrevLogIndex)]
		}

		// 4. Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries...)
		needPersist = true
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// what is commitIndex used for? - to commit log entries to follower state machine -> change application's state
	// fmt.Println("leaderCommit=", args.LeaderCommit, "commitIndex=", rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		lastCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.toRaftIndex(len(rf.log)))
		for idx := lastCommitIndex + 1; idx <= rf.commitIndex; idx++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				CommandIndex: idx,
				Command:      rf.log[rf.toLogIndex(idx)-1].Command,
			})
		}
	}
	if needPersist {
		rf.persist(rf.snapshot)
	}
	Pf("[%d]follower ready to commit msgs, msgs=%v, lastApplied=%v\n", rf.me, msgs, atomic.LoadInt32(&rf.lastApplied))
	rf.stateMutex.Unlock()
	// Pf("[%d]AppendEntries last lock unlock, args=%+v\n", rf.me, args)
	for _, msg := range msgs {
		for {
			if int32(msg.CommandIndex) == atomic.LoadInt32(&rf.lastApplied)+1 {
				rf.applyCh <- msg
				Pf("[%d]follower committed index %d, command=%v, args=%+v\n", rf.me, msg.CommandIndex, msg.Command, args)
				atomic.AddInt32(&rf.lastApplied, 1)
				break
			}
		}
	}
	// for _, msg := range msgs {
	// 	rf.applyCh <- msg
	// 	Pf("[%d]follower committed index %d, command=%v, args=%+v\n", rf.me, msg.CommandIndex, msg.Command, args)
	// }
	reply.Success = true

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func generateRandomHexStr(length int) (string, error) {
	bytes := make([]byte, length/2)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
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
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist(rf.snapshot)
	rf.matchIndex[rf.me] = rf.toRaftIndex(len(rf.log))
	index := rf.toRaftIndex(len(rf.log))
	Pf("[%d]leader append command %v, index=%v,nextIndex=%v\n", rf.me, command, index, rf.nextIndex)
	term := rf.currentTerm
	rf.stateMutex.Unlock()
	go rf.replicateEntry(command)
	return index, term, true
}

func (rf *Raft) replicateEntry(command interface{}) {
	defer func() {
		rf.stateMutex.Lock()
		Pf("[%d]log=%v\n", rf.me, rf.log)
		rf.stateMutex.Unlock()
	}()

	var wg sync.WaitGroup
	ch := make(chan interface{})
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			threadname, _ := generateRandomHexStr(10)
			// Pf("[%d][%s]try to get replicationMutexes lock %d\n", rf.me, threadname, i)
			// rf.replicationMutexes[i].Lock()
			// Pf("[%d][%s]try to get replicationMutexes lock %d ok\n", rf.me, threadname, i)
			// Pf("[%d][%s]Enter replicateEntry Single, send to %d\n", rf.me, threadname, i)
			rf.replicateEntrySingle(i, threadname)
			// Pf("[%d][%s]Quit replicateEntry Single, send to %d\n", rf.me, threadname, i)
			// rf.replicationMutexes[i].Unlock()
			ch <- nil
		}(i)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	received := 0
	// gather response, until majority of servers append entries successfully
	for received < len(rf.peers)-1 {
		select {
		case <-ch:
			received++
			// update commitIndex, but first check if the node is still a leader (done outside of the loop)
			rf.stateMutex.Lock()
			if rf.leaderCtx.Err() != nil {
				Pf("[%d] Replication: state is not leader anymore, return\n", rf.me)
				rf.stateMutex.Unlock()
				return
			}
			lastCommitIndex := rf.commitIndex
			rf.commitIndex = rf.findCommitIndex(rf.matchIndex, rf.log, rf.currentTerm, rf.commitIndex)
			Pf("[%d]leader last commit index=%v, commitindex=%v, matchindex=%v\n", rf.me, lastCommitIndex, rf.commitIndex, rf.matchIndex)
			// one time may commit multiple entries
			msgs := make([]ApplyMsg, 0)
			for idx := lastCommitIndex + 1; idx <= rf.commitIndex; idx++ {
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					CommandIndex: idx,
					Command:      rf.log[rf.toLogIndex(idx)-1].Command,
				})
			}
			Pf("[%d]leader ready to commit msgs=%v, last applied=%v\n", rf.me, msgs, atomic.LoadInt32(&rf.lastApplied))
			rf.stateMutex.Unlock()
			// How do raft guruntee order?
			for _, msg := range msgs {
				for {
					if int32(msg.CommandIndex) == atomic.LoadInt32(&rf.lastApplied)+1 {
						rf.applyCh <- msg
						Pf("[%d]leader committed index %d, command=%v\n", rf.me, msg.CommandIndex, msg.Command)
						atomic.AddInt32(&rf.lastApplied, 1)
						break
					}
				}
			}
			// for _, msg := range msgs {
			// 	rf.applyCh <- msg
			// 	Pf("[%d]leader committed index %d, command=%v\n", rf.me, msg.CommandIndex, msg.Command)
			// }
		case <-rf.leaderCtx.Done():
			Pf("[%d]Replication(main): state is not leader anymore, return\n", rf.me)
			return
		case <-rf.stopCtx.Done():
			Pf("[%d]Replication(main): destory\n", rf.me)
			return
		}

	}
}

func (rf *Raft) replicateEntrySingle(i int, threadname string) {
	rf.stateMutex.Lock()
	var args AppendEntriesArgs
	currentTerm := rf.currentTerm
	args.Term = currentTerm
	args.LeaderId = rf.me
	rf.stateMutex.Unlock()

	var reply AppendEntriesReply
	ok := false
	// infiinte loop, mentioned by paper
	// TODO: will this cause problme in a real system?
	retryTime := 50
	nextIndex := rf.nextIndex[i]
	for retryTime > 0 && !rf.killed() {
		rf.stateMutex.Lock()
		// this check must be placed ahead, or rf.log could be changed by leader's AppendEntries RPC
		if rf.leaderCtx.Err() != nil || rf.killed() {
			Pf("[%d] Replication: state is not leader anymore or destroyed, return\n", rf.me)
			rf.stateMutex.Unlock()
			return
		}
		args.LeaderCommit = rf.commitIndex
		args.Term = rf.currentTerm
		args.PrevLogIndex = nextIndex - 1
		logIndex := rf.toLogIndex(args.PrevLogIndex)
		if logIndex > 0 {
			args.PrevLogTerm = rf.log[logIndex-1].Term
		} else {
			args.PrevLogTerm = rf.snapshotPrevLogTerm
		}
		// (2D): send snapshot if needed
		nextLogIndex := rf.toLogIndex(nextIndex) - 1
		if nextLogIndex < 0 {
			// send snapshot with retry
			retryTime := 10
			success := false
			for retryTime > 0 && !rf.killed() {
				snapshotArgs := &InstallSnapShotArgs{
					Term:              currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.snapshotPrevLogIndex,
					LastIncludedTerm:  rf.snapshotPrevLogTerm,
					Data:              rf.snapshot,
				}
				snapShotReply := &InstallSnapShotReply{}
				Pf("[%d][%s] Replication: nextIndex=%v, snapshotLastIndex=%v, need to send InstallSnapShot to %d, req=%+v\n",
					rf.me, threadname, nextIndex, rf.snapshotPrevLogIndex, i, snapshotArgs)
				rf.stateMutex.Unlock()
				ok = rf.sendInstallSnapShot(i, snapshotArgs, snapShotReply)
				rf.stateMutex.Lock()
				Pf("[%d][%s] Replication: send InstallSnapShot to %d ok, ok=%v, reply=%+v\n", rf.me, threadname, i, ok, snapShotReply)
				if ok {
					if reply.Term > currentTerm {
						Pf("[%d] Replication: find higher term: %v from [%v], self term is %v\n, change to follower, cancelfunc=%v\n", rf.me, reply.Term, i, currentTerm, rf.leaderCancelFunc == nil)
						rf.leaderCancelFunc()
						rf.stateChangeToFollower <- reply.Term
						return
					}
					success = true
					break
				}
				retryTime--
			}
			if !success {
				Pf("[%d][%s] Replication: send InstallSnapShot to %d failed, req=%+v\n", rf.me, threadname, i, args)
				// panic("send InstallSnapShot failed")
				return
			}
			nextIndex = rf.snapshotPrevLogIndex + 1
			nextLogIndex = 0
			args.PrevLogIndex, args.PrevLogTerm = rf.snapshotPrevLogIndex, rf.snapshotPrevLogTerm
		}
		args.Entries = rf.log[nextLogIndex:]
		currentMaxLogIndex := rf.toRaftIndex(len(rf.log))
		rf.stateMutex.Unlock()
		Pf("[%d][%s] Replication: send AppendEntries to %d, req=%+v\n", rf.me, threadname, i, args)
		ok = rf.sendAppendEntries(i, &args, &reply)
		Pf("[%d][%s] Replication: send AppendEntries to %d ok, ok=%v, req=%+v, reply=%+v\n", rf.me, threadname, i, ok, args, reply)
		if !ok {
			retryTime--
			continue
		}

		if reply.Success {
			rf.stateMutex.Lock()
			rf.nextIndex[i] = currentMaxLogIndex + 1
			rf.matchIndex[i] = currentMaxLogIndex
			rf.stateMutex.Unlock()
			return
		}

		if reply.Term > currentTerm {
			Pf("[%d] Replication: find higher term: %v from [%v], self term is %v\n, change to follower, cancelfunc=%v\n", rf.me, reply.Term, i, currentTerm, rf.leaderCancelFunc == nil)
			rf.leaderCancelFunc()
			rf.stateChangeToFollower <- reply.Term
			// In practice I should cancel all goroutines and rpc call, but this lab's rpc framework does not support context cancellation
			return
		}

		rf.stateMutex.Lock()
		if reply.XTerm > 0 {
			foundXTerm := false
			// optimization: binary search
			for j := len(rf.log) - 1; j >= 0; j-- {
				if rf.log[j].Term == reply.XTerm {
					nextIndex = rf.toRaftIndex(j + 1)
					foundXTerm = true
				} else if rf.log[j].Term < reply.XTerm {
					break
				}
			}
			if !foundXTerm {
				nextIndex = reply.XIndex
			}

		} else {
			nextIndex = reply.XLen + 1
		}
		// Pf("[%d][%s] Replication: updated nextIndex[%d] = %d, log=%v\n", rf.me, threadname, i, rf.nextIndex[i], rf.log)

		if nextIndex <= 0 {
			Pf("[%d][%s]pre panic nextindex[%d] <= 0\n", rf.me, threadname, i)
			panic("nextIndex[i] <= 0")
		}
		retryTime--
		rf.stateMutex.Unlock()
	}

}

func (rf *Raft) findCommitIndex(matchIndexes []int, log []LogEntry, currentTerm int, commitIndex int) int {
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
		if log[rf.toLogIndex(N)-1].Term == currentTerm {
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

type InstallSnapShotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	// offset            int    // byte offset where chunk is positioned in the snapshot file (not needed in this lab)
	// done              bool   // true if this is the last chunk (not needed in this lab)
}

type InstallSnapShotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	// 1. Reply immediately if term < currentTerm
	rf.stateMutex.Lock()
	Pf("[%d]InstallSnapShot begin, args=%+v\n", rf.me, args)
	defer rf.stateMutex.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// 2. Create a new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	rf.snapshot = args.Data
	if int32(args.LastIncludedIndex) > atomic.LoadInt32(&rf.lastApplied) {
		atomic.StoreInt32(&rf.lastApplied, int32(args.LastIncludedIndex))
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)

	// 6. If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
	if rf.snapshotPrevLogIndex == args.LastIncludedIndex && rf.snapshotPrevLogTerm == args.LastIncludedTerm {
		Pf("[%d]InstallSnapShot: snapshotPrevLogIndex and snapShotPrevLogTerm match\n", rf.me)
		rf.persist(rf.snapshot)
		return
	}
	rf.snapshotPrevLogIndex, rf.snapshotPrevLogTerm = args.LastIncludedIndex, args.LastIncludedTerm

	lastIncludedLogIndex := rf.toLogIndex(args.LastIncludedIndex)
	if lastIncludedLogIndex > 0 && lastIncludedLogIndex <= len(rf.log) && rf.log[rf.toLogIndex(args.LastIncludedIndex)-1].Term == args.LastIncludedTerm {
		Pf("[%d]InstallSnapShot: discard part of log, before=%v\n", rf.log, rf.me)
		moveLen := args.LastIncludedIndex - rf.indexOffset
		newlog := make([]LogEntry, len(rf.log)-moveLen)
		copy(newlog, rf.log[moveLen:])
		rf.log = newlog
		Pf("[%d]InstallSnapShot: discard part of log, after=%v\n", rf.log, rf.me)
		rf.indexOffset = args.LastIncludedIndex
		rf.persist(rf.snapshot)
		return
	}

	// 7. Discard the entire log
	rf.log = make([]LogEntry, 0)
	Pf("[%d]InstallSnapShot: discard all of log\n", rf.me)

	// 8. Reset state machine using snapshot contents (and load snapshot's cluster configuration)
	rf.indexOffset = args.LastIncludedIndex
	rf.persist(rf.snapshot)
	return
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
	rf.stopFunc()
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
		prevLogIndex, prevLogTerm := rf.getLastLogIndexAndTerm()
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
				Pf("[%d]is leader, try to send heartbeats to %d %v\n", rf.me, i, time.Now().UnixMilli())
				ok := rf.sendAppendEntries(i, &args, &reply)
				Pf("[%d]is leader, send heartbeats to %d ok, ok=%v %v\n", rf.me, i, ok, time.Now().UnixMilli())
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
				rf.stateMutex.Lock()
				if reply.Term > rf.currentTerm {
					// CHANGE STATE: leader -> follower
					Pf("[%d]leader's term is stale, change to follower\n", rf.me)
					rf.leaderCancelFunc()
					rf.state = STATE_FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist(rf.snapshot)
					rf.stateMutex.Unlock()
					return
				}
				rf.stateMutex.Unlock()
			case newTerm := <-rf.stateChangeToFollower:
				// CHANGE STATE: leader -> follower
				// Pf("[%d]receive AppendEntries RPC from leader, reset election timeout\n", rf.me)
				rf.stateMutex.Lock()
				rf.state = STATE_FOLLOWER
				rf.currentTerm = newTerm
				rf.votedFor = -1
				rf.persist(rf.snapshot)

				rf.stateMutex.Unlock()
				return
			case <-timingCh:
				continue loop
			case <-rf.stopCtx.Done():
				Pf("[%d]LeaderLoop: destory\n", rf.me)
				return
			}
		}
	}
}

func (rf *Raft) followerLoop() {
	Pf("[%d]become follower\n", rf.me)
	// 2. follower state: respond to RPCs from candidates and leaders, wait for election timeout
loop:
	for {
		ms := 200 + (rand.Int63() % 300)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			// CHANGE STATE: follower -> candidate
			Pf("[%d]election timeout, change state to candidate, start a new election %v\n", rf.me, time.Now().UnixMilli())
			rf.stateMutex.Lock()
			rf.state = STATE_CANDIDATE
			rf.votedFor = -1
			rf.persist(rf.snapshot)

			rf.stateMutex.Unlock()
			break loop
		case newTerm := <-rf.stateChangeToFollower:
			// remain as a follower
			rf.stateMutex.Lock()
			if rf.currentTerm != newTerm {
				rf.currentTerm = newTerm
				rf.votedFor = -1
				rf.persist(rf.snapshot)

			}
			rf.stateMutex.Unlock()
			// Pf("[%d]receive AppendEntries RPC from leader, reset election timeout\n", rf.me)
		case <-rf.stopCtx.Done():
			Pf("[%d]FollowerLoop: destory\n", rf.me)
			return
		}
	}
}

func (rf *Raft) candidateLoop() {
	Pf("[%d]become candidate\n", rf.me)
	// 3. candidate state: start an election and wait for votes
loop:
	for {
		rf.stateMutex.Lock()
		if rf.votedFor != rf.me && rf.votedFor != -1 {
			// CHANGE STATE: candidate -> follower
			rf.state = STATE_FOLLOWER
			rf.persist(rf.snapshot)
			rf.stateMutex.Unlock()
			return
		}
		rf.currentTerm++
		rf.votedFor = rf.me
		currentTerm := rf.currentTerm

		lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
		rf.persist(rf.snapshot)
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
				Pf("[%d]candidate send RequestVote to %d ok, args=%+v, reply=%+v\n", rf.me, i, args, reply)
				ch <- reply
			}(i, lastLogIndex, lastLogTerm)
			// TODO lastLogIndex, lastLogTerm 可以不传
		}
		go func() {
			wg.Wait()
			close(ch)
		}()
		votes := 1
		ms := 200 + (rand.Int63() % 300)
		received := 0
		for received < len(rf.peers)-1 {
			select {
			// receive supporter
			case reply := <-ch:
				received++
				rf.stateMutex.Lock()
				if reply.Term > rf.currentTerm {
					// CHANGE STATE: candidate -> follower
					Pf("[%d]candidate's term is stale, change to follower\n", rf.me)
					rf.state = STATE_FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist(rf.snapshot)
					rf.stateMutex.Unlock()
					return
				} else if reply.VoteGranted {
					votes++
					if votes >= (len(rf.peers)+1)/2 {
						// CHANGE STATE: candidate -> leader
						Pf("[%d]win the election, get %d votes(including self), total %d peers, become leader\n", rf.me, votes, len(rf.peers))
						rf.state = STATE_LEADER
						rf.votedFor = -1
						rf.nextIndex = make([]int, len(rf.peers))
						// nextIndex should be initialized to leader last log index + 1
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								// not really matter
								continue
							}
							rf.nextIndex[i] = rf.toRaftIndex(len(rf.log) + 1)
						}
						rf.matchIndex = make([]int, len(rf.peers))
						rf.leaderCtx, rf.leaderCancelFunc = context.WithCancel(context.Background())
						rf.persist(rf.snapshot)
						rf.stateMutex.Unlock()
						return
					}
				}
				rf.stateMutex.Unlock()
			// receive other leader's authority
			case newTerm := <-rf.stateChangeToFollower:
				// CHANGE STATE: candidate -> follower
				// Pf("[%d]receive AppendEntries RPC from leader, reset election timeout\n", rf.me)
				// TODO: Can newTerm be smaller than currentTerm?
				rf.stateMutex.Lock()
				rf.state = STATE_FOLLOWER
				rf.votedFor = -1
				rf.currentTerm = newTerm
				rf.persist(rf.snapshot)
				rf.stateMutex.Unlock()
				return
			// election timeout, start election again
			case <-time.After(time.Duration(ms) * time.Millisecond):
				Pf("[%d]election timeout, start a new election\n", rf.me)
				continue loop
			case <-rf.stopCtx.Done():
				Pf("[%d]CandidateLoop: destory\n", rf.me)
				return
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
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.stateChangeToFollower = make(chan int)
	rf.commitIndex = 0 // this stupid variable is 1-indexed
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.replicationMutexes = make([]sync.Mutex, len(peers))
	rf.stopCtx, rf.stopFunc = context.WithCancel(context.Background())
	if rf.state == STATE_LEADER {
		rf.nextIndex = make([]int, len(peers))
		// nextIndex should be initialized to leader last log index + 1
		for i := 0; i < len(peers); i++ {
			if i == rf.me {
				continue
			}
			rf.nextIndex[i] = rf.toRaftIndex(len(rf.log) + 1)
		}
		rf.matchIndex = make([]int, len(peers))
		rf.leaderCtx, rf.leaderCancelFunc = context.WithCancel(context.Background())
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

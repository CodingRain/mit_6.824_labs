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
	"fmt"
	"math/rand"
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
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term (or null if none)
	// TODO change to log entry struct
	log []int // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// volatile state on all servers
	commitIndex           int      // index of highest log entry known to be committed
	lastApplied           int      // index of highest log entry applied to state machine
	state                 State    // TODO: paper do not mention this, is it necessary or not?
	stateChangeToFollower chan int // channel contains term when follower state is changed to follower
	stateMutex            sync.Mutex

	// volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

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
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.stateMutex.Unlock()
		return
	}

	// 2. Grant vote, and change to follower if candidate's term > currentTerm
	if args.Term > rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.stateMutex.Unlock()
		rf.stateChangeToFollower <- args.Term
		return
	}

	// 3. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// (args.LastLogTerm > rf.log[len(rf.log)-1] || (args.LastLogTerm == rf.log[len(rf.log)-1] && args.LastLogIndex >= len(rf.log)-1)) {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.stateMutex.Unlock()
		return
	}

	// 4. Reply false
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.stateMutex.Unlock()
	return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// do this have tiemout?
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []int // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader's commitIndex
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
	// fmt.Printf("[%d]append entries started\n", rf.me)
	rf.stateMutex.Lock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		rf.stateMutex.Unlock()
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.stateMutex.Unlock()
	rf.stateChangeToFollower <- args.Term
	// fmt.Printf("[%d]append entries finished\n", rf.me)

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	fmt.Printf("[%d]become leader\n", rf.me)
	for {
		var wg sync.WaitGroup
		ch := make(chan AppendEntriesReply, len(rf.peers)-1)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				// TODO add other fields
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, &args, &reply)
				ch <- reply

			}(i)
		}
		go func() {
			wg.Wait()
			close(ch)
		}()
		received := 0
		for received < len(rf.peers)-1 {
			select {
			case reply := <-ch:
				received++
				if reply.Term > rf.currentTerm {
					// CHANGE STATE: leader -> follower
					fmt.Printf("[%d]leader's term is stale, change to follower\n", rf.me)
					rf.stateMutex.Lock()
					rf.state = STATE_FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.stateMutex.Unlock()
					return
				}
			case <-rf.stateChangeToFollower:
				// CHANGE STATE: leader -> follower
				// fmt.Printf("[%d]receive AppendEntries RPC from leader, reset election timeout\n", rf.me)
				rf.stateMutex.Lock()
				rf.state = STATE_FOLLOWER
				rf.votedFor = -1
				rf.stateMutex.Unlock()
				return
			}
		}

		// what's the interval between heartbeats?
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) followerLoop() {
	fmt.Printf("[%d]become follower\n", rf.me)
	// 2. follower state: respond to RPCs from candidates and leaders, wait for election timeout
loop:
	for {
		ms := 50 + (rand.Int63() % 300)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			// change to candidate
			fmt.Printf("[%d]election timeout, change state to candidate, start a new election\n", rf.me)
			rf.stateMutex.Lock()
			rf.state = STATE_CANDIDATE
			rf.votedFor = -1
			rf.stateMutex.Unlock()
			break loop
		case <-rf.stateChangeToFollower:
			// fmt.Printf("[%d]receive AppendEntries RPC from leader, reset election timeout\n", rf.me)
		}
	}
}

func (rf *Raft) candidateLoop() {
	fmt.Printf("[%d]become candidate\n", rf.me)
	// 3. candidate state: start an election and wait for votes
loop:
	for {
		rf.stateMutex.Lock()
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.stateMutex.Unlock()
		var wg sync.WaitGroup
		ch := make(chan RequestVoteReply, len(rf.peers)-1)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				var args RequestVoteArgs
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				var reply RequestVoteReply
				rf.sendRequestVote(i, &args, &reply)
				ch <- reply
			}(i)
		}
		go func() {
			wg.Wait()
			close(ch)
		}()
		votes := 1
		ms := 50 + (rand.Int63() % 300)
		received := 0
		for received < len(rf.peers)-1 {
			select {
			// receive supporter
			case reply := <-ch:
				received++
				if reply.Term > rf.currentTerm {
					// STATE CHANGE: candidate -> follower
					fmt.Printf("[%d]candidate's term is stale, change to follower\n", rf.me)
					rf.stateMutex.Lock()
					rf.state = STATE_FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.stateMutex.Unlock()
					return
				} else if reply.VoteGranted {
					votes++
					if votes >= (len(rf.peers)+1)/2 {
						// STATE CHANGE: candidate -> leader
						fmt.Printf("[%d]win the election, get %d votes, total %d peers, become leader\n", rf.me, votes, len(rf.peers))
						rf.stateMutex.Lock()
						rf.state = STATE_LEADER
						rf.votedFor = -1
						rf.stateMutex.Unlock()
						return
					}
				}
			// receive other leader's authority
			case newTerm := <-rf.stateChangeToFollower:
				// STATE CHANGE: candidate -> follower
				// fmt.Printf("[%d]receive AppendEntries RPC from leader, reset election timeout\n", rf.me)
				rf.stateMutex.Lock()
				rf.state = STATE_FOLLOWER
				rf.votedFor = -1
				rf.currentTerm = newTerm
				rf.stateMutex.Unlock()
				return
			// election timeout, start election again
			case <-time.After(time.Duration(ms) * time.Millisecond):
				fmt.Printf("[%d]election timeout, start a new election, %v\n", rf.me, time.Now().UnixMilli())
				continue loop
			}
		}
		fmt.Printf("[%d]lose the election, get %d votes, start a new election\n", rf.me, votes)

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
	// 其他暂时不用管

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

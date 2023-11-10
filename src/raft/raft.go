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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	ELECTION_TIMEOUT_MIN = 300
	ELECTION_TIMEOUT_MAX = 500
	HEARBEAT             = 200 * time.Millisecond

	FOLLOWER  = 1
	LEADER    = 2
	CANDIDATE = 3
)

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
	// Persistent state on all servers
	currentTerm int // latest term server has seen - init to 0
	votedFor    int //candidate ID that received vote in current term
	log         []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int
	state       int

	timer *time.Ticker

	//Volatile state on leaders
	nextIndex       []int
	matchIndex      []int
	votesReceived   int
	applyCond       *sync.Cond
	electionTimeOut time.Duration
	// lastHeardFromPeer time.Time
	applyChannel chan ApplyMsg

	// Snapshots part
	lastIncludedIndex int
	lastIncludedTerm  int
	snapShotCmd       []byte
}

var HeartBeatTimeOut = 120 * time.Millisecond

type AppendEntriesArgs struct {
	Term         int // leaders term
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // leader's commit index
	LogIndex     int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	AppendErr     AppendEntriesError
	NotMatchIndex int
}

type InstallSnapshotRequest struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotErr int64

const (
	InstallSnapshotErr_Nil InstallSnapshotErr = iota
	InstallSnapshotErr_ReqOutOfDate
	InstallSnapshotErr_OldIndex
)

type InstallSnapshotResponse struct {
	Term int
	Err  InstallSnapshotErr
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	data := buf.Bytes()
	rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	buf := bytes.NewBuffer(data)
	de := labgob.NewDecoder(buf)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	if de.Decode(&currentTerm) != nil || de.Decode(&votedFor) != nil || de.Decode(&log) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	rf.mu.Lock()
	if len(rf.log)+rf.lastIncludedIndex > lastIncludedIndex {
		return false
	}
	rf.snapShotCmd = snapshot
	rf.log = []LogEntry{}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}
	ind := index - rf.lastIncludedIndex - 1
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[ind].Term
	rf.log = rf.log[ind+1:]
	rf.snapShotCmd = snapshot
}

type VoteError int64

const (
	Nil VoteError = iota
	VoteReqOutOfDate
	CandidateLogTooOld
	VotedThisTerm
	RaftKilled
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // ID of the candidate requesting vote.
	LastLogIndex int // index of candidate's last log entry.
	LastLogTerm  int // term of candidate's last log entry.
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current term for candidate to update itself
	VoteGranted bool // true means candidate received the vote.
	VoteErr     VoteError
}

type AppendEntriesError int64

const (
	AppendErr_Nil AppendEntriesError = iota
	AppendErr_LogsNotMatch
	AppendErr_ReqOutOfDate
	AppendErr_ReqRepeat
	AppendErr_Committed
	AppendErr_RaftKilled
)

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// How to handle incoming  vote rqst from another peer
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		reply.VoteErr = RaftKilled
		return
	}
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		// If the candidates term is lesser than my term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		reply.VoteErr = VoteReqOutOfDate
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		// If candidate is at an advanced term, convert to follower and update my term
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	// Is the candidate's last log term < my last log term or
	candidateLogOld := args.LastLogTerm < rf.lastIncludedTerm || (len(rf.log) > 0 && args.LastLogTerm < rf.log[len(rf.log)-1].Term)
	shortLogOfC := (args.LastLogIndex < rf.lastIncludedIndex) || (len(rf.log) > 0 && args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)+rf.lastIncludedIndex)

	// If candidate's last log entry has older term or if candidate's log is shorter than my log
	if candidateLogOld || shortLogOfC {
		// Update my term but dont grant vote
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranted = false
		reply.VoteErr = CandidateLogTooOld
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if args.Term == rf.currentTerm {
		reply.Term = args.Term
		if rf.votedFor == args.CandidateID {
			rf.state = FOLLOWER
			rf.timer.Reset(rf.electionTimeOut)
			reply.VoteGranted = true
			reply.VoteErr = VotedThisTerm
			rf.mu.Unlock()
			return
		}
		if rf.votedFor != -1 {
			reply.VoteGranted = false
			reply.VoteErr = VotedThisTerm
			rf.mu.Unlock()
			return
		}
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateID
	rf.state = FOLLOWER
	rf.timer.Reset(rf.electionTimeOut)

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	reply.VoteErr = Nil
	rf.persist()
	rf.mu.Unlock()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) bool {
	// Send RPCs to each of the peers except self & count the total number of votes received.
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// Repeatedly send the vote request to a server
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			break
		}
	}
	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	// expired rqst
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	// Based on response from peer determine election
	switch reply.VoteErr {
	case VoteReqOutOfDate, CandidateLogTooOld:
		// Become follower & update my term if outdated
		rf.mu.Lock()
		rf.state = FOLLOWER
		rf.timer.Reset(rf.electionTimeOut)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	case Nil, VotedThisTerm:
		rf.mu.Lock()
		// If votegranted in curr term & no decisive vote count reached yet.
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNum <= len(rf.peers)/2 {
			*voteNum++
		}
		// I received majority of the votes
		// Becme leader, reset nextIndex
		if *voteNum > len(rf.peers)/2 {
			*voteNum = 0
			if rf.state == LEADER {
				rf.mu.Unlock()
				return ok
			}
			rf.state = LEADER
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex + 1
			}
			rf.timer.Reset(HeartBeatTimeOut)
		}
		rf.mu.Unlock()
	case RaftKilled:
		return false
	}
	return ok
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

	if rf.killed() {
		return index, term, isLeader
	}
	rf.mu.Lock()
	isLeader = rf.state == LEADER
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	logEntry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, logEntry)

	index = len(rf.log) + rf.lastIncludedIndex
	term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
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
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			currStatus := rf.state
			switch currStatus {
			case FOLLOWER:
				rf.state = CANDIDATE
				fallthrough
			case CANDIDATE:
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.electionTimeOut = time.Duration(rand.Intn(150)+200) * time.Millisecond
				voteNum := 1
				rf.persist()
				rf.timer.Reset(rf.electionTimeOut)
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}
					voteArgs := &RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateID:  rf.me,
						LastLogIndex: len(rf.log) + rf.lastIncludedIndex,
						LastLogTerm:  rf.lastIncludedTerm,
					}
					if len(rf.log) > 0 {
						voteArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
					}
					voteReply := new(RequestVoteReply)
					go rf.sendRequestVote(i, voteArgs, voteReply, &voteNum)
				}
			case LEADER:
				appendNum := 1
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}
					appendEntriesArgs := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderID:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
						LogIndex:     len(rf.log) + rf.lastIncludedIndex,
					}
					//installSnapshot，如果rf.nextIndex[i]小于等lastCludeIndex,则发送snapShot
					if rf.nextIndex[i] <= rf.lastIncludedIndex {
						installSnapshotReq := &InstallSnapshotRequest{
							Term:              rf.currentTerm,
							LeaderID:          rf.me,
							LastIncludedIndex: rf.lastIncludedIndex,
							LastIncludedTerm:  rf.lastIncludedTerm,
							Data:              rf.snapShotCmd,
						}
						installSnapshotReply := &InstallSnapshotResponse{}
						//fmt.Println("installsnapshot", rf.me, i, rf.lastIncludeIndex, rf.lastIncludeTerm, rf.currentTerm, installSnapshotReq)
						go rf.sendInstallSnapshot(i, installSnapshotReq, installSnapshotReply)
						continue
					}
					for rf.nextIndex[i] > rf.lastIncludedIndex {
						appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1
						if appendEntriesArgs.PrevLogIndex >= len(rf.log)+rf.lastIncludedIndex+1 {
							rf.nextIndex[i]--
							continue
						}
						if appendEntriesArgs.PrevLogIndex == rf.lastIncludedIndex {
							appendEntriesArgs.PrevLogTerm = rf.lastIncludedTerm
						} else {
							appendEntriesArgs.PrevLogTerm = rf.log[appendEntriesArgs.PrevLogIndex-rf.lastIncludedIndex-1].Term
						}
						break
					}
					if rf.nextIndex[i] < len(rf.log)+rf.lastIncludedIndex+1 {
						appendEntriesArgs.Entries = make([]LogEntry, appendEntriesArgs.LogIndex+1-rf.nextIndex[i])
						copy(appendEntriesArgs.Entries, rf.log[rf.nextIndex[i]-rf.lastIncludedIndex-1:appendEntriesArgs.LogIndex-rf.lastIncludedIndex])
					}

					appendEntriesReply := new(AppendEntriesReply)
					go rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply, &appendNum)
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Term = -1
		reply.AppendErr = AppendErr_RaftKilled
		reply.Success = false
		return
	}

	rf.mu.Lock()
	// sender at older term or sender log last idx is less than snapshotted index of mine.
	if args.Term < rf.currentTerm || args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.AppendErr = AppendErr_ReqOutOfDate
		reply.NotMatchIndex = -1
		rf.mu.Unlock()
		return
	}

	rf.state = FOLLOWER
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderID
	rf.timer.Reset(rf.electionTimeOut)

	// Indexes of snapshotted part dont match
	if (args.PrevLogIndex != rf.lastIncludedIndex && (args.PrevLogIndex >= len(rf.log)+rf.lastIncludedIndex+1 || args.PrevLogTerm != rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term)) || (args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.AppendErr = AppendErr_LogsNotMatch
		reply.NotMatchIndex = rf.lastApplied + 1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// my commit index > candidates last log idx,
	// ive committed too many and candidate lags behind
	if rf.lastApplied > args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.AppendErr = AppendErr_Committed
		reply.NotMatchIndex = rf.lastApplied + 1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// If its not a heartbeat then:
	if args.Entries != nil {
		// Discard the uncommitted log entries that r extra than candidates len
		rf.log = rf.log[:args.PrevLogIndex-rf.lastIncludedIndex]
		rf.log = append(rf.log, args.Entries...)
	}
	// My commit index is behind candidate's
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command,
		}
		rf.applyChannel <- applyMsg
		rf.commitIndex = rf.lastApplied
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.AppendErr = AppendErr_Nil
	reply.NotMatchIndex = -1
	rf.persist()
	rf.mu.Unlock()
	return
}

// Sent by leader only
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNum *int) bool {
	if rf.killed() {
		return false
	}
	// Send appendentry to peer & repeatedly send it if not heard back from it
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			break
		}
	}

	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	// Expired?
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	}

	// Based on the reply from the peer for AppendEntry
	switch reply.AppendErr {
	case AppendErr_Nil:
		// got success in same term & majoirty of peers haven't appended the entry yet
		if reply.Success && reply.Term == rf.currentTerm && *appendNum <= len(rf.peers)/2 {
			*appendNum++
		}
		if rf.nextIndex[server] > args.LogIndex+1 {
			rf.mu.Unlock()
			return ok
		}
		rf.nextIndex[server] = args.LogIndex + 1
		// Majority of peers appended the entry to their logs
		if *appendNum > len(rf.peers)/2 {
			*appendNum = 0
			if (args.LogIndex > rf.lastIncludedIndex && rf.log[args.LogIndex-rf.lastIncludedIndex-1].Term != rf.currentTerm) ||
				(args.LogIndex == rf.lastIncludedIndex && rf.lastIncludedTerm != rf.currentTerm) {
				rf.mu.Unlock()
				return false
			}
			// if any of the log entries are not applied to state machine
			for rf.lastApplied < args.LogIndex {
				// Create a msg out of log entry & Write the entry to channel
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.applyChannel <- applyMsg
				rf.commitIndex = rf.lastApplied
			}
		}
	case AppendErr_LogsNotMatch:
		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return false
		}
		rf.nextIndex[server] = reply.NotMatchIndex
	case AppendErr_ReqOutOfDate:
		// Become follower
		rf.state = FOLLOWER
		rf.timer.Reset(rf.electionTimeOut)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}
	case AppendErr_ReqRepeat:
		if reply.Term > rf.currentTerm {
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.timer.Reset(rf.electionTimeOut)
			rf.persist()
		}
	case AppendErr_Committed:
		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return false
		}
		rf.nextIndex[server] = reply.NotMatchIndex
	case AppendErr_RaftKilled:
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	return ok
}

// how to deal with incoming InstallSnapshotRPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotResponse) {
	if rf.killed() {
		reply.Term = args.Term
		return
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Err = InstallSnapshotErr_ReqOutOfDate
		rf.mu.Unlock()
		return
	}
	// Last included idx = is the idx at which snapshot finished & saved
	// if I snapshotted farther than the candidate i dont install snapshot
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Err = InstallSnapshotErr_OldIndex
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderID
	rf.state = FOLLOWER
	rf.timer.Reset(rf.electionTimeOut)

	// candidate snapshotted farther than my old snapshot + my curr log
	// then I install their snapshot n reset my log as they might hv captured
	// all details
	if len(rf.log)+rf.lastIncludedIndex <= args.LastIncludedIndex {
		rf.log = []LogEntry{}
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
	} else {
		// only install the parts that are the diff bw last snapshot indices
		rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
	}
	rf.applyChannel <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = rf.lastApplied

	reply.Term = rf.currentTerm
	reply.Err = InstallSnapshotErr_Nil
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		if ok {
			break
		}
	}
	if rf.killed() {
		return false
	}

	rf.mu.Lock()
	if reply.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	}
	switch reply.Err {
	case InstallSnapshotErr_Nil:
		if reply.Term > rf.commitIndex {
			// become follower
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.timer.Reset(rf.electionTimeOut)
			rf.persist()
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	case InstallSnapshotErr_OldIndex:
		if reply.Term > rf.currentTerm {
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.timer.Reset(rf.electionTimeOut)
			rf.persist()
		}
		rf.nextIndex[server] = len(rf.log) + rf.lastIncludedIndex + 1
	case InstallSnapshotErr_ReqOutOfDate:
	}
	rf.mu.Unlock()
	return false
}

func (rf *Raft) applier(applyChan chan ApplyMsg) {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			// // For 2D:
			// SnapshotValid bool
			// Snapshot      []byte
			// SnapshotTerm  int
			applyChan <- ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied, false, nil, -1, -1}
		}
		rf.mu.Unlock()
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
	rf.state = FOLLOWER
	rf.votedFor = -1
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeOut = time.Duration(rand.Intn(150)+200) * time.Millisecond
	rf.currentTerm, rf.commitIndex, rf.lastApplied = 0, 0, 0
	rf.nextIndex, rf.matchIndex, rf.log = nil, nil, []LogEntry{{0, nil}}
	rf.timer = time.NewTicker(rf.electionTimeOut)
	rf.applyChannel = applyCh

	// 2D
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = 0
	rf.snapShotCmd = make([]byte, 0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

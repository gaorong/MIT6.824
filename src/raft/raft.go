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
	"labrpc"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"
const (
	constElectionBurstMs  = 100
	constElectionTimeout  = 500 * time.Millisecond
	constHeartbeatTimeout = 100 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Command struct {
	term    int
	command string
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // TODO: protect by lock
	votedFor    int
	log         []Command

	commitIndex int
	lastApplied int

	isLeader       bool // TODO: protect by lock
	electionTimout time.Duration
	done           chan struct{} // done inidicas thi instance has been killed

	nextIndex  []int
	matchIndex []int

	heartbeatChan  chan struct{}
	heatbeatTicker *time.Ticker
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()
	if term == -1 {
		return term, false
	}
	DPrintf("%d is leader: %v", rf.me, rf.isLeader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("%d get vote request from %d, arg: %v", rf.me, args.CandidateId, args)
	if args.Term <= rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.mu.Lock()
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.isLeader = false
	rf.mu.Unlock()
	rf.heartbeatChan <- struct{}{}

	reply.Term = args.Term
	reply.VoteGranted = true
	DPrintf("%d approve vote from %d", rf.me, args.CandidateId)
}

func (rf *Raft) sendRequestVote() {
	voteGrantedCh := make(chan bool)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			reply := &RequestVoteReply{}

			callResult := make(chan bool)
			go func() {
				DPrintf("%d send vote request to %d, args: %+v", rf.me, index, args)
				callResult <- rf.peers[index].Call("Raft.RequestVote", args, reply)
			}()
			select {
			case r := <-callResult:
				if r && reply.VoteGranted {
					voteGrantedCh <- true
				}
			case <-time.After(constHeartbeatTimeout):
				DPrintf("%d send vote request to peer %d timeout", rf.me, index)
			}
		}(i)

	}

	var voteGrantedNum int
	select {
	case <-time.After(constHeartbeatTimeout):
		DPrintf("%d elect leader failed", rf.me)
		return
	case <-voteGrantedCh:
		voteGrantedNum++
		if voteGrantedNum > len(rf.peers)/2+1 {
			break
		}
	}

	DPrintf("%d request vote success, start leading", rf.me)
	rf.isLeader = true
	rf.sendAppendEntries()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	// Entries      []Command
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%d get append entry from leader %d: %v", rf.me, args.LeaderId, args)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// reset timer
	rf.isLeader = false
	rf.heartbeatChan <- struct{}{}

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// todo other things managing stat machine
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries() {
	for {
		if !rf.isLeader {
			return
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				args := &AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				reply := &AppendEntriesReply{}
				DPrintf("%d send append request to peer %d: %+v time: %s", rf.me, i, args, time.Now().String())
				r := rf.peers[i].Call("Raft.AppendEntries", args, reply)
				if !r {
				}
			}(i)
		}
		select {
		case <-rf.done:
			return
		case <-rf.heatbeatTicker.C:
		}
	}

}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.done)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = -1
	rf.votedFor = -1
	rf.isLeader = false
	rf.heartbeatChan = make(chan struct{})
	rf.done = make(chan struct{})
	rf.electionTimout = RandomTimeout(constElectionTimeout, constElectionBurstMs)
	DPrintf("%d election timeout is %v", rf.me, rf.electionTimout)
	rf.heatbeatTicker = time.NewTicker(constHeartbeatTimeout)

	// Your initialization code here (2A, 2B, 2C).
	electionTimer := time.NewTimer(rf.electionTimout)
	go func() {
		for {
			electionTimer.Stop()
			electionTimer.Reset(rf.electionTimout)
			select {
			case <-rf.done:
				DPrintf("%d finished...", rf.me)
				return
			case <-electionTimer.C: // election timeout, start new election and become leader if success
				if rf.isLeader {
					break
				}
				DPrintf("%d election timeout, %v", rf.me, time.Now())
				rf.mu.Lock()
				rf.votedFor = rf.me
				rf.currentTerm = rf.currentTerm + 1
				rf.electionTimout = RandomTimeout(constElectionTimeout, constElectionBurstMs)
				rf.mu.Unlock()
				go rf.sendRequestVote()

			case <-rf.heartbeatChan:
				DPrintf("%d reset timer casue get append entry to %s", rf.me, time.Now().Add(rf.electionTimout).String())
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

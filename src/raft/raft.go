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
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const(
	masterState = iota
	condidateState
	followerState
)

const heartBeatTimer = 100*time.Millisecond

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

//log entries struct
type LogEntry struct {
	LogIndex   int
	LogTerm    int
	LogCommand interface{}
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state int
	currentTerm int
	votedFor int
	electChan chan bool
	heartBeatchan chan bool
	chanCommit chan bool
	electionTimer *time.Timer
	appendEntriesTimer *time.Timer
	stopCh              chan struct{}
	chanApply chan ApplyMsg
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	logs []LogEntry
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	if(rf.state == masterState){
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	// Your code here (2A).
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CondidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VotedFor bool
}

//appendEntries RPC struct
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextIndex int
}

//tu
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("%d received requested vote from %d,args.term:%d,my term %d\n",rf.me,args.CondidateId,args.Term,rf.currentTerm)
	reply.VotedFor = false
	reply.Term = rf.currentTerm
	if args.Term<rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = followerState
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	term := rf.getLastTerm()
	index := rf.getLastIndex()
	uptoDate := false

	//If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	//Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms,then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date
	if args.LastLogTerm > term {
		uptoDate = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index {
		// at least up to date
		uptoDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CondidateId) && uptoDate {
		state:=rf.state
		rf.becomefollower()
		DPrintf("server:%d state change from %d to %d",rf.me,state,rf.state)
		reply.VotedFor = true
		rf.votedFor = args.CondidateId
		rf.electionTimer.Reset(getRandomElectionTimer())
	}
	/*if args.Term > term {
		reply.VotedFor = true
		rf.votedFor = condidateId
		rf.currentTerm = args.Term
		state:=rf.state
		rf.becomefollower()
		DPrintf("server:%d state change from %d to %d",rf.me,state,rf.state)
		rf.electionTimer.Reset(getRandomElectionTimer())
		return
	}*/
	/*if term == args.Term  {
		if args.CondidateId == rf.votedFor {
			reply.votedFor = true
			rf.becomefollower()
			rf.electionTimer.Reset(heartBeatTimer)
			return
		}
	}*/
}

func (rf *Raft) getLastIndex() int {
	return rf.logs[len(rf.logs) - 1].LogIndex
}
func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs) - 1].LogTerm
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%d send request vote to %d,term:%d",rf.me,server,args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("%d to %d requestVote RPC failed\n",rf.me,server)
		return false
	}
	ok = reply.VotedFor
	if ok {
		DPrintf("%d receive vote from %d",rf.me,server)
	} else {
		DPrintf("%d receive refused vote from %d",rf.me,server)
	}
	rf.mu.Lock()
	if reply.Term>rf.currentTerm {
		rf.currentTerm =reply.Term
		rf.persist()
	}
	rf.mu.Unlock()
	return ok
}

func (rf * Raft) StartElection() {
	if rf.state!=condidateState {
		return
	}
	votedNum := 1
	done := false;
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.currentTerm++
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	rf.electionTimer.Reset(getRandomElectionTimer())
	lastLogIndex := rf.getLastIndex()
	lastLogTerm := rf.getLastTerm()
	DPrintf("%d start election,term:%d",rf.me,currentTerm)
	for i:=0;i<len(rf.peers);i++{
		if i==rf.me{
			continue
		}
		go func(numPeer int) {
			request := new(RequestVoteArgs)
			rf.makeRequestVoteArgs(request,currentTerm,lastLogIndex,lastLogTerm)
			reply := new(RequestVoteReply)
			ok:=rf.sendRequestVote(numPeer,request,reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()
			votedNum++
			if done || votedNum<=len(rf.peers)/2{
				return
			}
			if rf.currentTerm != currentTerm || rf.state != condidateState {
				return
			}
			done = true
			DPrintf("[%d] become leader in term %d",rf.me,currentTerm)
			rf.becomeLeader()

			for i:=0;i<len(rf.peers);i++ {
				rf.nextIndex[i]=rf.getLastIndex()+1
				rf.matchIndex[i]=0
			}
			//rf.persist()
			go rf.broadCastAppendEntries()
		}(i)
	}
}

func (rf *Raft)AppendEntries(args *AppendEntriesArgs,reply * AppendEntriesReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("%d received from %d appendEntries ,term:%d",rf.me,args.LeaderId,rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.Success = false
	if(args.Term<rf.currentTerm){
		reply.Success = false
 	} else {
		if args.PreLogIndex > rf.getLastIndex() {
			reply.Success = false
			reply.NextIndex = rf.getLastIndex() + 1
			return
		}
		baseIndex := rf.logs[0].LogIndex
		if args.PreLogIndex > baseIndex {
			term := rf.logs[args.PreLogIndex - baseIndex].LogTerm
			if args.PreLogTerm != term {
				for i := args.PreLogIndex - 1; i >= baseIndex; i-- {
					if rf.logs[i - baseIndex].LogTerm != term {
						reply.NextIndex = i + 1
						break
					}
				}
				return
			}
		}
		if args.PreLogIndex < baseIndex {

		} else {
			//DPrintf("%d receive appendEntries from index %d to index %d",rf.me,rf.logs[len(rf.logs)-1].LogIndex,args.Entries[len(args.Entries)-1].LogIndex)
			rf.logs = rf.logs[: args.PreLogIndex + 1 - baseIndex]
			rf.logs = append(rf.logs, args.Entries...)
			reply.Success = true
			reply.NextIndex = rf.getLastIndex() + 1
		}

		if args.LeaderCommit > rf.commitIndex {
			last := rf.getLastIndex()
			if args.LeaderCommit > last {
				rf.commitIndex = last
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.chanCommit <- true
		}

		reply.Success = true
		state:=rf.state
		rf.state = followerState
		rf.electionTimer.Reset(getRandomElectionTimer())
		DPrintf("server:%d,state change from %d to %d",rf.me,state,rf.state)
	}
}

func (rf *Raft) broadCastAppendEntries()  {
	rf.mu.Lock()
	DPrintf("%d,my rf state %d",rf.me,rf.state)

	if rf.state != masterState {
		rf.state = followerState
		rf.electionTimer.Reset(getRandomElectionTimer())
		rf.mu.Unlock()
		return
	}
	rf.electionTimer.Reset(getRandomElectionTimer())
	term := rf.currentTerm
	rf.appendEntriesTimer.Reset(heartBeatTimer)
	N := rf.commitIndex
	last := rf.getLastIndex()
	baseIndex := rf.logs[0].LogIndex
	//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.logs[i - baseIndex].LogTerm == rf.currentTerm {
				num++
			}
		}
		if 2 * num > len(rf.peers) {
			N = i
		}
	}
	if N != rf.commitIndex {
		DPrintf("leader commit index from %d to %d",rf.commitIndex,N)
		rf.commitIndex = N
		rf.chanCommit <- true
	}
	DPrintf("leader:%d start append entries,term:%d",rf.me,rf.currentTerm)
	rf.mu.Unlock()
	for i:=0;i<len(rf.peers);i++ {
		if i == rf.me {
			continue
		}
		args := new(AppendEntriesArgs)
		reply := new(AppendEntriesReply)
		rf.mu.Lock()
		args.LeaderId = rf.me
		args.Term = term
		args.PreLogIndex= rf.nextIndex[i]-1
		args.PreLogTerm = rf.logs[args.PreLogIndex - baseIndex].LogTerm
		args.Entries = make([]LogEntry,len(rf.logs[args.PreLogIndex+1:]))
		copy(args.Entries,rf.logs[args.PreLogIndex+1:])
		args.LeaderCommit = rf.commitIndex
		//rf.persist()
		rf.mu.Unlock()
		go func(peerNum int,args *AppendEntriesArgs) {
			rf.sendAppendEntries(peerNum,args,reply)
		}(i,args)
	}

}

func (rf *Raft)sendAppendEntries(serverId int,args *AppendEntriesArgs,reply * AppendEntriesReply)bool {
	rf.mu.Lock()
	if rf.state != masterState {
		rf.mu.Unlock()
		return false
	}
	DPrintf("%d send appendEntries to %d,term:%d",rf.me,serverId,rf.currentTerm)
	rf.mu.Unlock()
	ok := rf.peers[serverId].Call("Raft.AppendEntries",args,reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if !ok {
		DPrintf("%d to %d appendEntries RPC failed",rf.me,serverId)
		return ok
	}
	if ok {
		if rf.state != masterState {
			return false
		}
		/*if args.Term != rf.currentTerm {
			return ok
		}*/

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			state:=rf.state
			rf.becomefollower()
			DPrintf("server:%d,state chage from %d to %d",rf.me,state,rf.state)
			rf.electionTimer.Reset(getRandomElectionTimer())
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				nextIndex := rf.nextIndex[serverId]
				rf.nextIndex[serverId] = args.Entries[len(args.Entries) - 1].LogIndex + 1
				DPrintf("server %d succeed to send appendEntry to %d from index %d to index %d",rf.me,serverId,nextIndex,rf.nextIndex[serverId])
				//reply.NextIndex
				//rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[serverId] = rf.nextIndex[serverId] - 1
			}
		} else {
			rf.nextIndex[serverId] = reply.NextIndex
		}
	}
	return ok
	/*if(ok){
		DPrintf("%d,failed to send request vote to %d",rf.me,serverId)
	} else {

	}*/
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == masterState
	if isLeader {
		DPrintf("client append log:%v",command)
		index = rf.getLastIndex() + 1
		rf.logs = append(rf.logs, LogEntry{LogTerm:term, LogCommand:command, LogIndex:index})
		rf.persist()
	}
	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	close(rf.stopCh)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electChan = make(chan bool)
	rf.heartBeatchan = make(chan bool)
	rf.chanCommit = make(chan bool)

	rf.stopCh = make(chan struct{})
	rf.state = followerState
	rand.Seed(time.Now().UnixNano())
	rf.electionTimer = time.NewTimer(getRandomElectionTimer())
	rf.appendEntriesTimer = time.NewTimer(10*time.Second)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int,len(peers))
	rf.matchIndex = make([]int,len(peers))
	rf.logs = append(rf.logs,LogEntry{LogTerm: 0,LogIndex: 0})
	rf.chanApply = applyCh
	rf.readPersist(persister.ReadRaftState())

	/*go func() {
		for {
			DPrintf("%d",rf.state)
			switch rf.state {
			case followerState:
				select {
				case <-rf.electionTimer.C:
					rf.becomCondidate()
				case <-rf.stopCh:
					return
				}
			case condidateState:
				 go rf.StartElection()
				select {
				case <-rf.stopCh:
					return
				 }
			case masterState:
				select {
				case <-rf.heartBeatchan:
					go rf.broadCastAppendEntries()

				case <-rf.appendEntriesTimer.C:
					go rf.broadCastAppendEntries()
				case <-rf.stopCh:
					return
				}

			}
		}
	}()*///架构发生冲突 舍弃

	// 发起投票
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.becomCondidate()
				rf.StartElection()
			}
		}
	}()

	// leader 发送日志
	go func() {
		for  {
			select {
			case <-rf.stopCh:
				return
			case <-rf.appendEntriesTimer.C:
				rf.broadCastAppendEntries()
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.chanCommit:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex := rf.logs[0].LogIndex
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{CommandIndex: i, Command: rf.logs[i - baseIndex].LogCommand,CommandValid: true}
					rf.chanApply <- msg
					//fmt.Printf("me:%d %v\n",rf.me,msg)
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()


	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash

	return rf
}

func (rf *Raft) becomCondidate () {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = condidateState
	rf.persist()
}

func (rf *Raft)becomefollower()  {
	rf.state = followerState
}

func (rf *Raft)becomeLeader() {
	rf.state=masterState
}

func (rf * Raft)makeRequestVoteArgs(args *RequestVoteArgs,term int,lastLogIndex int,lastLogTerm int)  {
	args.CondidateId=rf.me
	args.Term = term
	args.LastLogTerm = lastLogTerm
	args.LastLogIndex = lastLogIndex
}

func getRandomElectionTimer() time.Duration  {
	 return time.Duration(rand.Int63n(300-150) + 150)*time.Millisecond
}

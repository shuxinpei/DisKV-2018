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
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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

//m log
type LogEntry struct {
	LogId 		int
	LogIndex	int
	LogTerm		int
	Command	interface{}
}

//m Append Entries
type AppendEntriesArgs struct {
	Term 			int
	LeaderId		int
	PrevLogIndex	int
	PreLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct{
	Term 			int
	Success 		bool

	//额外机制
}

type LogReplicationReply struct {
	Index 			int
	*AppendEntriesReply
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoteGranted bool
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	isLeader  bool

	resetTimer			chan struct{}
	electionTimer  		*time.Timer
	electionTimeOut		time.Duration
	heartBeatInterval	time.Duration

	//persistent state on all servers
	currentTerm 	int	// candidateId that received vote in current term (or null if none)
	votedFor		int //
	logs			[]LogEntry

	//Volatile state on all servers
	commitIndex 	int		//index of highest log entry has committed
	lastApplied		int		//highest log entry has applied

	//Volatile state on leaders, reinitialized after election
	nextIndex 		[]int	//next index of log of each peers to send the log
	matchIndex		[]int	//for each server's highest log entry known to be replicated
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.votedFor == rf.me
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
	term := 0
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		if _, isLeader := rf.GetState(); isLeader{
			defer rf.mu.Unlock()
			return index, term, false
		}
		log := LogEntry{
			LogId:		rand.Intn(1e8) + 1e8,
			LogIndex:	len(rf.logs),
			LogTerm:	rf.currentTerm,
			Command:	command,
		}
		rf.logs = append(rf.logs, log)
		//update itself's match index
		rf.nextIndex[rf.me] = len(rf.logs) + 1
		rf.matchIndex[rf.me] = len(rf.logs)

		// go send msg to followers and
		// get if majority of follower get the msg and add it to its' msg
		go rf.LogReplication(log)
		rf.mu.Unlock()
	}
	return len(rf.logs) -1 , term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//------------选举
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term <= rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//只有在满足拥有更适合的选举者的时候才会进行重置时间
	//日志约束条件, log term > rf.lastlog.term || term 相等，但是log index 大于 rf.last log term
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].LogTerm
	if args.LastLogTerm > lastLogTerm ||
		args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		
		rf.resetTimer <- struct{}{}
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
	DPrintf("%v send request vote to %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Election() {
	DPrintf("peer %v start election", rf.me)
	var voteargs RequestVoteArgs
	rf.fillVoteRequestArgs(&voteargs)
	replys := make(chan RequestVoteReply, len(rf.peers))
	wg := sync.WaitGroup{}
	for idx := range rf.peers{
		if idx == rf.me{
			rf.resetTimer <- struct{}{}
		} else {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				var reply RequestVoteReply
				if rf.sendRequestVote(idx, &voteargs, &reply){
					replys <- reply
				}
			}(idx)
		}
	}
	go func() {
		wg.Wait()
		close(replys)
	}()
	//如果有消息回送就会得到，循环直到chan关闭-->解决消息不用全部送达的问题
	//term 的更新不依赖这次发送，只负责是否能够当选
	var supportCount int
	for reply := range replys{
		if reply.VoteGranted{
			DPrintf("peer %d : get voted reply, reply: %v", rf.me, reply)
			if supportCount++; supportCount > len(rf.peers)/2 - 1 {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("peer %d : win", rf.me)
				rf.votedFor = rf.me
				go rf.HeartBeatDaemon()
				return
			}
		}
		//TODO
		//总觉有点问题
		if reply.Term > rf.currentTerm{
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.votedFor = -1
			return
		}
	}
}

func (rf *Raft) fillVoteRequestArgs(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1

	args.Term			=	rf.currentTerm
	args.LastLogIndex	=	len(rf.logs) -1
	args.LastLogTerm	=	rf.logs[args.LastLogIndex].LogTerm
	args.CandidateId	= 	rf.me
}

//选举没有异步，出问题
func (rf *Raft) ElectionDaemon() {
	for {
		select {
		case <-rf.resetTimer:
			rf.electionTimer.Reset(rf.electionTimeOut)
			DPrintf("peer %d : reset time", rf.me)
		case <-rf.electionTimer.C:
			//没有考虑到，需要重新进行指定
			DPrintf("peer %d : timeout", rf.me)
			rf.electionTimer.Reset(rf.electionTimeOut)
			go rf.Election()
		}
	}
}

//------------心跳，日志消息传递
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	DPrintf("follower %v get message， isHeartBeat %v", rf.me, args.Entries == nil)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//is heartbeat
	if args.Entries == nil{
		//选举的时候，接收到了新leader的消息
		if args.Term >= rf.currentTerm{
			rf.currentTerm 	= args.Term
			rf.votedFor = -1
			rf.resetTimer <- struct{}{}

			reply.Term 		= rf.currentTerm
			reply.Success 	= true
		}else{
			reply.Term 	  = rf.currentTerm
			reply.Success = false
		}
	}else{
		//TODO：
		// 这边需要更多考虑
		//TODO
		// 如果存在日志内容，2B最后内容--------
		if args.Term < rf.currentTerm{
			reply.Term 		= rf.currentTerm
			reply.Success 	= false
		}
		if args.LeaderId == rf.votedFor{
			for _, log := range args.Entries{
				rf.logs = append(rf.logs, log)
			}
		}
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) HeartBeat(arg *AppendEntriesArgs, peer int){
	var reply AppendEntriesReply
	if rf.SendAppendEntries(peer, arg, &reply){
		if !reply.Success{
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm < reply.Term{
				rf.currentTerm = reply.Term
				rf.votedFor	   = -1
			}
		}
	}
}

func (rf *Raft) fillAppendEntriesArg(args *AppendEntriesArgs, log LogEntry){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//如果是心跳的话，需要考虑什么
	args.Term			=	rf.currentTerm
	args.Entries		= 	append(args.Entries, log)
	args.LeaderCommit	=	rf.commitIndex
	args.LeaderId		=	rf.me
	args.PrevLogIndex	=	len(rf.logs) - 1
	args.PreLogTerm		=	rf.logs[args.PrevLogIndex].LogTerm
}

func (rf *Raft) HeartBeatDaemon() {
	for {
		if _, isLeader := rf.GetState(); isLeader {
			var appendEntry AppendEntriesArgs
			rf.fillAppendEntriesArg(&appendEntry, nil)
			for idx := range rf.peers {
				if idx == rf.me{
					rf.resetTimer <- struct{}{}
				}else{
					go rf.HeartBeat(&appendEntry, idx)
				}
			}
			time.Sleep(rf.heartBeatInterval)
		}else{
			break
		}
	}
}

func (rf *Raft) LogReplication(log LogEntry){
	wg := sync.WaitGroup{}
	args := make([]*AppendEntriesArgs, len(rf.peers))
	undoneChan := make(chan int, len(rf.peers) - 1)
	replys := make(chan LogReplicationReply, len(rf.peers) - 1)
	for idx := range rf.peers{
		if idx == rf.me{
			rf.resetTimer <- struct{}{}
			continue
		}
		undoneChan <- idx
		wg.Add(1)
	}
	go func() {
		wg.Wait()
		close(undoneChan)
		close(replys)
	}()
	for idx := range args{
		var arg AppendEntriesArgs
		rf.fillAppendEntriesArg(&arg, rf.logs)
		args[idx] = &arg
	}
	DPrintf("%v",args)
	for follower := range undoneChan{
		go func(follower int) {
			var reply *AppendEntriesReply
			ok := rf.SendAppendEntries(follower, args[follower], reply)
			if ok{
				if reply.Success{
					replys <- LogReplicationReply{follower, reply}
					args[follower].PrevLogIndex++
					wg.Done()
				}else{
					args[follower].PrevLogIndex--
					undoneChan <- follower
				}
			}else{
				undoneChan <- follower
			}
		}(follower)
	}
	var logReceiveCount int
	for replykv := range replys{
		if replykv.Success {
			if logReceiveCount++; logReceiveCount > len(rf.peers)/2 {
				//log 可以提交了,++是否可以？
				rf.commitIndex++
				//TODO
				// 复制之前所有日志到状态机
			}
		}
	}
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
	// Your initialization code here (2A, 2B, 2C).
	randTime := rand.Intn(300) + 200
	rf.resetTimer = make(chan struct{})
	rf.electionTimeOut = time.Duration(randTime) * time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electionTimeOut)
	rf.heartBeatInterval = time.Duration(100) * time.Millisecond
	// 缺少考虑
	rf.votedFor = -1				//-> 初始化 0 不可取
	rf.logs = make([]LogEntry,1)	//初始化一个log，从1开始进行，否则选举参数会出现问题

	DPrintf("peer %d : election(%s) heartbeat(%s)\n", rf.me, rf.electionTimeOut, rf.heartBeatInterval)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ElectionDaemon()
	return rf
}

package raft

import (
	"sync"
	"time"
)

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

func (rf *Raft) fillVoteRequestArgs(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1

	args.Term			=	rf.currentTerm
	args.LastLogIndex	=	len(rf.logs) -1
	args.LastLogTerm	=	rf.logs[args.LastLogIndex].LogTerm
	args.CandidateId	= 	rf.me
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

				rf.matchIndex = make([]int, len(rf.peers))
				for idx := range rf.nextIndex{
					rf.matchIndex[idx] = len(rf.logs) - 1
				}
				rf.nextIndex = make([]int, len(rf.peers))
				for idx := range rf.nextIndex{
					rf.nextIndex[idx] = len(rf.logs)
				}
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

func (rf *Raft) HeartBeatDaemon() {
	for {
		if _, isLeader := rf.GetState(); isLeader {
			var appendEntry AppendEntriesArgs
			rf.fillAppendEntriesArg(&appendEntry, -1, true)
			for peer := range rf.peers {
				if peer == rf.me{
					rf.resetTimer <- struct{}{}
				}else{
					go rf.HeartBeat(&appendEntry, peer)
				}
			}
			time.Sleep(rf.heartBeatInterval)
		}else{
			break
		}
	}
}
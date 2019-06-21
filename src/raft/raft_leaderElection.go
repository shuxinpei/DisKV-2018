package raft

import (
	"bytes"
	"labgob"
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
	isleader = rf.isLeader
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
	e := labgob.NewEncoder(w)

	var logs []LogEntry
	for i := 0; i <= rf.lastApplied; i++ {
		logs = append(logs, rf.logs[i])
	}
	persistData := PersistData{
		Raftstate{rf.commitIndex, rf.lastApplied, rf.currentTerm},
		Snapshot{logs},
	}
	// rf.commitIndex, rf.lastApplied, rf.currentTerm, logs
	e.Encode(persistData)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//DPrintf("peer %v save PersistData %+v",rf.me, persistData)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persistData PersistData
	if d.Decode(&persistData) != nil {
		//DPrintf("[ readPersist ] failed err is not nil")
	} else {
		//DPrintf("[ readPersist ] peer %v success persistData %+v", rf.me, persistData)
		rf.commitIndex = persistData.CommitIndex
		rf.lastApplied = persistData.ApplyIndex
		rf.currentTerm = persistData.CurrentTerm
		// recover the logs, if the log still in the object then jump, else append it
		for i := range persistData.Logs{
			logIndex := persistData.Logs[i].LogIndex
			if logIndex < len(rf.logs) && rf.logs[logIndex].LogTerm == persistData.Logs[i].LogTerm &&
				rf.logs[logIndex].LogIndex == persistData.Logs[i].LogIndex {
				rf.logs[logIndex] = persistData.Logs[i]
			}else{
				rf.logs = append(rf.logs, persistData.Logs[i])
			}
		}
	}
}

func (rf *Raft) fillVoteRequestArgs(args *RequestVoteArgs) {
	Lock("fillVoteRequestArgs", rf,true)

	rf.votedFor = rf.me
	rf.currentTerm = rf.currentTerm + 1
	args.Term			=	rf.currentTerm
	args.LastLogIndex	=	len(rf.logs) -1
	args.LastLogTerm	=	rf.logs[args.LastLogIndex].LogTerm
	args.CandidateId	= 	rf.me
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	Lock("RequestVote", rf, true)
	//TODO:进行回归测试
	//如果不加，则会出现一个周期内出现两个leader的情况
	//如果加了，则会出现大家都选举自己的情况，导致没有leader
	//用term有个问题，3个在进行选举，只有一个是符合选举的要求的，但是因为超时时间太长，就会导致它的term一直小于其他两者，这是不行的
	if args.Term < rf.currentTerm || args.Term == rf.currentTerm && rf.votedFor != rf.me { // 去掉等于进行尝试，vote失败会更新term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[ RequestVote failed ] peer %v term  %v me %v term %v", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	}else{
		//只有在满足拥有更适合的选举者的时候才会进行重置时间
		//日志约束条件, log term > rf.lastlog.term || term 相等，但是log index 大于 rf.last log term
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].LogTerm
		DPrintf("[ RequestVote success ] peer %v term  %v me %v term %v \r\n" +
			"lastLogTerm %v lastlogIndex %v arg: log term %v  log index %v",
			args.CandidateId, args.Term, rf.me, rf.currentTerm,
			lastLogIndex, lastLogTerm, args.LastLogTerm, args.LastLogIndex)
		if args.LastLogTerm > lastLogTerm ||
			args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {

			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			rf.isLeader = false
			reply.Term = rf.currentTerm
			reply.VoteGranted = true

			rf.resetTimer <- struct{}{}
			return
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
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
			DPrintf("[----Election]peer %v got vote and term %v ", rf.me, rf.currentTerm)
			if supportCount++; supportCount > len(rf.peers)/2 - 1 {
				Lock("Election-->success", rf,false)
				DPrintf("peer %d : win", rf.me)
				rf.votedFor = -1
				rf.isLeader = true

				for idx := range rf.nextIndex{
					rf.nextIndex[idx] = len(rf.logs)
					rf.matchIndex[idx] = len(rf.logs) - 1
				}
				//尝试发送没有commit的log给follower
				for i := rf.commitIndex + 1; i < len(rf.logs); i++ {
					rf.LogReplication(i)
				}
				go rf.HeartBeatDaemon()
				return
			}
		}
		//TODO
		//总觉有点问题
		if reply.Term > rf.currentTerm{
			Lock("Election-->i am old term", rf,false)
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.isLeader = false
			return
		}
	}
}

func (rf *Raft) ElectionDaemon() {
	for {
		select {
		case <-rf.resetTimer:
			rf.electionTimer.Reset(rf.electionTimeOut)
		case <-rf.electionTimer.C:
			DPrintf("peer %d : timeout ", rf.me)
			rf.electionTimer.Reset(rf.electionTimeOut)
			// 不是leader 才会进行选举
			rf.mu.mu.Lock()
			_, isLeader := rf.GetState()
			rf.mu.mu.Unlock()
			if !isLeader {
				go rf.Election()
			}
		}
	}
}

func (rf *Raft) HeartBeat(arg *AppendEntriesArgs, peer int){
	var reply AppendEntriesReply
	if rf.SendAppendEntries(peer, arg, &reply){
		if !reply.Success {
			DPrintf("heartBeat failed , leader %v peer %v", arg.LeaderId, peer)
			Lock("HeartBeat Reply faild", rf,false)
			 // 更新term使用心跳来
			 // rf.currentTerm  	= reply.Term
			 rf.votedFor	  	= -1
			 rf.isLeader		= false
		}
	}
}

func (rf *Raft) HeartBeatDaemon() {
	for {
		if _, isLeader := rf.GetState(); isLeader {
			for peer := range rf.peers {
				var appendEntry AppendEntriesArgs
				rf.fillAppendEntriesArg(&appendEntry, peer,-1, true)
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
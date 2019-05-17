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
	"strconv"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

const sendTimeOut = 100

type Raft struct {
	mu        MMutex          // Lock to protect shared access to this peer's state
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
	currentTerm 	int	//
	votedFor		int // candidateId that received vote in current term (or null if none)
	logs			[]LogEntry

	//Volatile state on all servers
	commitIndex 	int		//index of highest log entry has committed
	lastApplied		int		//highest log entry has applied

	//Volatile state on leaders, reinitialized after election
	nextIndex 		[]int	//next index of log of each peers to send the log
	matchIndex		[]int	//for each server's highest log entry known to be replicated

	appendStatus 	map[int]*appendCond
	//commitCond		*sync.Cond
	applyCh 		chan ApplyMsg // outgoing channel to service
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
		rf.mu.Lock("Start", rf, true)

		if _, isLeader := rf.GetState(); !isLeader {
			defer rf.mu.Unlock("Start", rf, true)
			return index, term, false
		}
		log := LogEntry{
			LogId:		rand.Intn(1e8) + 1e8,
			LogIndex:	len(rf.logs),
			LogTerm:	rf.currentTerm,
			Command:	command,
		}
		rf.logs = append(rf.logs, log)
		index := len(rf.logs) - 1
		//update itself's match index
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		// go send msg to followers and
		// get if majority of follower get the msg and add it to its' msg
		DPrintf("[start command]--Leader %v log, %v", rf.me, log)
		rf.mu.Unlock("Start", rf,true)

		rf.LogReplication(index)
	}
	return len(rf.logs) -1 , term, isLeader
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock("AppendEntries "+ strconv.FormatBool(args.Entries == nil) , rf, true)
	defer rf.mu.Unlock("AppendEntries 666", rf, true)
	if  args.PrevLogIndex < len(rf.logs) - 1 {
		reply.Term 		= rf.currentTerm
		reply.Success 	= false
		DPrintf("[ AppendEntries Failed ] follow %v", rf.me)
		return
	}
	if args.Entries == nil {
		rf.currentTerm 	= args.Term
		rf.votedFor 	= -1
		rf.isLeader		= false

		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > len(rf.logs) {
				rf.commitIndex = len(rf.logs)
			}else{
				rf.commitIndex = args.LeaderCommit
			}
		}
		rf.resetTimer <- struct{}{}
		reply.Term 		= rf.currentTerm
		reply.Success 	= true
		return
	}else {
		//如果时间需要很久的话，会可能出现超时，因为心跳与发送日志共用一个接口的
		preLogIndex := args.PrevLogIndex
		if preLogIndex < len(rf.logs) && rf.logs[preLogIndex].LogIndex == preLogIndex && args.PreLogTerm == rf.logs[preLogIndex].LogTerm {
			for i := range args.Entries{
				j := len(args.Entries) - 1 - i
				logIndex := args.Entries[j].LogIndex

				if logIndex < len(rf.logs) {
					rf.logs[logIndex] = args.Entries[j]
				}else{
					rf.logs = append(rf.logs, args.Entries[j])
				}
			}
			DPrintf("[ peer %v AppendEntries log] log %v:", rf.me, rf.logs)
			if args.LeaderCommit > rf.commitIndex{
				if args.LeaderCommit > len(rf.logs){
					rf.commitIndex = len(rf.logs)
				}else{
					rf.commitIndex = args.LeaderCommit
				}
			}
			reply.Success = true
			reply.Term = rf.currentTerm
			return
		}
		reply.Success = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) fillAppendEntriesArg(args *AppendEntriesArgs, peer int, idx int, isHeartBeat bool){
	Lock("fillAppendEntriesArg",rf, true)
	args.Term			=	rf.currentTerm
	args.LeaderCommit	=	rf.commitIndex
	args.LeaderId		=	rf.me
	if !isHeartBeat{
		var i = idx
		for ; i >= rf.nextIndex[peer]; i-- {
			args.Entries	= 	append(args.Entries, rf.logs[i])
		}
		args.PrevLogIndex	=	i
		args.PreLogTerm		=	rf.logs[i].LogTerm
	}else{
		/*args.Entries		= 	append(args.Entries, rf.logs[idx])*/
		args.PrevLogIndex	=	len(rf.logs) - 1
		args.PreLogTerm		=	rf.logs[args.PrevLogIndex].LogTerm
	}
}

func (rf *Raft) LogReplication(index int){
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(n, idx int) {
				rf.appendStatus[n].mu.Lock("LogReplication", rf, true)
				for rf.appendStatus[n].sending {
					rf.appendStatus[n].cond.Wait()
				}
				rf.appendStatus[n].mu.Unlock("LogReplication", rf, true)
				rf.SendLog(n, idx)
			}(i, index)
		}
	}
	/*DPrintf("[LogReplication] leader %v start ", rf.me)
	wg := sync.WaitGroup{}
	undoneChan := make(chan int, len(rf.peers) - 1)
	replys := make(chan LogReplicationReply, len(rf.peers) - 1)
	args := make([]AppendEntriesArgs, len(rf.peers))
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
	for i := 0; i < len(rf.peers); i++{
		if i != rf.me{
			rf.fillAppendEntriesArg(&args[i], index, false)
		}
	}
	go func() {
		var logReceiveCount int
		for replykv := range replys{
			if replykv.Success {
				if logReceiveCount++; logReceiveCount > len(rf.peers)/2 - 1 {
					rf.commitIndex++
					DPrintf("[LogReplication] msg pass, commit Index %v", rf.commitIndex)
					return
				}
			}
		}
	}()
	//send
	//并发存在问题
	for follower := range undoneChan{
		go func(follower int, arg AppendEntriesArgs) {
			//TODO 如果index小则进行等待，大则通过进行运行
			var reply AppendEntriesReply
			ok := rf.SendAppendEntries(follower, &arg, &reply)
			if ok{
				if reply.Success{
					replys <- LogReplicationReply{follower, &reply}
					//TODO
					rf.mu.Lock()
					rf.nextIndex[follower] = arg.Entries[0].LogIndex + 1
					rf.matchIndex[follower] = arg.Entries[0].LogIndex
					rf.mu.Unlock()
					DPrintf("[send msg success], follower %v nextIndex %v, matchIndex %v",follower, rf.nextIndex[follower], rf.matchIndex[follower])
					wg.Done()
				}else {
					DPrintf("[send msg fail], follower %v nextIndex %v, matchIndex %v", follower, rf.nextIndex[follower], rf.matchIndex[follower])
					if reply.Term > rf.currentTerm {
						rf.votedFor = -1
						return
					}
					//失败说明日志没有匹配，需要进行更新日志
					//有两种是需要进行覆盖的，一种是断线了，需要进行补充的
					index := len(rf.logs)-2
					for index > rf.matchIndex[follower] {
						rf.fillAppendEntriesArg(&arg, index, false)
						rf.SendAppendEntries(follower, &arg, &reply)
						if reply.Success {
							rf.mu.Lock()
							rf.nextIndex[follower] = arg.Entries[0].LogIndex + 1
							rf.matchIndex[follower] = arg.Entries[0].LogIndex
							rf.mu.Unlock()
							wg.Done()
							break
						} else {
							index--
						}
					}
				}
			}else{
				//因为此处会一直重新发送
				wg.Done()
			}
		}(follower, args[follower])
	}*/
}

func (rf *Raft) SendLog(peer, index int){

	rf.appendStatus[peer].mu.Lock("SendLog -> start", rf, true)
	rf.appendStatus[peer].sending = true
	rf.appendStatus[peer].sendTimer.Reset(time.Duration(sendTimeOut) * time.Millisecond)
	rf.appendStatus[peer].mu.Unlock("SendLog -> start", rf, true)

	var args AppendEntriesArgs
	rf.fillAppendEntriesArg(&args, peer, index, false)

	var reply AppendEntriesReply
	if rf.SendAppendEntries(peer, &args, &reply){
		if reply.Success {
			// update
			rf.mu.Lock("SendLog", rf,true)
			if rf.nextIndex[peer] < args.Entries[0].LogIndex + 1{
				if rf.nextIndex[peer] < args.Entries[0].LogIndex + 1 {
					rf.nextIndex[peer] = args.Entries[0].LogIndex + 1
					rf.matchIndex[peer] = rf.nextIndex[peer] - 1
				}
			}
			DPrintf("[send msg %v success], follower %v nextIndex %v, matchIndex %v", args, peer, rf.nextIndex[peer], rf.matchIndex[peer])
			rf.UpdateCommitIndex()
			rf.mu.Unlock("SendLog", rf,true)

			rf.appendStatus[peer].mu.Lock("SendLog -> finish -> cond",rf,true)
			rf.appendStatus[peer].sending = false
			rf.appendStatus[peer].cond.Signal()
			rf.appendStatus[peer].mu.Unlock("SendLog -> finish -> cond", rf,true)
		} else {
			DPrintf("[send msg %v fail], follower %v nextIndex %v, matchIndex %v", args, peer, rf.nextIndex[peer], rf.matchIndex[peer])
			/*if reply.Term > rf.currentTerm {
				rf.votedFor = -1
				return
			}
			index := len(rf.logs) - 2
			for index > rf.matchIndex[peer] {
				rf.fillAppendEntriesArg(&args, index, false)
				rf.SendAppendEntries(peer, &args, &reply)
				if reply.Success {
					rf.mu.Lock("SendLog -> finish -> append Lose-> cond", rf,false)
					if rf.nextIndex[peer] < args.Entries[0].LogIndex + 1 {
						rf.nextIndex[peer] = args.Entries[0].LogIndex + 1
						rf.matchIndex[peer] = args.Entries[0].LogIndex
					}
					rf.UpdateCommitIndex()
					rf.mu.Unlock("SendLog -> finish -> append Lose-> cond", rf,false)

					rf.appendStatus[peer].mu.Lock("SendLog", rf,false)
					rf.appendStatus[peer].sending = false
					rf.appendStatus[peer].cond.Signal()
					rf.appendStatus[peer].mu.Unlock("SendLog", rf,false)
					DPrintf("[send msg %v success], follower %v nextIndex %v, matchIndex %v, now log %v ", args, peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.logs)
					break
				} else {
					index--
				}
			}*/
		}
	}else{
		DPrintf("crach log %v", args.Entries)
		rf.appendStatus[peer].mu.Lock("SendLog -> Crash -> finish", rf,true)
		rf.appendStatus[peer].sending = false
		rf.appendStatus[peer].cond.Signal()
		rf.appendStatus[peer].mu.Unlock("SendLog -> Crash -> finish ", rf,true)
	}
}

func (rf *Raft) UpdateCommitIndex() {
	if rf.isLeader && rf.commitIndex < len(rf.logs) {
		CommitNum := 0
		for i := 0; i < len(rf.peers); i++{
			if rf.matchIndex[i] > rf.commitIndex{
				CommitNum++
			}
			if CommitNum > len(rf.peers) / 2 {
				rf.commitIndex++
				DPrintf("leader %v update the commit index %v", rf.me, rf.commitIndex)
				break
			}
		}
	}
}

func (rf *Raft) ApplyUncommitLog(){
	for {
		rf.mu.mu.Lock()
		last, cur := rf.lastApplied, rf.commitIndex
		//DPrintf("[ApplyUncommitLog--] peer %v last applied number %v ", rf.me, last)
		//DPrintf("peer %v last applied %v commitIndex %v",rf.me, last, cur)
		if last < cur {
			//DPrintf("[ApplyUncommitLog] peer %v last applied number %v last commit number %v", rf.me, last, cur)
			for i := 0; i < cur-last; i++ {
				//wait for log to update
				if  last + i + 1 <= len(rf.logs) -1 {
					// current command is replicated
					reply := ApplyMsg{
						CommandIndex:   last + i + 1,
						Command: 		rf.logs[last + i + 1].Command,
						CommandValid:   true,
					}
					rf.lastApplied ++
					// reply to outer service
					// Note: must in the same goroutine, or may result in out of order apply
					rf.applyCh <- reply
					DPrintf("[ApplyUncommitLog] peer %v last applied number %v command %v", rf.me, last + i + 1, reply.Command)
				}
			}
		}
		rf.mu.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
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
	randTime := rand.Intn(300) + 300
	rf.resetTimer = make(chan struct{})
	rf.electionTimeOut = time.Duration(randTime) * time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electionTimeOut)
	rf.heartBeatInterval = time.Duration(100) * time.Millisecond

	rf.votedFor = -1
	rf.logs = make([]LogEntry,1)

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	DPrintf("peer %d : election(%s) heartbeat(%s)\n", rf.me, rf.electionTimeOut, rf.heartBeatInterval)

	rf.mu = *NewMMutex()
	rf.appendStatus = make(map[int]*appendCond)
	for i := 0; i < len(rf.peers); i++ {
		ac := new(appendCond)
		mmu := NewMMutex()
		ac.mu = mmu
		ac.sending = false
		ac.cond = sync.NewCond(&ac.mu.mu)
		ac.sendTimer = time.NewTimer(time.Duration(sendTimeOut) * time.Millisecond)
		ac.TimeOutFree()
		rf.appendStatus[i] = ac
	}
	//rf.commitCond = sync.NewCond(&rf.mu.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ApplyUncommitLog()
	go rf.ElectionDaemon()
	//go rf.SubmitCommitLogDaemon()
	//go rf.LogConsistencyDaemon()

	return rf
}

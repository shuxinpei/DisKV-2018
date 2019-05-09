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
		rf.mu.Lock()
		DPrintf("[start]--Leader %v get command, %v", rf.me, command)
		if _, isLeader := rf.GetState(); !isLeader {
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	DPrintf("follower %v get message， isHeartBeat %v", rf.me, args.Entries == nil)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//is heartbeat
	if args.Entries == nil{
		//选举的时候，接收到了新leader的消息
		if args.Term >= rf.currentTerm{
			rf.currentTerm 	= args.Term
			rf.votedFor = args.LeaderId
			rf.resetTimer <- struct{}{}

			reply.Term 		= rf.currentTerm
			reply.Success 	= true
			if args.PrevLogIndex == len(rf.logs)-1 && args.PreLogTerm == rf.logs[len(rf.logs)-1].LogTerm{
				if rf.commitIndex != args.LeaderCommit{
					DPrintf("change commit log index: %v", rf.commitIndex)
					for rf.commitIndex < args.LeaderCommit{
						rf.commitIndex++
						logToApply := rf.logs[rf.commitIndex]
						reply := ApplyMsg{
							CommandValid:	true,
							Command:		logToApply.Command,
							CommandIndex:	logToApply.LogIndex,
						}
						DPrintf("%v write to apply msg, reply %v", rf.me, reply)
						rf.applyCh <- reply
					}
				}
			}
		}else{
			reply.Term 	  = rf.currentTerm
			reply.Success = false
		}
	}else{
		//TODO：
		// 这边需要更多考虑
		//TODO
		// 如果存在日志内容，2B最后内容--------
		DPrintf("\n[%v] get command vote for %v, arg %+v, \n---> currentTerm %v logs:%+v", rf.me, rf.votedFor, args, rf.currentTerm, rf.logs)
		currentTerm := rf.currentTerm
		lenLog := len(rf.logs)-1
		if args.Term < currentTerm{
			reply.Term 		= currentTerm
			reply.Success 	= false
			DPrintf("[faild], term is old")
			return
		}
		if args.LeaderId == rf.votedFor && args.PrevLogIndex == lenLog && args.PreLogTerm == rf.logs[lenLog].LogTerm {
			for idx := range args.Entries{
				logIndex := len(args.Entries)-1-idx
				log := args.Entries[logIndex]
				if log.LogIndex < len(rf.logs){
					rf.logs[log.LogIndex] = log
				}else{
					rf.logs = append(rf.logs, log)
				}
			}
			reply.Success = true
			reply.Term = currentTerm
			reply.MatchedIndex = args.Entries[0].LogIndex
		}else{
			reply.Success = false
			reply.Term = currentTerm
		}
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) fillAppendEntriesArg(args *AppendEntriesArgs, idx int, isHeartBeat bool){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//如果是心跳的话，需要考虑什么
	if !isHeartBeat{
		args.Term			=	rf.currentTerm
		args.Entries		= 	append(args.Entries, rf.logs[idx])
		args.LeaderCommit	=	rf.commitIndex
		args.LeaderId		=	rf.me
		args.PrevLogIndex	=	idx-1
		args.PreLogTerm		=	rf.logs[args.PrevLogIndex].LogTerm
		DPrintf("[fill AppendEntries Command]--%v",args)
	}else{
		args.Term			=	rf.currentTerm
		/*args.Entries		= 	append(args.Entries, rf.logs[idx])*/
		args.LeaderCommit	=	rf.commitIndex
		args.LeaderId		=	rf.me
		args.PrevLogIndex	=	len(rf.logs) - 1
		args.PreLogTerm		=	rf.logs[args.PrevLogIndex].LogTerm
	}

}

func (rf *Raft) LogReplication(log LogEntry){
	DPrintf("[replication] leader %v start ", rf.me)
	wg := sync.WaitGroup{}
	var arg AppendEntriesArgs
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
	rf.fillAppendEntriesArg(&arg, len(rf.logs)-1, false)

	for follower := range undoneChan{
		go func(follower int) {
			var reply AppendEntriesReply
			ok := rf.SendAppendEntries(follower, &arg, &reply)
			if ok{
				DPrintf("----------%+v",reply)
				if reply.Success{
					DPrintf("[success]")
					replys <- LogReplicationReply{follower, &reply}
					rf.nextIndex[follower]++
					rf.matchIndex[follower]++
					wg.Done()
				}else{
					if reply.Term > rf.currentTerm{
						rf.votedFor = -1
						return
					}
					rf.nextIndex[follower]--
					rf.matchIndex[follower]--
					var arg AppendEntriesArgs
					offset := 1
					rf.fillAppendEntriesArg(&arg, len(rf.logs)-1-offset,false)
					for {
						ok := rf.SendAppendEntries(follower, &arg, &reply)
						if ok{
							rf.matchIndex[follower] = reply.MatchedIndex
							break
						}else{
							rf.nextIndex[follower]--
							rf.matchIndex[follower]--
							offset++
							rf.fillAppendEntriesArg(&arg, len(rf.logs)-1-offset,false)
						}
					}
					//挫一点，先和heartbeat分开
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
				reply := ApplyMsg{
					CommandValid	:true,
					Command			: log.Command,
					CommandIndex	: log.LogIndex,
				}
				rf.applyCh <- reply
				rf.lastApplied++
				DPrintf("------------commit Index %v", rf.commitIndex)
				DPrintf("%v write to apply msg, reply %v", rf.me, reply)
				return
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

	rf.applyCh = applyCh
	DPrintf("peer %d : election(%s) heartbeat(%s)\n", rf.me, rf.electionTimeOut, rf.heartBeatInterval)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ElectionDaemon()

	//go rf.LogConsistencyDaemon()

	return rf
}

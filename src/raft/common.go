package raft

import (
	"math/rand"
	"sync"
	"time"
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
	ShutDown 		bool
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

type appendCond struct {
	mu 		 	*MMutex
	sending  	bool
	sendTimer   *time.Timer
	cond 	 	*sync.Cond
	//先按照自己思路写一下
	//exit bool
}
func (ac *appendCond) Send(rf *Raft) {
	ac.mu.Lock("SendLog -> start", rf, true)
	ac.sending = true
	ac.sendTimer.Reset(time.Duration(sendTimeOut) * time.Millisecond)
	ac.mu.Unlock("SendLog -> start", rf, true)
}

func (ac *appendCond) Finish(rf *Raft) {
	ac.mu.Lock("SendLog -> finish -> cond",rf,true)
	ac.sending = false
	ac.cond.Signal()
	ac.mu.Unlock("SendLog -> finish -> cond", rf,true)
}

func (ac *appendCond) TimeOutFree() {
	go func() {
		for {
			select {
			case <-ac.sendTimer.C:
				if ac.sending{
					ac.mu.Lock("TimeOutFree", nil, true)
					ac.sending = false
					ac.cond.Signal()
					ac.mu.Unlock("TimeOutFree", nil, true)
				}
			}
		}
	}()
}

type MMutex struct {
	id			int
	mu			sync.Mutex
	lockTime 	int64
}

func NewMMutex() *MMutex{
	mmu := &MMutex{
		id:		rand.Intn(1e8) + 1e8,
	}
	return mmu
}

func (mmu *MMutex) Lock(funcName string, rf *Raft, mute bool){
	mmu.mu.Lock()
	if !mute{
		waitTime := time.Now().Unix() - mmu.lockTime
		mmu.lockTime = time.Now().Unix()
		if rf != nil {
			LPrintf("[%v] %v LLLock--id %v [waitTime %v]", funcName, rf.me, mmu.id, waitTime)
		}else {
			LPrintf("[%v] LLLock--id %v [waitTime %v]", funcName, mmu.id, waitTime)
		}
	}
}

func (mmu *MMutex) Unlock(funcName string, rf *Raft, mute bool){
	mmu.mu.Unlock()
	if !mute{
		duration := time.Now().Unix() - mmu.lockTime
		if rf != nil {
			LPrintf("[%v] %v UULock--id %v [time: %v]", funcName, rf.me, mmu.id, duration)
		}else {
			LPrintf("[%v] UULock--id %v [time: %v]", funcName, mmu.id, duration)
		}
	}
}

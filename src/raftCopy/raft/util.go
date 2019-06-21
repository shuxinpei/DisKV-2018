package raft

import (
	"log"
	"math/rand"
	"runtime/debug"
)

// Debugging
const Debug = 1
const DebugLock = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func LPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugLock > 0 {
		log.Printf(format, a...)
	}
	return
}

func Recover(msg interface{}) {
	if e := recover(); e != nil {
		DPrintf("%v, panic %s: %s", msg ,e, string(debug.Stack()))
	}
}

func Lock(funcName string, rf *Raft, mute bool) {
	lockId := rand.Intn(1e8) + 1e8
	rf.mu.Lock(funcName, rf, mute)
	defer func(lockId int) {
		rf.mu.Unlock(funcName, rf, mute)
	}(lockId)
}
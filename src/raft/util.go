package raft

import (
	"6.824/debugFlag"
	"log"
)

// Debugging
const Debug = debugFlag.RaftFlag

func init() {
	log.SetFlags(log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

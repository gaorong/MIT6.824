package raft

import (
	"log"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// RandomTimeout generate random timeout duration unit by millsecond
func RandomTimeout(base time.Duration, burst int) time.Duration {
	s := rand.Int() % burst
	return time.Duration(s)*time.Millisecond + base
}

package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 时间工具
// 时间配置参数
const (
	electionTimeoutLower = time.Millisecond * time.Duration(500)
	electionTimeoutUpper = time.Millisecond * time.Duration(900)
	heartbeatTimeout     = time.Millisecond * time.Duration(100)
)

// 随机产生一个 electionTimeoutLower ~ electionTimeoutUpper 的时间
func randomizedElectionTimeout() time.Duration {
	t := rand.Int63n(electionTimeoutUpper.Milliseconds()-electionTimeoutLower.Milliseconds()) + electionTimeoutLower.Milliseconds()
	return time.Duration(t) * time.Millisecond
}

// 返回心跳超时时长 heartbeatTimeout
func stableHeartbeatTimeout() time.Duration {
	return heartbeatTimeout
}

// 重置 timer
func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

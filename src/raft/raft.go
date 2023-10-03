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
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// 节点状态常量
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft 节点结构体
type Raft struct {
	mu        sync.Mutex          // 用于保护共享变量的互斥锁
	peers     []*labrpc.ClientEnd // 集群内所有 raft 节点的 RPC 端
	persister *Persister          // 用于保存持久化状态的对象
	me        int                 // 本节点在 peers 中的索引
	dead      int32               // 用于测试器设置崩溃状态的变量，会被 Kill() 设置

	// 选举相关状态
	currentTerm    int         // raft 节点当前的任期
	votedFor       int         // 本节点在当前任期给索引为 votedFor 的节点投票
	state          StateType   // 本节点的状态
	electionTimer  *time.Timer // 选举计时器,time.NewTicker() 返回的是一个指向 time.Tiker 的指针
	heartbeatTimer *time.Timer // 心跳计时器

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// Make：上层调用 Make() 创建一个 Raft 节点
// peers 是集群内所有 raft 节点的端口，本节点对应的端口是 peers[me]
// persister 本节点保存持久化状态的地方
// applyCh 是本 raft 节点向上层服务提交日志的通道，往里发送 ApplyMsg 消息
// Make() 函数需要立即返回，因此要通过启动协程来执行长期任务
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 初始化选举相关状态
	// 初始化时所有节点的任期都是 0，均处于未投票，并且都处于 StateFollower 状态
	// 初始化时所有节点只启动选举过期计时器，停止心跳计时器
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = StateFollower
	rf.electionTimer = time.NewTimer(randomizedElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(stableHeartbeatTimeout())
	rf.heartbeatTimer.Stop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 启动 ticker goroutine 来监听选举/心跳到时
	go rf.ticker()

	return rf
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			// 选举超时，开启一轮选举
			// 如果是从 StateFollower 超时，将状态改成 StateCandidate 再发起选举
			// 如果是从 StateCandidate 超时，直接发起选举
			// 需要重置选举计时器
			// 注意实现 becomeCandidate() 和 startElection() 时必须立刻返回，不能阻塞
			// 注意实现 becomeCandidate() 和 startElection() 时这两个方法本身不能尝试去获得锁
			rf.mu.Lock()
			if rf.state == StateFollower {
				rf.becomeCandidate()
			} else if rf.state == StateLeader {
				DPrintf("[Error] {Node %v} 从 leader 状态选举超时，不合理", rf.me)
			}
			DPrintf("[Info] {Node %v} 选举超时，发起一轮选举", rf.me)
			rf.startElection()
			rf.electionTimer.Reset(randomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			// 心跳超时，发送心跳
			// 需要重置心跳计时器
			rf.mu.Lock()
			if rf.state != StateLeader {
				DPrintf("[Error] {Node %v} 从非 leader 状态心跳超时，不合理", rf.me)
				continue
			}
			rf.broadcastHeartbeat()
			rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
			rf.mu.Unlock()
		}
	}
}

// 投票 RPC 请求结构体，首字母必须大写
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

// 投票 RPC 响应结构体，首字母必须大写
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// 发送选举 RPC 请求
// server 是目的节点在 peers 中的索引
// 实验提供的 labrpc 模拟了不稳定网络，请求和响应可能会丢失
// 如果在超时时间内，没响应，Call 会返回 false
// 如果在超时时间内，响应了，Call 会返回 true
// Call 的第一个参数是相应的 handler，记住这个 handler 一定不要不陷入死等
// ../labrpc/labrpc.go 有更多信息
// 确保参数正确，是指针，结构体内字段首字母大写
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 发起选举（候选人视角）
func (rf *Raft) startElection() {
	// 开启一轮选举
	// 首先是自增任期，给自己投票，计数初始化为 1
	rf.currentTerm += 1
	rf.votedFor = rf.me
	var voteCnt int32 = 1

	// 然后构造请求，不能在启动的协程内构造，因为返回后，锁会被释放，状态可能会变
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	// 用协程并发地向其它节点发去投票请求
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(peer, args, reply) {
				// 根据响应 reply 来统计 voteCnt,如果超出一半节点投票
				// 注意必须检查当前任期是否仍与投票请求RPC中的Term相同，如果不相同，那就是过时响应，应该丢弃
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.state == StateCandidate {
					if reply.VoteGranted {
						// 获得选票: 增加 voteCnt，注意需要用到原子操作，不同 goroutine 会并发访问 voteCnt
						// 如果 voteCnt 超过半数，转变状态为 Leader
						atomic.AddInt32(&voteCnt, 1)
						if atomic.LoadInt32(&voteCnt) > int32(len(rf.peers)/2) {
							rf.becomeLeader()
							rf.electionTimer.Stop()
							resetTimer(rf.heartbeatTimer, stableHeartbeatTimeout())
						}
					} else {
						// 没获得选票，看响应中的 Term 是否大于本节点的 Term，是的话就放弃选举，变成 Follower
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.becomeFollower()

						}
					}
				}
			} else {
				DPrintf("[Info] {Node %v} 向 {Node %v} 发出请求投票RPC,通信失败", rf.me, peer)
			}
		}(peer)
	}
}

// 响应选举（follower视角）
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 首先是判断 args 里的 Term 是否大于本节点的任期
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		// 如果 args.Term 大于当前节点的任期，则同意投票，并设置自己的任期为 args.Term
		// 如果当前节点不是 follower 来放弃当前状态，转变成 follower
		// 同意投票需要重置选举计时器
		DPrintf("[Info] {Node %v} 当前任期为 %v, 同意向 {Node %v} 投票", rf.me, rf.currentTerm, args.CandidateId)
		if rf.state == StateLeader {
			rf.heartbeatTimer.Stop()
		}
		rf.becomeFollower()
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		resetTimer(rf.electionTimer, randomizedElectionTimeout())
	} else {
		// 如果 args.Term 不大于当前节点的任期，则不同意投票，告知对方自己的任期
		DPrintf("[Info] {Node %v} 当前任期为 %v, 拒绝向 {Node %v} 投票", rf.me, rf.currentTerm, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

// 同步日志RPC 请求结构体，首字母必须大写
type AppendEntriesArgs struct {
	Term int
}

// 同步日志RPC 响应结构体，首字母必须大写
type AppendEntriesReply struct {
	Term int
}

// 发送同步日志 RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 发送心跳(leader视角)
func (rf *Raft) broadcastHeartbeat() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// 这里不需要加锁，因为 peers 和 rf.me 都不会引起并发冲突
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			// 构造请求结构体，这里选择在 goroutine 内部构造，是因为后面同步日志时需要考虑每个 peer 的情况
			args := &AppendEntriesArgs{
				Term: rf.currentTerm,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if args.Term == rf.currentTerm {
					// 处理任期内响应
					if reply.Term > rf.currentTerm {
						// 如果 reply.Term 大于当前节点的任期，转变成 follower
						rf.becomeFollower()
						rf.heartbeatTimer.Stop()
						resetTimer(rf.electionTimer, randomizedElectionTimeout())
						rf.currentTerm = reply.Term
					}
				}
			}
		}(peer)
	}
}

// 处理同步日志请求（follower视角）
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		if rf.state == StateCandidate {
			rf.becomeFollower()
		} else if rf.state == StateLeader {
			rf.becomeFollower()
			rf.heartbeatTimer.Stop()
		}
		resetTimer(rf.electionTimer, randomizedElectionTimeout())
	} else {
		reply.Term = rf.currentTerm
	}
}

// 状态转变系列方法
func (rf *Raft) becomeCandidate() {
	rf.state = StateCandidate
}

func (rf *Raft) becomeFollower() {
	rf.state = StateFollower
}

func (rf *Raft) becomeLeader() {
	rf.state = StateLeader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = (rf.state == StateLeader)
	term = rf.currentTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

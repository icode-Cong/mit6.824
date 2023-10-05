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

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

// 日志条目
type Entry struct {
	Term    int         // 日志任期
	Index   int         // 日志索引
	Command interface{} // 日志命令
}

type debugEntry struct {
	Term  int // 日志任期
	Index int // 日志索引
}

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

	// 日志复制相关状态
	logs        []Entry       // 日志列表
	matchIndex  []int         // matchIndex[i]表示 peer[i]已经同步到了索引matchIndex[i]的日志
	nextIndex   []int         // nextIndex[i]表示 下次往peer[i]同步日志的起点
	commitIndex int           // commitIndex表示 已经安全的最新的日志索引
	lastApplied int           // lastApplied表示 最新的已经提交给上层的日志索引
	applyCh     chan ApplyMsg // applyCh 是用于向上层提交日志的通道
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

	// 初始化日志复制相关状态
	// 第一个条目被空出来的原因是为了后面快照的实现，也就是 Index 为 0 的条目被占用了，真实的日志索引从 1 开始
	rf.logs = make([]Entry, 1)
	rf.matchIndex = make([]int, len(peers)) // matchIndex 初始化时置为 0
	rf.nextIndex = make([]int, len(peers))  // nextIndex 初始化时置为最后一条日志索引+1
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = rf.getLastLog().Index + 1
	}
	rf.commitIndex = rf.getFirstLog().Index // 这里在lab2B以前都实质是 0，这样做是为了后面快照的实现
	rf.lastApplied = rf.getFirstLog().Index
	rf.applyCh = applyCh

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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	rf.persist()

	// 然后构造请求，不能在启动的协程内构造，因为返回后，锁会被释放，状态可能会变
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
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
							rf.persist()
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

		// 无论同不同意投票，如果对方任期比自己的任期大，都一定得更新任期并退位
		if rf.state == StateLeader {
			rf.heartbeatTimer.Stop()
		}
		rf.becomeFollower()
		rf.currentTerm = args.Term
		rf.persist()

		// 增加考虑日志的选举，选举限制
		if args.LastLogTerm < rf.getLastLog().Term || (args.LastLogTerm == rf.getLastLog().Term && args.LastLogIndex < rf.getLastLog().Index) {
			reply.VoteGranted = false
			return
		}

		DPrintf("[Info] {Node %v} 当前任期为 %v, 同意向 {Node %v} 投票", rf.me, rf.currentTerm, args.CandidateId)
		reply.VoteGranted = true
		resetTimer(rf.electionTimer, randomizedElectionTimeout())
	} else {
		// 如果 args.Term 不大于当前节点的任期，则不同意投票，告知对方自己的任期
		DPrintf("[Info] {Node %v} 当前任期为 %v, 拒绝向 {Node %v} 投票", rf.me, rf.currentTerm, args.CandidateId)
		reply.Term = rf.currentTerm
		rf.persist()
		reply.VoteGranted = false
	}
}

// 同步日志RPC 请求结构体，首字母必须大写
type AppendEntriesArgs struct {
	Term        int
	PreLogIndex int
	PreLogTerm  int
	Logs        []Entry
	CommitIndex int
}

// 同步日志RPC 响应结构体，首字母必须大写
type AppendEntriesReply struct {
	Term    int
	Success bool
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
			if rf.state != StateLeader {
				rf.mu.Unlock()
				return
			}
			// 构造请求结构体，这里选择在 goroutine 内部构造，是因为后面同步日志时需要考虑每个 peer 的情况
			// 在日志同步功能中，leader 希望其它 raft 节点做两件事：
			// 1. 添加待同步的日志
			//	1.1 需要判断两者日志列表是否已经一致，只有在一致状态下才能往里添加新的日志
			// 		preLogIndex、preLogTerm 用于判断新日志的前一条日志是否已经一致
			//		logs 需要添加到日志列表里的新日志
			// 2. 向上层提交已被共识的安全日志
			//		commitIndex 用于告知 follower 节点，可被提交的日志索引
			// DPrintf("[Info] {Node %v} 向 {Node %v} 发去同步日志，{Node %v} 当前状态：rf.nextIndex=%v, rf.getFirstLog().Index=%v", rf.me, peer, rf.me, rf.nextIndex[peer], rf.getFirstLog().Index)
			args := &AppendEntriesArgs{
				Term:        rf.currentTerm,
				PreLogIndex: rf.nextIndex[peer] - 1,
				PreLogTerm:  rf.logs[rf.nextIndex[peer]-1-rf.getFirstLog().Index].Term,
				Logs:        rf.cloneLogs(rf.nextIndex[peer]), // 这里必须切断与原切片的联系，因为在发送请求之前会先释放锁，因此不能直接用简单的截断切片的做法
				CommitIndex: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			DPrintf("[Info] {Node %v} 向 {Node %v} 发去同步日志，{Node %v} 当前状态: logs = %v", rf.me, peer, rf.me, getDebugEntry(rf.logs))
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
						rf.persist()
					} else {
						// 正常响应
						if reply.Success {
							// 如果同步日志成功，则更新对应 peer 的 matchIndex 和 nextIndex
							// 还要检查 matchIndex 来更新 commitIndex
							rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PreLogIndex+len(args.Logs))
							rf.nextIndex[peer] = rf.matchIndex[peer] + 1
							// rf.checkMatchIndexToUpdateCommitIndex()
							// 从后往前,确定最大的安全日志索引
							for i := len(rf.logs) - 1; i > 0; i-- {
								cnt := 1
								for _, matchIndex := range rf.matchIndex {
									if matchIndex >= i {
										cnt += 1
									}
								}
								if cnt > len(rf.peers)/2 {
									rf.commitIndex = rf.logs[i].Index
									break
								}
							}
							// rf.applyLogToService()
							// 从 rf.lastApplied 开始提交，提交完毕后更新 lastApplied
							if rf.commitIndex > rf.lastApplied {
								for i := rf.lastApplied; i <= rf.commitIndex; i++ {
									msg := ApplyMsg{
										CommandValid: true,
										Command:      rf.logs[i-rf.getFirstLog().Index].Command,
										CommandIndex: rf.logs[i-rf.getFirstLog().Index].Index,
									}
									rf.applyCh <- msg
								}
								rf.lastApplied = rf.commitIndex
							}
						} else {
							// 一致性检查不通过，回退同步日志索引
							rf.nextIndex[peer]--
						}
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
		// 进行一致性检查
		//if rf.checkConsistency() {
		if rf.getLastLog().Index >= args.PreLogIndex && rf.logs[args.PreLogIndex-rf.getFirstLog().Index].Term == args.PreLogTerm {
			// // 通过则添加日志
			// // 首先要裁掉不一致的日志
			// rf.logs = rf.logs[:args.PreLogIndex-rf.getFirstLog().Index+1]
			// // 然后再追加新日志
			// rf.logs = append(rf.logs, args.Logs...)

			unmatchIndex := -1
			for i := 0; i < len(args.Logs); i++ {
				toAddIndex := args.Logs[i].Index
				toAddTerm := args.Logs[i].Term
				if toAddIndex-rf.getFirstLog().Index < len(rf.logs) && rf.logs[toAddIndex-rf.getFirstLog().Index].Term == toAddTerm {
					continue
				}
				unmatchIndex = i
				break
			}
			if unmatchIndex != -1 {
				rf.logs = rf.logs[:args.Logs[unmatchIndex].Index-rf.getFirstLog().Index]
				rf.logs = append(rf.logs, args.Logs[unmatchIndex:]...)
				rf.persist()
			}
			// 全都匹配，这是延迟到达的请求，不予理会
			reply.Success = true
			if len(args.Logs) > 0 {
				DPrintf("[Info] {Node %v} 同意同步日志，{Node %v} 得到参数：{logs = %v}, 当前状态: {logs = %v}", rf.me, rf.me, getDebugEntry(args.Logs), getDebugEntry(rf.logs))
			}

			// 进行提交
			// 只有更大的commitIndex时才更新
			if args.CommitIndex > rf.commitIndex {
				rf.commitIndex = args.CommitIndex
			}

			if rf.commitIndex > rf.lastApplied {
				for i := rf.lastApplied; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[i-rf.getFirstLog().Index].Command,
						CommandIndex: rf.logs[i-rf.getFirstLog().Index].Index,
					}
					rf.applyCh <- msg
				}
				rf.lastApplied = rf.commitIndex
			}
		} else {
			// 不通过，则拒绝添加日志
			DPrintf("[Info] {Node %v} 拒绝同步日志，{Node %v} 当前状态: logs = %v", rf.me, rf.me, getDebugEntry(rf.logs))
			reply.Success = false
		}

		resetTimer(rf.electionTimer, randomizedElectionTimeout())
	} else {
		reply.Term = rf.currentTerm
		rf.persist()
		reply.Success = false
	}
}

// 上层使用 Start 方法来向 raft 节点传递命令，以期将该命令加到日志列表中，达成共识
// 如果该节点不是 leader 返回 false
// 如果该节点是 leader 就将该日志加到日志列表中，并并行地向其它节点发起同步请求
// 这个方法必须立刻返回，不能阻塞，也就是说就算返回 true 也不保证对应的命令可以成功被共识
// 第一个返回值是命令在日志列表中的索引
// 第二个返回值是命令在日志列表中的任期
// 第三个返回值是命令是该节点是否是 leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = (rf.state == StateLeader)
	if isLeader {
		newLog := rf.appendLog(command)
		index = newLog.Index
		term = newLog.Term
		rf.broadcastHeartbeat()
	}
	return index, term, isLeader
}

// 保存持久化状态用于崩溃恢复
// 没有快照时，persister.Save() 第二个参数为 nil
// 首先考虑极端情况，所有节点都崩溃，重新恢复
// 1. 必须有日志；如果部分节点已经提交了一些日志，但部分节点没提交这部分日志，重启后他们也没机会提交这部分日志，日志从1开始，任期从1开始，会导致提交日志不一致
// 2. 只保存日志，会重新选举，日志从1开始，任期从1开始，选举限制就失效了，需要保存 currentTerm
// 3. commitIndex 和 lastApplied 需要持久化吗？重启后，两者都为0/快照，不会引起提交，待新的 leader 选出后，会由新的 leader 去推断 commitIndex
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var logs []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&logs) != nil {
		DPrintf("[Error] {Node %v} 读取持久化状态失败", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.logs = logs
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

// 测试器用来获取节点的任期和是否是leader的
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = (rf.state == StateLeader)
	term = rf.currentTerm
	return term, isleader
}

// 日志系列工具方法，注意该类工具方法全都不加锁，调用时需考虑到这个问题
func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) appendLog(command interface{}) Entry {
	newLog := Entry{
		Index:   rf.getLastLog().Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, newLog)
	rf.persist()
	return newLog
}

func (rf *Raft) cloneLogs(index int) []Entry {
	// DPrintf("[Info] {Node %v} 克隆日志: 入参 {index = %v}, 中间变量{rf.lastLogIndex = %v, rf.logs = %v}", rf.me, index, rf.getLastLog().Index, getDebugEntry(rf.logs))
	temp := make([]Entry, rf.getLastLog().Index-index+1)
	copy(temp, rf.logs[index-rf.getFirstLog().Index:])
	// DPrintf("[Info] {Node %v} 克隆日志: 入参 {index = %v}, 中间变量{len(temp) = %v, rf.logs = %v}, 出参{temp = %v}", rf.me, index, rf.getLastLog().Index-index+1, rf.logs[index-rf.getFirstLog().Index:], temp)
	return temp
}

func getDebugEntry(logs []Entry) []debugEntry {
	debuges := []debugEntry{}
	for _, e := range logs {
		debuges = append(debuges, debugEntry{
			Term:  e.Term,
			Index: e.Index,
		})
	}
	return debuges
}

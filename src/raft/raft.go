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

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"fmt"
	"math"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{} //client发送的执行命令
	Term    int         //从leader读取到的term
}

const (
	Leader    = 0
	Candidate = 1
	Follower  = 2
)

const (
	NoLeader = -1
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state             int           // 所属的状态
	heartbeatNotify   chan bool     //心跳通知
	voteNotify        chan bool     //投票通知
	electLeaderNotify chan bool     //选举leader通知
	electionTimeout   time.Duration //选举超时channel
	votedCount        int           //票数
	leaderId          int           //领导者id

	//持久化数据
	currentTerm int           // 最新term
	votedFor    int           // 保存的候选人id
	log         []LogEntry    //日志
	applyCh     chan ApplyMsg //提交的日志，该channel是client传递给raft的一个参数，用于监听提交的消息

	//所有server上的volatile数据
	commitIndex int // 最新的已提交日志的index  单调递增
	lastApplied int // 最新的已apply日志的index 单调递增

	//leader上的volatile数据，用数组存储用来维护每个server的index信息
	nextIndex  []int // 即将要发送给所有server的日志
	matchIndex []int // 已发送给所有server的日志的最高index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) SyncState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的term
	CandidatId   int // 请求选票的候选人id
	LastLogIndex int // 候选人最后一条日志的index
	LastLogTerm  int // 候选人最后一条日志的term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前term
	VoteGranted bool // 是否通过投票
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) canVote(candidateId int, candidateLastLogIndex int, candidateLastLogTerm int) bool {
	return rf.agreeVote(candidateId) && rf.agreeLog(candidateLastLogTerm, candidateLastLogIndex)
}

func (rf *Raft) agreeLog(candidateLastLogTerm, candidateLastLogIndex int) bool {
	lastLogIndex := len(rf.log) - 1
	lastLog := rf.log[lastLogIndex]
	return candidateLastLogTerm > lastLog.Term ||
		(candidateLastLogTerm == lastLog.Term &&
			candidateLastLogIndex >= lastLogIndex)
}

func (rf *Raft) agreeVote(candidateId int) bool {
	return rf.votedFor < 0 || rf.votedFor == candidateId
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// 当rpc请求方term大于自己term时，立马转变为follower，并同步自己的term信息
		rf.turnFollower(args.Term, NoLeader)
	}

	if rf.canVote(args.CandidatId, args.LastLogIndex, args.LastLogTerm) {
		//如果发现还没有投票，或者已投票给该候选人，并且候选人的日志不比自己旧，则投票给该候选人
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidatId
		rf.heartbeatNotify <- true
	} else {
		//否则，投否决票，并将term更新为自己的term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) hasLog(logIndex, logTerm int) bool {
	log := rf.log
	return logIndex < len(log) && log[logIndex].Term == logTerm
}

//candidate或follower响应leader的AppendEntries请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.heartbeatNotify <- true

	// 当rpc请求方term大于自己term时，立马转变为follower，并同步自己的term信息
	if args.Term > rf.currentTerm {
		rf.turnFollower(args.Term, args.LeaderId)
	}

	//收敛状态，统一转换为follower进行处理
	if rf.state == Candidate {
		rf.turnFollower(rf.currentTerm, args.LeaderId)
	}

	//日志一致性check
	//如果follower不包含preLogIndex和preLogTerm，则响应失败，表示日志不匹配
	if !rf.hasLog(args.PreLogIndex, args.PreLogTerm) {
		//截断与leader不一致的日志 （论文提到这里的截断日志的开销，可优化，但是优化收益并不大，见5.3最后）
		end := minInt(args.PreLogIndex, len(rf.log))
		rf.log = rf.log[:end]
		reply.Term = args.Term
		reply.Success = false
		return
	}

	//追加新日志
	for _, item := range args.Entries {
		rf.log = append(rf.log, item)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Term = args.Term
	reply.Success = true
}

func (rf *Raft) apply(logs []LogEntry, start, end int) {
	for i := start; i <= end; i++ {
		debug("======> server %d:index %d and command %v", rf.me, i, logs[i].Command)
		rf.applyCh <- ApplyMsg{
			Index:   i,
			Command: logs[i].Command,
		}
	}
}

func minInt(a ...int) int {
	min := math.MaxInt64
	for _, i := range a {
		if i < min {
			min = i
		}
	}

	return min
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.state == Leader
	term = rf.currentTerm
	index = len(rf.log)

	//如果不是leader，不可发送appendEntries消息，提前返回false
	if !isLeader {
		return index, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    term,
	})

	return index, term, isLeader
}

func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			rf.matchIndex[i]++
			rf.nextIndex[i]++
		} else if rf.nextIndex[i] <= len(rf.log)-1 {
			go func(follower, leader, leaderTerm, leaderCommit, preLogIndex, preLogTerm int, appendLogs []LogEntry) {
				req := AppendEntriesArgs{
					Term:         leaderTerm,
					LeaderId:     leader,
					PreLogIndex:  preLogIndex,
					PreLogTerm:   preLogTerm,
					Entries:      appendLogs,
					LeaderCommit: leaderCommit,
				}
				resp := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(follower, &req, &resp); ok {
					rf.mu.Lock()
					if resp.Success {
						rf.matchIndex[follower] = preLogIndex + len(appendLogs)
						rf.nextIndex[follower] = rf.matchIndex[follower] + 1

						for n := leaderCommit + 1; n < len(rf.log); n++ {

							var replicas int

							for m := range rf.peers {
								if rf.matchIndex[m] >= n {
									//debug("log %d is match in server %d", n, m)
									replicas++
								}
							}

							debug("replicas of log %d is %d", n, replicas)

							if replicas > len(rf.peers)/2 && rf.log[n].Term == leaderTerm {
								rf.commitIndex = n
							}

							replicas = 0
						}

					} else if resp.Term == leaderTerm {
						//如果rpc请求响应ok，但是response.Success为false并且leader term没有过期，
						// 则表示日志的一致性check检测到冲突，就将nextIndex减一
						rf.nextIndex[follower]--
					}
					rf.mu.Unlock()
				}
			}(i, rf.me, rf.currentTerm, rf.commitIndex, rf.nextIndex[i]-1, rf.log[rf.nextIndex[i]-1].Term, rf.log[rf.nextIndex[i]:])
		} else {
			go func(follower, leader, leaderTerm, leaderCommit int) {
				req := AppendEntriesArgs{
					Term:         leaderTerm,
					LeaderId:     leader,
					LeaderCommit: leaderCommit,
				}
				resp := AppendEntriesReply{
				}
				rf.sendAppendEntries(follower, &req, &resp)

				if resp.Term > leaderTerm {
					rf.mu.Lock()
					rf.turnFollower(resp.Term, leader)
					rf.mu.Unlock()
				}
			}(i, rf.me, rf.currentTerm, rf.commitIndex)
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//定期执行precheck可以保证所有committed的日志都会被apply
func (rf *Raft) preCheck() {
	rf.mu.Lock()
	commitIndex := rf.commitIndex
	lastApplied := rf.lastApplied
	log := rf.log
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()

	//提前保存所有变量，就可以减少持有锁的时间
	//debug("-----------> server %d:commitIndex %d and lastApplied %v", rf.me, commitIndex, lastApplied)
	if commitIndex > lastApplied {
		go rf.apply(log, lastApplied+1, commitIndex)
	}
}

func (rf *Raft) server() {
	for {
		rf.preCheck()
		switch rf.SyncState() {
		case Leader:
			rf.serverAsLeader()
		case Candidate:
			rf.serverAsCandidate()
		case Follower:
			rf.serverAsFollower()
		}
	}
}

const EnableDebug = false

func debug(format string, a ...interface{}) {
	if EnableDebug {
		fmt.Printf(format+"\n", a...)
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(150)+150)
}

func (rf *Raft) synctElectionTimeout() time.Duration {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.electionTimeout
}

func (rf *Raft) serverAsLeader() {
	rf.broadcastAppendEntries()
	time.Sleep(50 * time.Millisecond)
}

func (rf *Raft) serverAsCandidate() {
	rf.broadcastRequestVotes()
	select {
	case <-time.Tick(rf.synctElectionTimeout()):
		rf.mu.Lock()
		debug("candicate elect timeout")
		rf.turnCandidate()
		rf.mu.Unlock()
	case <-rf.heartbeatNotify:
		//收到通知，发现心跳
	case <-rf.electLeaderNotify:
		//收到通知，发现状态变为leader
	}
}

func (rf *Raft) serverAsFollower() {
	select {
	case <-time.Tick(rf.synctElectionTimeout()):
		rf.mu.Lock()
		rf.turnCandidate()
		rf.mu.Unlock()
	case <-rf.voteNotify:
		//收到投票请求，状态不变
	case <-rf.heartbeatNotify:
		//收到心跳请求，状态不变
	}
}
func (raft *Raft) isHeartbeat(args AppendEntriesArgs) bool {
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) turnCandidate() {
	rf.currentTerm++    //inc currentTerm
	rf.votedFor = rf.me //vote for selft
	rf.votedCount = 1
	rf.resetElectionTimeout()
	rf.state = Candidate
	debug("====>[%d] %d server as candidate", rf.currentTerm, rf.me)
}

func (rf *Raft) turnFollower(targetTerm, leaderId int) {
	rf.currentTerm = targetTerm
	rf.state = Follower
	rf.votedCount = 0
	rf.votedFor = -1
	rf.leaderId = leaderId
	debug("====>[%d] %d server as follower", rf.currentTerm, rf.me)
}

func (rf *Raft) turnLeader() {
	rf.state = Leader
	debug("====>[%d] %d server as leader", rf.currentTerm, rf.me)
	//重新初始化leader维护的一些基本信息
	rf.reinitialize()
}

func (rf *Raft) reinitialize() {
	for i := range rf.peers {
		//初始化为last log index +1，也就是日志的长度
		rf.nextIndex[i] = len(rf.log)
		//初始化为0
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) broadcastRequestVotes() {
	for i := range rf.peers {
		if i != rf.me {
			//只有请求成功再计算票数
			go func(target, candidate, candidateTerm, candidateLastLogIndex, candidateLastLogTerm int) {

				req := RequestVoteArgs{
					Term:         candidateTerm,
					CandidatId:   candidate,
					LastLogIndex: candidateLastLogIndex,
					LastLogTerm:  candidateLastLogTerm,
				}
				var resp RequestVoteReply
				ok := rf.sendRequestVote(target, &req, &resp)
				rf.mu.Lock()
				if rf.state == Candidate {
					if ok && resp.VoteGranted {
						rf.votedCount++
					}
					if rf.votedCount > len(rf.peers)/2 {
						rf.turnLeader()
						rf.electLeaderNotify <- true
					}
				}
				rf.mu.Unlock()

			}(i, rf.me, rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)

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
	rf.leaderId = NoLeader
	rf.turnFollower(0, NoLeader)
	rf.resetElectionTimeout()
	rf.electLeaderNotify = make(chan bool)
	rf.heartbeatNotify = make(chan bool, len(rf.peers))
	rf.voteNotify = make(chan bool, len(rf.peers))
	rf.log = []LogEntry{{}} //初始化一个空日志，保证真正的第一个日志的索引为1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	go rf.server()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

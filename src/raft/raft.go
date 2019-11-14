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
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

/*
P1 当一个server crash又回来时,从哪里初始化？
P2 新的config的线程和旧的config的线程似乎会相互影响？？？？ 用布尔变量判断不能有效判断结束???

*/

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
	Term    int         //term when the entry is created
	Index   int         //index of the log entry
	Command interface{} //command from clients
}

//
// A Go object implementing a single Raft peer.
//

const StLeader int = 0
const StCandidate int = 1
const StFollower int = 2
const TimeHeartBeat int = 50
const ElectionBase int = 250
const ElectionRange int = 250

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd //peers 里 记录的名称是这个raft为其它raft server取的名称 也就是信道名称的集合
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int

	//persistent state for all servers
	currentTerm     int
	votedFor        int //index of the voted server,initialize to -1
	log             []LogEntry
	electionTimeOut int

	//volatile states for all servers
	commitIndex int
	lastApplied int

	//timer for electiontimeout
	Timer *time.Timer

	//applyMsg channel
	applyChan chan ApplyMsg

	//volatile states for leaders
	nextIndex  []int
	matchIndex []int

	//if killed,set false
	//alive bool
	alive chan int
}

func (rf *Raft) PrintState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("id:", rf.me, " ", "term:", rf.currentTerm, " ", "state:", rf.state, " timeout:", rf.electionTimeOut)
}
func test_term(s string) {
	fmt.Println(s, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = (rf.state == StLeader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.electionTimeOut)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Println(rf.me, " reads persisted state!")

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.electionTimeOut)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	EndIndex     int
	LastlogIndex int
	LastlogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int //leader tem
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	// 重要：当appendentries中有[]logentry时 其它量会被清零
	Entries []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Id      int // server id
	Index   int // the latest index that matches
}

//RAFT对象与其他raft对象交互时，应该使用interface{}作为标示，内部状态使用int
// 代码满足所有raft对象的peer序列一致，那么使用int
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()

	fmt.Println(rf.me, "recieved a requestvote!", "myterm:", rf.currentTerm, " term:", args.Term, " candidate:", args.EndIndex,
		" lastindex:", args.LastlogIndex, " lastterm:", args.LastlogTerm)

	reply.VoteGranted = true
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		/*
			change state to follower
		*/
		rf.state = StFollower
		rf.votedFor = -1

	} else {
		reply.Term = args.Term
		if rf.votedFor != -1 && rf.votedFor != args.EndIndex {
			reply.VoteGranted = false
		}
	}
	if reply.VoteGranted {
		if len(rf.log) == 0 {
			reply.VoteGranted = true
		} else {
			l := len(rf.log) - 1
			if rf.log[l].Term > args.LastlogTerm || ((rf.log[l].Term == args.LastlogTerm) && (l > args.LastlogIndex)) {
				reply.VoteGranted = false
			}
		}
	}
	if reply.VoteGranted {
		rf.votedFor = args.EndIndex
	}
	rf.mu.Unlock()

	// 同意投票时，需要重置electionTimeOut

	if reply.VoteGranted {
		fmt.Println(rf.me, " grands ", args.EndIndex, "'s requestvote,term:", args.Term)
		rf.resetTimer()
	}
	//返回前写磁盘
	rf.persist()
}

//example AppendEntries RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	reply.Index = -1
	reply.Id = rf.me
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		//	fmt.Println(rf.me,"recieved a heartbeat!","myterm:",rf.currentTerm," term:",args.Term," leader:",args.LeaderId,
		//	" prevLogIndex:",args.PrevLogIndex," prevLogTerm:",args.PrevLogTerm)
		rf.mu.Unlock()
		return
	}
	//fmt.Println(rf.me," He! recieved a heartbeat!","myterm:",rf.currentTerm," term:",args.Term," leader:",args.LeaderId,
	//	" prevLogIndex:",args.PrevLogIndex," prevLogTerm:",args.PrevLogTerm)

	reply.Term = args.Term
	if args.Term > rf.currentTerm {
		/*han replyMsg)han replyMsg)han rhan replyMsg)han replyMsg)eplyMsg)
		change state to follower
		*/
		rf.state = StFollower
		rf.currentTerm = args.Term
	}
	/*检查prevLogIndex 与 prevLogTerm */
	b := false
	index := 0
	for i, x := range rf.log {
		if x.Term == args.PrevLogTerm && i == args.PrevLogIndex {
			b = true
			index = i
			reply.Index = i
		}
	}
	//第一个log entry
	if args.PrevLogIndex == -1 && args.PrevLogTerm == -1 {
		b = true
	}
	if !b {
		reply.Success = false
	} else {
		reply.Success = true
	}
	if b && args.Entries != nil {

		if args.PrevLogTerm == -1 && args.PrevLogIndex == -1 {
			rf.log = rf.log[:0]
			for _, e := range args.Entries {
				rf.log = append(rf.log, e)
			}
			reply.Index = len(args.Entries) - 1
		} else {
			index++
			rf.log = rf.log[:index]
			for _, e := range args.Entries {
				rf.log = append(rf.log, e)
			}
			reply.Index = len(rf.log) - 1
		}
	}
	if b {

		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > len(rf.log)-1 {
				rf.commitIndex = len(rf.log) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}
		//将commited的log entry应用到本地
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{}
				//实验下标似乎从1开始
				msg.Index = i + 1
				msg.Command = rf.log[i].Command
				rf.applyChan <- msg
			}
			rf.lastApplied = rf.commitIndex
		}
	}
	rf.mu.Unlock()

	fmt.Print(rf.me, " finished append,term:", rf.currentTerm, " commitIndex: ", rf.commitIndex, " lastApplied: ", rf.lastApplied)
	for i := 0; i < len(rf.log); i++ {
		fmt.Print(rf.log[i], " ")
	}
	fmt.Println()

	rf.resetTimer()
	// 返回前写磁盘
	rf.persist()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := (rf.state == StLeader)
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	//append the new command to leader's log
	fmt.Println(rf.me, " add a new log entry term: ", rf.currentTerm, " index: ", len(rf.log), " command: ", command)
	entry := LogEntry{}
	entry.Command = command
	entry.Index = len(rf.log)
	entry.Term = rf.currentTerm
	rf.log = append(rf.log, entry)
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
	rf.mu.Unlock()
	rf.persist()
	return index + 1, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
//新的config的线程和旧的config的线程似乎会相互影响？？？？
func (rf *Raft) Kill() {
	//fmt.Println(rf.me,"killed")
	//rf.alive=false
	rf.alive <- 1
}
func (rf *Raft) resetTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(rf.me,"timer reseted!")
	rf.Timer.Reset(time.Duration(rf.electionTimeOut) * time.Millisecond)
}
func (rf *Raft) makeAppendEntriesArgs(target int) AppendEntriesArgs {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	// 若nextIndex是0 则prevLogIndex和prevLogTerm为-1
	args.PrevLogIndex = rf.nextIndex[target] - 1
	if args.PrevLogIndex == -1 {
		args.PrevLogTerm = -1
	} else {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	//保证leader的log的长度等于logentry的个数
	if len(rf.log) > rf.nextIndex[target] {
		entries := make([]LogEntry, len(rf.log)-rf.nextIndex[target])
		copy(entries, rf.log[rf.nextIndex[target]:])
		args.Entries = entries
	} else {
		args.Entries = nil
	}
	return args
}

func (rf *Raft) leaderInit() {
	//fmt.Println("implement me: leaderInit")
	//初始化nextIndex与matchIndex

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		//初始化为-1
		rf.matchIndex[i] = -1
	}
}
func (rf *Raft) setCommitIndex() {
	//不要互斥 因为已经在调用的地方互斥
	com := rf.commitIndex
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {
		if rf.log[i].Term < rf.currentTerm {
			continue
		}
		cnt := 0
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i {
				cnt++
			}
		}
		if cnt >= len(rf.peers)/2+1 {
			com = i
		}
	}
	rf.commitIndex = com
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{}
			//实验测试似乎下标从1开始
			msg.Index = i + 1
			msg.Command = rf.log[i].Command
			rf.applyChan <- msg
		}
		rf.lastApplied = rf.commitIndex
	}
	fmt.Print(rf.me, " LEADER STATUS:", rf.currentTerm, " commitIndex: ", rf.commitIndex, " lastApplied: ", rf.lastApplied)
	for i := 0; i < len(rf.log); i++ {
		fmt.Print(rf.log[i], " ")
	}
	fmt.Println()
}

/*the raft server behavior*/
// 互斥问题
func (rf *Raft) Waiting() {
	// leader 50ms 发送一次heartbeat信号
	heartbeat := time.NewTimer(time.Duration(TimeHeartBeat) * time.Millisecond)
	rf.resetTimer()
	fmt.Println(rf.me, " am coming!state: ", rf.state, " term: ", rf.currentTerm)
	for {
		select {
		case <-rf.Timer.C:
			// 这里使用break跳出循环
			if rf.state != StFollower {
				//	fmt.Print(rf.me,"leader")
				rf.resetTimer()
				break
			}
			fmt.Println(rf.me, " want to enter!!!")
			rf.mu.Lock()
			fmt.Println("TT:", rf.currentTerm)

			rf.state = StCandidate
			rf.currentTerm++
			fmt.Println("TTB:", rf.currentTerm)
			rf.votedFor = rf.me
			fmt.Println(rf.me, " starts sending requestvote msg term:", rf.currentTerm)
			ct, li := rf.currentTerm, len(rf.log)-1
			lt := -1
			if li != -1 {
				lt = rf.log[len(rf.log)-1].Term
			}
			cnt := 1
			rf.mu.Unlock()
			rf.resetTimer()

			start := time.Now() // 获取当前时间
			//采用buffered channel
			c := make(chan *RequestVoteReply, len(rf.peers))

			tc := false
			tm := sync.Mutex{}

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				//并发地发送requestVote
				go func(ch chan *RequestVoteReply, s int) {

					args := RequestVoteArgs{}
					args.Term = ct
					args.EndIndex = rf.me
					args.LastlogIndex = li
					args.LastlogTerm = lt
					reply := &RequestVoteReply{}
					fmt.Println("Vote Msg:", "id:", args.EndIndex, "term: ", args.Term, " Lastlogindex: ", args.LastlogIndex,
						" LastLogTerm:", args.LastlogTerm)

					//若无法到达则重复发送，最多持续electiontimeout时间
					timeout := time.After(time.Millisecond * time.Duration(rf.electionTimeOut-TimeHeartBeat))
					suc := false
					for !suc {
						select {
						case <-timeout:
							return
						default:
							suc = rf.sendRequestVote(s, args, reply)
						}
					}
					if suc {
						tm.Lock()
						if !tc {
							ch <- reply
						}
						tm.Unlock()
					}
				}(c, i)
			}
			//等待rf.electiontimeout-10的时间 然后关闭channel
			//time.Sleep(time.Millisecond * time.Duration(rf.electionTimeOut-TimeHeartBeat))
			//等待最短时间
			time.Sleep(time.Millisecond * time.Duration(ElectionBase-TimeHeartBeat))
			tm.Lock()
			tc = true
			tm.Unlock()
			close(c)

			cost := time.Since(start)
			fmt.Printf("election cost=[%s]\n", cost)

			rf.mu.Lock()
			for rep := range c {
				if rep.Term > rf.currentTerm {
					rf.currentTerm = rep.Term
					rf.state = StFollower
					rf.votedFor = -1
				} else {
					if rep.VoteGranted {
						cnt++
						// 得到多数的投票
						if cnt >= len(rf.peers)/2+1 {
							break
						}
					}
				}
			}
			if rf.state == StFollower || cnt < len(rf.peers)/2+1 {
				/* 竞争失败*/
				rf.state = StFollower
			}
			// 竞争成功
			if rf.state == StCandidate && cnt >= len(rf.peers)/2+1 {
				rf.state = StLeader
				/*
					leader的初始化
				*/
				rf.leaderInit()
			}
			rf.mu.Unlock()

			// leader发送heartbeat信号
			// !!!!!这里有超时问题
		case <-heartbeat.C:
			heartbeat.Reset(time.Duration(TimeHeartBeat) * time.Millisecond)
			if rf.state != StLeader {
				break
			}
			fmt.Println(rf.me, " starts sending heartbeat term: ", rf.currentTerm)
			start := time.Now() // 获取当前时间

			// buffered channel
			c := make(chan *AppendEntriesReply, len(rf.peers))

			//同步子线程调用完成
			//com := make(chan int)
			timeout := false
			tm := sync.Mutex{}

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				/*
						//并发地发送heartbeat msg
						// 用定时器似乎不能限制函数的执行时间
						go func(ch chan *AppendEntriesReply,s int) {

							timeout := time.After(time.Millisecond *time.Duration(TimeHeartBeat/2))
							args := rf.makeAppendEntriesArgs(false, s)
							reply := &AppendEntriesReply{}
							for{
								select{
									case <-timeout:
										return
									default:
										if rf.sendAppendEntries(s, args, reply) {
											ch <- reply
											return
									}
								}
							}
						}(c,i)
					}
					//等待60ms 关闭channel
					time.Sleep(time.Duration(TimeHeartBeat/2+10) * time.Millisecond)
					close(c)
				*/
				// 这里无法用时钟控制函数的执行时间
				/*
						go func(ch chan *AppendEntriesReply,s int,com chan int) {
							arg := rf.makeAppendEntriesArgs(s)
							fmt.Println("Heart Msg:","id:",arg.LeaderId," term:",arg.Term," lastindex:",arg.PrevLogIndex," lastterm:",
								arg.PrevLogTerm)
							clock:=time.NewTimer(time.Millisecond*time.Duration(TimeHeartBeat))
							select {
							case <- clock.C:
								com <- 1
								return
							default:
								reply := &AppendEntriesReply{}
								if rf.sendAppendEntries(s, arg, reply) {
									ch <- reply
								}
								com <- 1
								return
							}
						}(c,i,com)
					}
					//等待所有线程结束
					for i:=0;i<len(rf.peers)-1;i++{
						<- com
					}
				*/
				go func(ch chan *AppendEntriesReply, s int) {
					arg := rf.makeAppendEntriesArgs(s)
					fmt.Println("Heart Msg:", "id:", arg.LeaderId, " term:", arg.Term, " lastindex:", arg.PrevLogIndex, " lastterm:",
						arg.PrevLogTerm)
					reply := &AppendEntriesReply{}
					if rf.sendAppendEntries(s, arg, reply) {
						tm.Lock()
						if !timeout {
							ch <- reply
						}
						tm.Unlock()
					}
				}(c, i)
			}
			//等待timeheartbeat时间 当主线程都退出时，此时关闭不会有panic
			time.Sleep(time.Duration(TimeHeartBeat) * time.Millisecond)
			tm.Lock()
			timeout = true
			tm.Unlock()
			close(c)

			cost := time.Since(start)
			fmt.Printf("heartbeat cost=[%s]\n", cost)

			rf.mu.Lock()
			for rep := range c {
				if rep.Term > rf.currentTerm {
					rf.currentTerm = rep.Term
					rf.state = StFollower
					rf.votedFor = -1
					break
				} else {
					if rep.Success {
						rf.nextIndex[rep.Id] = rep.Index + 1
						rf.matchIndex[rep.Id] = rep.Index
					} else {
						rf.nextIndex[rep.Id]--
					}
				}
			}
			if rf.state == StLeader {
				rf.setCommitIndex()
			}
			rf.mu.Unlock()
		case <-rf.alive:
			fmt.Println(rf.me, "i am gone")
			return
		default:
			//用布尔变量判断线程结束会出问题？？？？？？？
			//检查服务器是否crash,若是，则退出线程
			//fmt.Println(rf.me,"check")
			/*
				if !rf.alive{
					fmt.Println(rf.me,"i am gone")
					return
				}
			*/
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

	// Your initialization code here.

	//become follower after a reboot
	rf.state = StFollower

	//初始化为-1
	rf.commitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	if persister.ReadRaftState() != nil {
		fmt.Println("read persisit state")
		rf.readPersist(persister.ReadRaftState())
	} else {
		fmt.Println("persister: nil")
		rf.currentTerm = 0
		rf.votedFor = -1
		//electiontimeout : 300-600ms
		rf.electionTimeOut = rand.Int()%ElectionRange + ElectionBase
		rf.persist()
	}

	rf.applyChan = applyCh
	//rf.alive=true
	rf.alive = make(chan int)
	rf.Timer = time.NewTimer(time.Duration(rf.electionTimeOut) * time.Millisecond)
	//fmt.Println(rf.me,"construction completed!")
	go rf.Waiting()

	return rf
}

package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format+"\n", a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string
	ClientId int64
	Seq      int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database  map[string]string
	clientSeq map[int64]int //为每个client维护的最大消息seq，通过不接受较低seq来去除重复request，保证request幂等性
	opDone    chan string
	noopDone  chan string
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("%d:server get %s start====", kv.me, args.Key)
	defer DPrintf("%d:server get %s done====", kv.me, args.Key)
	// Your code here.
	if _, _, isLeader := kv.rf.Start(kv.GetCommand(args)); !isLeader {
		reply.WrongLeader = true
		reply.Err = "Not leader"
		leaderId, _ := kv.rf.GetLeader()
		reply.LeaderId = leaderId
		return
	}
	DPrintf("%d:prepare get value by key %s", kv.me, args.Key)
	select {
	case val := <-kv.noopDone:
		DPrintf("%d:Start get value by key %s", kv.me, args.Key)
		reply.WrongLeader = false
		reply.Value = val
		reply.Err = OK
	case <-time.After(CommandTimeout * time.Millisecond):
		DPrintf("%d:Failed get value by key %s", kv.me, args.Key)
		reply.Err = "timeout"
		reply.WrongLeader = false
	}
}

func (kv *RaftKV) GetCommand(args *GetArgs) Op {
	return Op{
		Key:      args.Key,
		ClientId: args.ClientId,
	}
}

func (kv *RaftKV) PutAppendCommand(args *PutAppendArgs) Op {
	return Op{
		Key:      args.Key,
		Value:    args.Value,
		Op:       args.Op,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("%d:server %s %s %s start====", kv.me, args.Op, args.Key, args.Value)
	defer DPrintf("%d:server %s %s %s done====", kv.me, args.Op, args.Key, args.Value)
	// Your code here.
	kv.mu.Lock()
	seq, seqExist := kv.clientSeq[args.ClientId]
	kv.mu.Unlock()
	if seqExist && args.Seq <= seq {
		reply.WrongLeader = false
		reply.Err = OK
		return
	}

	if _, _, isLeader := kv.rf.Start(kv.PutAppendCommand(args)); !isLeader {
		reply.WrongLeader = true
		reply.Err = "Not leader"
		leaderId, _ := kv.rf.GetLeader()
		reply.LeaderId = leaderId
		return
	}
	DPrintf("%d:prepare set value for key %s", kv.me, args.Key)
	select {
	case <-kv.opDone:
		DPrintf("%d:start set value for key %s", kv.me, args.Key)
		reply.WrongLeader = false
		reply.Err = OK
	case <-time.After(CommandTimeout * time.Millisecond):
		DPrintf("%d:failed set value for key %s", kv.me, args.Key)
		reply.WrongLeader = false
		reply.Err = "timeout"
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.clientSeq = make(map[int64]int)
	kv.opDone = make(chan string)
	kv.noopDone = make(chan string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go func() {
		for {
			msg := (<-kv.applyCh).Command.(Op)

			if msg.Op == "Put" || msg.Op == "Append" {
				kv.mu.Lock()
				if seq, ok := kv.clientSeq[msg.ClientId]; !ok || seq < msg.Seq {
					kv.clientSeq[msg.ClientId] = msg.Seq

					if msg.Op == "Put" {
						kv.database[msg.Key] = msg.Value
					} else {
						kv.database[msg.Key] += msg.Value
					}

				}

				kv.mu.Unlock()
				if _, isLeader := kv.rf.GetLeader(); isLeader {
					kv.opDone <- msg.Value
				}
			} else {
				DPrintf("no op come")
				kv.mu.Lock()
				val := kv.database[msg.Key]
				kv.mu.Unlock()
				if _, isLeader := kv.rf.GetLeader(); isLeader {
					kv.noopDone <- val
				}
			}
			DPrintf("me %d Notify key value [%s:%s]", kv.me, msg.Key, msg.Value)
		}
	}()

	return kv
}

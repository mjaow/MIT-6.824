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
	Key         string
	Value       string
	Op          string
	ClientId    int64
	Seq         int
	Term        int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database  map[string]string
	clientSeq map[int64]int   //为每个client维护的最大消息seq，通过不接受较低seq来去除重复request，保证request幂等性
	opDone    map[int]chan Op //为每个消息维护一个channel，每当这个消息被提交，op->opDone

	done bool //server端任务是否完结
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if index, term, isLeader := kv.rf.Start(kv.GetCommand(args)); !isLeader {
		reply.WrongLeader = true
	} else {
		kv.mu.Lock()
		done := kv.getOpFromMap(index)
		kv.mu.Unlock()
		select {
		case op := <-done:
			reply.WrongLeader = op.Term != term
			reply.Value = op.Value
			reply.Err = OK
		case <-time.After(CommandTimeout * time.Millisecond):
		}
	}
}

func (kv *RaftKV) GetCommand(args *GetArgs) Op {
	return Op{
		Key:      args.Key,
		ClientId: args.ClientId,
		Op:       Get,
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

func (kv *RaftKV) getOpFromMap(index int) chan Op {
	if op, ok := kv.opDone[index]; !ok {
		op = make(chan Op, 1)
		kv.opDone[index] = op
		return op
	} else {
		return op
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	seq, seqExist := kv.clientSeq[args.ClientId]
	kv.mu.Unlock()
	if seqExist && args.Seq <= seq {
		reply.Err = OK
		return
	}

	if index, term, isLeader := kv.rf.Start(kv.PutAppendCommand(args)); !isLeader {
		reply.WrongLeader = true
	} else {
		kv.mu.Lock()
		done := kv.getOpFromMap(index)
		kv.mu.Unlock()
		select {
		case op := <-done:
			reply.WrongLeader = op.Term != term
			reply.Err = OK
		case <-time.After(CommandTimeout * time.Millisecond):
		}
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.done = true
}

func (kv *RaftKV) isDone() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.done
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
	kv.opDone = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.done = false

	// You may need initialization code here.

	go func() {
		for !kv.isDone() {
			msg := <-kv.applyCh
			index := msg.Index
			op := msg.Command.(Op)
			term, _ := kv.rf.GetState()
			op.Term = term
			kv.mu.Lock()
			done := kv.getOpFromMap(index)
			if op.Op == Get {
				DPrintf("no op come")
				op.Value = kv.database[op.Key]
			} else if seq, ok := kv.clientSeq[op.ClientId]; !ok || seq < op.Seq {
				kv.clientSeq[op.ClientId] = op.Seq
				if op.Op == Put {
					kv.database[op.Key] = op.Value
				} else {
					kv.database[op.Key] += op.Value
				}
			}
			kv.mu.Unlock()
			done <- op
		}
	}()

	return kv
}

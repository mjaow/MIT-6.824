package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	//由于client很多而server很少，所以使用int64来修饰clientid，int来修饰serverid
	id             int64 //client id，生成大随机数来代表
	seq            int   //消息序列号，属于该client的消息唯一标示符
	possibleLeader int   //概率性较高的leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientId: ck.id,
	}
	var i int
	for {
		i = ck.getLeader()
		reply := GetReply{}

		if ok := ck.servers[i].Call("RaftKV.Get", &args, &reply); !ok || reply.WrongLeader {
			ck.updateLeader((i + 1) % len(ck.servers))
			continue
		}

		if reply.Err != OK {
			continue
		}

		ck.updateLeader(i)
		return reply.Value
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.id,
		Seq:      ck.getAndIncSeq(1),
	}
	var i int
	for {
		i = ck.getLeader()
		reply := PutAppendReply{}

		if ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply); !ok || reply.WrongLeader {
			ck.updateLeader((i + 1) % len(ck.servers))
			continue
		}

		if reply.Err != OK {
			continue
		}

		ck.updateLeader(i)
		return
	}
}

func (ck *Clerk) updateLeader(leaderId int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.possibleLeader = leaderId
}

func (ck *Clerk) getLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.possibleLeader
}

func (ck *Clerk) getAndIncSeq(diff int) int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	tmp := ck.seq
	ck.seq += diff
	return tmp
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, Append)
}

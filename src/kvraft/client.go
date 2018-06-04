package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	sr "math/rand"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	//由于client很多而server很少，所以使用int64来修饰clientid，int来修饰serverid
	id             int64 //client id，生成大随机数来代表
	lastSeenLeader int   //记录最近一次看到订单leader id

	seq int //消息序列号，属于该client的消息唯一标示符
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
	ck.lastSeenLeader = NoneLeader
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
	DPrintf("client get %s start====", key)
	defer DPrintf("client get %s done====", key)
	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientId: ck.id,
	}
	for {
		ck.nextLeader()
		reply := GetReply{}

		DPrintf("peek servers %d", ck.lastSeenLeader)

		if !ck.servers[ck.lastSeenLeader].Call("RaftKV.Get", &args, &reply) {
			ck.lastSeenLeader = NoneLeader
			continue
		}

		if reply.WrongLeader {
			ck.lastSeenLeader = reply.LeaderId
			continue
		}

		if reply.Err != OK {
			ck.lastSeenLeader = NoneLeader
			continue
		}

		return reply.Value
	}
	return ""
}

func (ck *Clerk) nextLeader() {
	//if ck.lastSeenLeader == NoneLeader {
		//用simple random generator猜测leader id
		ck.lastSeenLeader = sr.Intn(len(ck.servers))
	//}
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
	DPrintf("client %s %s %s start====", op, key, value)
	defer DPrintf("client %s %s %s done====", op, key, value)
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.id,
		Seq:      ck.seq,
	}
	for {
		ck.nextLeader()
		reply := PutAppendReply{}

		if !ck.servers[ck.lastSeenLeader].Call("RaftKV.PutAppend", &args, &reply) {
			ck.lastSeenLeader = NoneLeader
			continue
		}

		if reply.WrongLeader {
			ck.lastSeenLeader = reply.LeaderId
			continue
		}

		if reply.Err != OK {
			ck.lastSeenLeader = NoneLeader
			continue
		}

		ck.seq++
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

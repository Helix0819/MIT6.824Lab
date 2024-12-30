package kvsrv

import (
	"crypto/rand"
	"math/big"
	"os"
	"strconv"

	"6.5840/labrpc"
)

type Clerk struct {
	server    *labrpc.ClientEnd
	clientId  string // 使用 nrand() 生成唯一ID
	seq       int64  // 严格递增的序列号
	lastReply string // 记录上一次操作的结果
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientId = strconv.FormatInt(nrand(), 10) // 使用随机数生成唯一ID
	ck.seq = 0
	return ck
}

func getWorkerId() string {
	return strconv.Itoa(os.Getpid())
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.seq++ // 确保序列号严格递增
	args := GetArgs{
		Key:       key,
		RequestId: ck.clientId + strconv.FormatInt(ck.seq, 10),
	}
	reply := GetReply{}

	// for {
	// 	if ok := ck.server.Call("KVServer.Get", &args, &reply); ok {
	// 		ck.lastReply = reply.Value
	// 		value := reply.Value
	// 		return value
	// 	}
	// 	DPrintf("Get failed, Retrying... seq=%d", ck.seq)
	// }
	for !ck.server.Call("KVServer.Get", &args, &reply) {
		DPrintf("Get failed, Retrying... seq=%d", ck.seq)
	}
	ret := reply.Value
	return ret
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.seq++ // 确保序列号严格递增
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		RequestId: ck.clientId + strconv.FormatInt(ck.seq, 10),
	}
	reply := PutAppendReply{}

	// for {
	// 	if ok := ck.server.Call("KVServer."+op, &args, &reply); ok {
	// 		ck.lastReply = reply.Value
	// 		value := reply.Value
	// 		args.MessageType = "ReportType"
	// 		ck.server.Call("KVServer."+op, &args, &reply)

	// 		return value
	// 	}
	for !ck.server.Call("KVServer."+op, &args, &reply) {
		DPrintf("%s failed, Retrying... seq=%d", op, ck.seq)
	}

	ret := reply.Value

	args.MessageType = "ReportType"
	for !ck.server.Call("KVServer."+op, &args, &reply) {
		DPrintf("%s failed, Retrying... seq=%d", op, ck.seq)
	}
	// DPrintf("%s failed, Retrying... seq=%d", op, ck.seq)
	return ret
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

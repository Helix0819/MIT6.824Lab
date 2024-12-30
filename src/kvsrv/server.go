package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]string
	// Cache results per client request
	results map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if args.MessageType == "ReportType" {
	// 	delete(kv.results, args.RequestId)
	// 	return
	// }
	// // Check for duplicate request
	// if result, ok := kv.results[args.RequestId]; ok {
	// 	reply.Value = result
	// 	return
	// }

	value, ok := kv.data[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}

	// kv.results[args.RequestId] = reply.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.MessageType == "ReportType" {
		delete(kv.results, args.RequestId)
		DPrintf("Delete request id %s", args.RequestId)
		return
	}
	// Check for duplicate request
	if result, ok := kv.results[args.RequestId]; ok {
		reply.Value = result
		return
	}

	kv.data[args.Key] = args.Value
	reply.Value = args.Value

	// Cache result

	kv.results[args.RequestId] = reply.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.MessageType == "ReportType" {
		delete(kv.results, args.RequestId)
		DPrintf("Delete request id %s", args.RequestId)
		return
	}
	// Check for duplicate request
	if result, ok := kv.results[args.RequestId]; ok {
		reply.Value = result
		return
	}

	oldValue, exists := kv.data[args.Key]
	if exists {
		kv.data[args.Key] = oldValue + args.Value
	} else {
		kv.data[args.Key] = args.Value
	}
	reply.Value = oldValue

	// Cache result
	kv.results[args.RequestId] = reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.results = make(map[string]string, 0)
	return kv
}

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

type requestStatus struct {
	SN    int64
	value string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	ssm         map[string]string
	lastRequest map[int64]*requestStatus
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key

	if v, ok := kv.ssm[key]; ok {
		reply.Value = v
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.RepeatTimes != 0 {
		if v, ok := kv.lastRequest[args.ID]; ok {
			if v.SN == args.SN {
				return
			}
		}
	}

	key, value := args.Key, args.Value

	kv.ssm[key] = value
	kv.lastRequest[args.ID] = &requestStatus{SN: args.SN}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.RepeatTimes != 0 {
		if v, ok := kv.lastRequest[args.ID]; ok {
			if v.SN == args.SN {
				reply.Value = v.value

				return
			}
		}
	}

	key, value := args.Key, args.Value

	reply.Value = kv.ssm[key]
	kv.ssm[key] += value
	kv.lastRequest[args.ID] = &requestStatus{SN: args.SN, value: reply.Value}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.ssm = make(map[string]string)
	kv.lastRequest = make(map[int64]*requestStatus)

	return kv
}

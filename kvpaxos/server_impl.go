package kvpaxos

import (

	// "net/rpc"
	"time"

	"usc.edu/csci499/proj3/paxos"
)

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters,
// otherwise RPC will break.
type Op struct {
	Key       string
	Value     string
	Operation string
	ClientID  int64
	Seq       int
}

// additions to KVPaxos state
type KVPaxosImpl struct {
	lastApply    int
	KVMap        map[string]string
	lastApplyLog Op
	maxClientSeq map[int64]int
}

// initialize kv.impl.*
func (kv *KVPaxos) InitImpl() {
	kv.impl = KVPaxosImpl{
		KVMap:        make(map[string]string),
		maxClientSeq: make(map[int64]int),
		lastApply:    -1,
	}
	// fmt.Println("peers: ", kv.impl.peers)

}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("Get key %s", args.Key)

	v := Op{Key: args.Key, Operation: "Get", ClientID: args.Impl.ClientID, Seq: args.Impl.Seq}
	clientID := args.Impl.ClientID
	if maxSeq, ok := kv.impl.maxClientSeq[clientID]; ok && args.Impl.Seq <= maxSeq {
		reply.Err = OK
		reply.Value = kv.impl.KVMap[v.Key]
		return nil
	}
	seq := kv.impl.lastApply + 1
	for {
		kv.rsm.Start(seq, v)
		decidedValue := kv.Wait(seq)
		if v == decidedValue {
			break
		}
		seq++
	}
	for ; kv.impl.lastApply+1 < seq; kv.impl.lastApply++ {
		decidedValue := kv.Wait(kv.impl.lastApply + 1)
		kv.ApplyOp(decidedValue.(Op))
	}

	reply.Value = kv.impl.KVMap[args.Key]
	reply.Err = OK
	kv.rsm.Done(kv.impl.lastApply)
	//log.Printf("Reply %v", reply)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("PutAppend key %s, value %s, op %s", args.Key, args.Value, args.Op)

	v := Op{Key: args.Key, Operation: args.Op, Value: args.Value, ClientID: args.Impl.ClientID, Seq: args.Impl.Seq}
	clientID := args.Impl.ClientID
	if maxSeq, ok := kv.impl.maxClientSeq[clientID]; ok && args.Impl.Seq <= maxSeq {
		// if isApply, ok := kv.isLogApply[v]; ok && isApply {
		reply.Err = OK
		return nil
	}
	seq := kv.impl.lastApply + 1
	for {
		kv.rsm.Start(seq, v)
		decidedValue := kv.Wait(seq)
		DPrintf("PutAppend decidedValue %v", decidedValue)
		if v == decidedValue {
			break
		}
		seq++
	}
	reply.Err = OK
	return nil
}

func (kv *KVPaxos) Wait(seq int) interface{} {
	sleepTime := 10 * time.Microsecond
	for {
		decided, decidedValue := kv.rsm.Status(seq)
		if decided == paxos.Decided {
			return decidedValue
		}
		if decided == paxos.Forgotten {
			break
		}
		time.Sleep(sleepTime)
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
	}
	return nil
}

func (kv *KVPaxos) ApplyOp(op Op) interface{} {
	if op.Operation == "Get" {
		return kv.impl.KVMap[op.Key]
	} else if op.Operation == "Put" {
		kv.impl.KVMap[op.Key] = op.Value
	} else {
		value, ok := kv.impl.KVMap[op.Key]
		if !ok {
			value = ""
		}
		kv.impl.KVMap[op.Key] = value + op.Value
	}
	if maxSeq, ok := kv.impl.maxClientSeq[op.ClientID]; !ok || maxSeq < op.Seq {
		kv.impl.maxClientSeq[op.ClientID] = op.Seq
	}
	// kv.impl.lastApplyLog = op
	// kv.isLogApply[op] = true
	return nil
}

// func (kv *KVPaxos) incrementSeq() int {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	kv.impl.seq++
// 	return kv.impl.seq
// }

// func (kv *KVPaxos) addSeqToCache(id int64, seq int) {
// 	kv.impl.seqCache[id] = seq
// }

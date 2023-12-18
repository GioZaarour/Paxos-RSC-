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
	// kvMap             map[string]string
	// operationsApplied map[int64]bool
	// seq               int
	// seqCache          map[int64]int
	lastApply    int
	kvMap        map[string]string
	lastApplyLog Op
	maxClientSeq map[int64]int
}

// initialize kv.impl.*
func (kv *KVPaxos) InitImpl() {
	kv.impl = KVPaxosImpl{
		//initialize everything
		// operationsApplied: make(map[int64]bool),
		// seqCache:          make(map[int64]int),
		// seq:               0,
		kvMap:        make(map[string]string),
		maxClientSeq: make(map[int64]int),
		lastApply:    -1,
		lastApplyLog: Op{},
	}
	// fmt.Println("peers: ", kv.impl.peers)

}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// op gets passed in as the value to the paxos instance
	op := Op{Key: args.Key, Operation: "Get", ClientID: args.Impl.ID, Seq: args.Impl.Seq}
	clientID := args.Impl.ID
	if maxSeq, ok := kv.impl.maxClientSeq[clientID]; ok && args.Impl.Seq <= maxSeq {
		reply.Err = OK
		reply.Value = kv.impl.kvMap[op.Key]
		return nil
	}
	seq := kv.impl.lastApply + 1
	for {
		kv.rsm.Start(seq, op)
		decidedValue := kv.waitForDecision(seq)
		if op == decidedValue {
			break
		}
		seq++
	}
	// apply all the Ops between the last one applied and the Seq of this Get()
	for ; kv.impl.lastApply+1 < seq; kv.impl.lastApply++ {
		decidedValue := kv.waitForDecision(kv.impl.lastApply + 1)
		kv.Apply(decidedValue.(Op))
	}

	reply.Value = kv.impl.kvMap[args.Key]
	reply.Err = OK
	kv.rsm.Done(kv.impl.lastApply)
	//log.Printf("Reply %v", reply)
	return nil
}

// we don't apply Op in here becuase we can save congestion in the system by only applying Ops once they are Get()
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//DPrintf("PutAppend key %s, value %s, op %s", args.Key, args.Value, args.Op)

	v := Op{Key: args.Key, Operation: args.Op, Value: args.Value, ClientID: args.Impl.ID, Seq: args.Impl.Seq}
	clientID := args.Impl.ID
	//detect duplicate request
	if maxSeq, ok := kv.impl.maxClientSeq[clientID]; ok && args.Impl.Seq <= maxSeq {
		// if isApply, ok := kv.isLogApply[v]; ok && isApply {
		reply.Err = OK
		return nil
	}
	seq := kv.impl.lastApply + 1
	for {
		kv.rsm.Start(seq, v)
		decidedValue := kv.waitForDecision(seq)
		//DPrintf("PutAppend decidedValue %v", decidedValue)
		if v == decidedValue {
			break
		}
		seq++
	}
	reply.Err = OK
	return nil
}

func (kv *KVPaxos) waitForDecision(seq int) interface{} {
	sleepTime := 10 * time.Microsecond
	//fmt.Printf("[%d, waitForDecision]: waiting for paxos decision\n", kv.me)
	for {
		status, decidedValue := kv.rsm.Status(seq)
		if status == paxos.Decided {
			//fmt.Printf("[%d, waitForDecision]: done waiting. value is %v\n", kv.me, decidedValue)
			return decidedValue
		}
		if status == paxos.Forgotten {
			break
		}
		time.Sleep(sleepTime)
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
	}
	//fmt.Printf("[%d, waitForDecision]: donewaiting. value forgotten\n", kv.me)
	return nil
}

func (kv *KVPaxos) Apply(op Op) interface{} {
	if op.Operation == "Get" {
		return kv.impl.kvMap[op.Key]
	} else if op.Operation == "Put" {
		kv.impl.kvMap[op.Key] = op.Value
	} else {
		value, ok := kv.impl.kvMap[op.Key]
		if !ok {
			value = ""
		}
		kv.impl.kvMap[op.Key] = value + op.Value
	}
	if maxSeq, ok := kv.impl.maxClientSeq[op.ClientID]; !ok || maxSeq < op.Seq {
		kv.impl.maxClientSeq[op.ClientID] = op.Seq
	}
	// kv.lastApplyLog = op
	// kv.isLogApply[op] = true
	return nil
}

// // Execute operation encoded in decided value v and update local state
// func (kv *KVPaxos) ApplyOp(v interface{}) {
// 	operation, ok := v.(Op)

// 	if !ok {
// 		return
// 	}

// 	// check again for duplicate
// 	if kv.impl.operationsApplied[operation.ID] {
// 		return
// 	}

// 	// apply operation
// 	curr, exists := kv.impl.kvMap[operation.Key]
// 	switch operation.Type {
// 	case "Put":
// 		kv.impl.kvMap[operation.Key] = operation.Value
// 	case "Append":
// 		if exists {
// 			kv.impl.kvMap[operation.Key] = curr + operation.Value
// 		} else {
// 			kv.impl.kvMap[operation.Key] = operation.Value
// 		}

// 		// op has been applied
// 		kv.impl.operationsApplied[operation.ID] = true

// 	}

// }

// func (kv *KVPaxos) incrementSeq() int {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	kv.impl.seq++
// 	return kv.impl.seq
// }

// func (kv *KVPaxos) addSeqToCache(id int64, seq int) {
// 	kv.impl.seqCache[id] = seq
// }

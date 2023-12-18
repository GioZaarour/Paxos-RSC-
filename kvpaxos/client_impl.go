package kvpaxos

import (
	"sync"

	"usc.edu/csci499/proj3/common"
	// "usc.edu/csci499/proj3/paxos"
)

// any additions to Clerk state
type ClerkImpl struct {
	mu       sync.Mutex
	clientID int64
	seq      int
}

// initialize ck.impl state
func (ck *Clerk) InitImpl() {
	ck.impl = ClerkImpl{
		clientID: common.Nrand(),
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	ck.impl.mu.Lock()
	defer ck.impl.mu.Unlock()
	ck.impl.seq++
	args := &GetArgs{
		Key: key,
		Impl: GetArgsImpl{
			ID:  ck.impl.clientID,
			Seq: ck.impl.seq,
		},
	}
	reply := GetReply{}
	idx := 0
	for {
		ok := common.Call(ck.servers[idx], "KVPaxos.Get", args, &reply)
		if ok && reply.Err == OK {
			break
		}
	}
	return reply.Value
}

// shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.impl.mu.Lock()
	defer ck.impl.mu.Unlock()
	ck.impl.seq++

	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Impl: PutAppendArgsImpl{
			ID:  ck.impl.clientID,
			Seq: ck.impl.seq,
		},
	}

	reply := PutAppendReply{}
	idx := 0
	for {
		ok := common.Call(ck.servers[idx], "KVPaxos.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			break
		}
		idx = (idx + 1) % len(ck.servers)
	}
}

// func (ci *ClerkImpl) createOpID() int64 {
// 	ci.opID++
// 	return ci.clientID + ci.opID
// }

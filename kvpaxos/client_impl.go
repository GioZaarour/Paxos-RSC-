package kvpaxos

import (
	"fmt"

	"usc.edu/csci499/proj3/common"
	// "usc.edu/csci499/proj3/paxos"
	"time"
)

//
// any additions to Clerk state
//
type ClerkImpl struct {
	clientID int64
	opID int64
	Key string
	Value string 
}

//
// initialize ck.impl state
//
func (ck *Clerk) InitImpl() {
	ck.impl = ClerkImpl {
		clientID: common.Nrand(),
		opID: 0,
		Key: "",
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs {
		Key: key, 
		Impl: GetArgsImpl{ 
            ID: ck.impl.createOpID(),
        },
	}
	for {
		args.Impl.ID = ck.impl.createOpID()
		args.Key = ck.impl.Key

		for _, srv := range ck.servers {
			var reply GetReply
			ok := common.Call(srv, "KVPaxos.Get", &args, &reply)
			if ok && reply.Err == OK {
				return reply.Value
			}
		}
		time.Sleep(100*time.Millisecond)
	}
}

//
// shared by Put and Append; op is either "Put" or "Append"
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}

	fmt.Println("client key: ", args.Key)
	fmt.Println("client value: ", args.Value)

	for {
		args.Impl.ID = ck.impl.createOpID()
		fmt.Println("client key: ", args.Key)

		for _, srv := range ck.servers {
			var reply PutAppendReply
			ok := common.Call(srv, "KVPaxos.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ci *ClerkImpl) createOpID() int64 {
	ci.opID++
	return ci.clientID + ci.opID
}
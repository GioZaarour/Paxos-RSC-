package kvpaxos

//
// additional state to include in arguments to PutAppend RPC.
// Field names must start with capital letters,
// otherwise RPC will break.
//
type PutAppendArgsImpl struct {
	ID  int64
	Seq int
}

//
// additional state to include in arguments to Get RPC.
//
type GetArgsImpl struct {
	ID  int64
	Seq int
}

//
// for new RPCs that you add, declare types for arguments and reply
//

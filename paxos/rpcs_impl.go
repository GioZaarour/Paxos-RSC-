package paxos

import (

	// "time"
	// "strconv"

	"usc.edu/csci499/proj3/common"
)

// In all data types that represent RPC arguments/reply, field names
// must start with capital letters, otherwise RPC will break.

const (
	OK     = "OK"
	Reject = "Reject"
)

type Response string

type PrepareArgs struct {
	Sequence  int
	N         int
	DoneValue int
	Replica   int
}

type PrepareReply struct {
	Prepared                  bool
	HighestAcceptedProposalID int
	AcceptedValue             interface{}
	Err                       string
}

type AcceptArgs struct {
	Sequence  int
	N         int
	Value     interface{}
	DoneValue int
}

type AcceptReply struct {
	Accepted                  bool
	Err                       string
	HighestAcceptedProposalID int
	AcceptedValue             interface{}
}

type DecidedArgs struct {
	Sequence  int
	N         int
	Value     interface{}
	DoneValue int
}

type DecidedReply struct {
	Err string
}

type BroadcastArgs struct {
	Replica int
	Seq     int
}

type BroadcastReply struct {
	Err string
}

// function sending prepare RPC
func (px *Paxos) sendPrepare(seq int, n int, v interface{}) error {

	if seq < px.Min() {
		return nil
	}

	okCount := 0

	// easy access to this instance
	thisInstance := px.impl.instances[seq]

	// thisInstance.PrepareOK = false
	// thisInstance.PrepareOKCount = 0

	//Printf("[%d]: majority is %d \n", px.me, px.impl.majority)

	for _, peer := range px.peers {

		args := &PrepareArgs{
			Sequence:  seq,
			N:         n,
			DoneValue: px.impl.minDone[px.me],
			Replica:   px.me,
		}
		reply := &PrepareReply{
			Prepared: false,
		}

		//if index in peers is this server's index (px.me), then call the local prepare function
		if peer == px.peers[px.me] {
			px.Prepare(args, reply)
			if reply.Prepared {
				okCount++
			} else if reply.Err == "Instance decided" {
				thisInstance.va = reply.AcceptedValue
				thisInstance.Decided = true
				thisInstance.PrepareOK = true
				return nil
			}
		} else if common.Call(peer, "Paxos.Prepare", args, reply) {
			if reply.Prepared {
				okCount++
			} else if reply.Err == "Instance decided" {
				thisInstance.va = reply.AcceptedValue
				thisInstance.Decided = true
				thisInstance.PrepareOK = true
				return nil
			} else if reply.AcceptedValue != nil {
				thisInstance.va = reply.AcceptedValue
				thisInstance.na = reply.HighestAcceptedProposalID
				thisInstance.PrepareOK = true
				return nil
			}
		}
	} // END FOR

	if okCount >= px.impl.majority {
		//Printf("[%d]: majority of prepares accepted\n", px.me)
		thisInstance.PrepareOKCount = okCount
		thisInstance.PrepareOK = true
	}

	return nil
} // END SEND PREPARE

// acceptor's prepare(n) RPC handler:
//
//	if n > n_p
//	  n_p = n
//	  reply prepare_ok(n, n_a, v_a)
//	else
//	  reply prepare_reject
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	if args.Sequence < px.Min() {
		return nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	//update min done of the sender
	if args.DoneValue > px.impl.minDone[args.Replica] {
		px.impl.minDone[args.Replica] = args.DoneValue

		// Use the Min() function to get the global minimum sequence number
		globalMinSeq := px.Min()

		// Remove instances that can be forgotten
		for s := range px.impl.instances {
			if s < globalMinSeq {
				delete(px.impl.instances, s)
			}
		}
	}

	//first check if instance exists
	_, exists := px.impl.instances[args.Sequence]

	//for every new paxos instance encountered, we make a new local instance and add to instance map
	if !exists {
		px.impl.instances[args.Sequence] = &Instance{
			PrepareOKCount: 0,
			AcceptOKCount:  0,
			DecideOKCount:  0,
			PrepareOK:      false,
			AcceptOK:       false,
			Decided:        false,
			np:             0,
			na:             0,
			va:             nil,
			AcceptedValue:  nil,
		}
	}

	// easy access to this instance
	thisInstance := px.impl.instances[args.Sequence]

	if thisInstance.Decided {
		reply.Prepared = false
		reply.Err = "Instance decided"
		reply.AcceptedValue = thisInstance.va
		reply.HighestAcceptedProposalID = thisInstance.na
		return nil
	}

	//if n > n_p
	//print n and n_p
	//Printf("[%d]: prepare called sequence %d N %d and NP %d \n", px.me, args.Sequence, args.N, thisInstance.np)
	if args.N >= thisInstance.np {

		//n_p = n
		thisInstance.np = args.N
		//reply prepare_ok(n, n_a, v_a)
		reply.Prepared = true
		reply.HighestAcceptedProposalID = thisInstance.na
		reply.AcceptedValue = thisInstance.va
		reply.Err = OK
		//Printf("[%d]: prepare accepted\n", px.me)
	} else {
		//reply prepare_reject
		//Printf("[%d]: prepare rejected\n", px.me)
		reply.Prepared = false
	}

	return nil
}

// send accept RPC function
func (px *Paxos) sendAccept(seq int, n int, v interface{}) error {
	if seq < px.Min() {
		return nil
	}

	//TODO: in this and sendPrepare -- should probably have some sort of loop to keep waiting until we get a majority of OKs
	// in order to not hinder paxos instances, whatever function calls sendAccept and sendPrepare should probably be in a goroutine

	// easy access to this instance
	thisInstance := px.impl.instances[seq]

	var args = &AcceptArgs{
		Sequence: seq,
		N:        n,
		Value:    v,
	}
	reply := &AcceptReply{}
	okCount := 0

	for _, peer := range px.peers {
		//if index in peers is this server's index (px.me), then call the local accept function
		if peer == px.peers[px.me] {
			px.Accept(args, reply)
			if reply.Accepted {
				okCount++
			}
		} else if common.Call(peer, "Paxos.Accept", args, reply) {
			if reply.Accepted {
				okCount++
			}
			// else if reply.Err == "Instance decided" {
			// 	thisInstance.va = reply.AcceptedValue
			// 	thisInstance.Decided = true
			// 	thisInstance.PrepareOK = true
			// 	thisInstance.AcceptOK = true
			// 	thisInstance.na = reply.HighestAcceptedProposalID
			// 	return nil
			// }
		}
	}

	if okCount >= px.impl.majority {
		thisInstance.AcceptOKCount = okCount
		thisInstance.AcceptOK = true
	}

	return nil
}

// acceptor's accept(n, v) RPC handler:
//
//	if n >= n_p
//	  n_p = n
//	  n_a = n
//	  v_a = v
//	  reply accept_ok(n)
//	else
//	  reply accept_reject
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	if args.Sequence < px.Min() {
		return nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	//Printf("[%d]: accept called sequence %d ID %d \n", px.me, args.Sequence, args.N)

	//first check if instance exists
	_, exists := px.impl.instances[args.Sequence]

	//for every new paxos instance encountered, we make a new local instance and add to instance map
	if !exists {
		px.impl.instances[args.Sequence] = &Instance{
			PrepareOKCount: 0,
			AcceptOKCount:  0,
			DecideOKCount:  0,
			PrepareOK:      false,
			AcceptOK:       false,
			Decided:        false,
			np:             0,
			na:             0,
			va:             nil,
			AcceptedValue:  nil,
		}
	}

	// easy access to this instance
	thisInstance := px.impl.instances[args.Sequence]

	if thisInstance.Decided {
		reply.Accepted = false
		reply.Err = "Instance decided"
		reply.AcceptedValue = thisInstance.va
		reply.HighestAcceptedProposalID = thisInstance.na
		return nil
	}

	if args.N >= thisInstance.np {
		thisInstance.np = args.N
		thisInstance.na = args.N
		thisInstance.va = args.Value

		reply.Accepted = true
		reply.HighestAcceptedProposalID = args.N
		reply.Err = OK
		//Printf("[%d]: accept accepted\n", px.me)

	} else {
		//Printf("[%d]: accept rejected\n", px.me)
		// duration := 5 * time.Minute
		// time.Sleep(duration)
		reply.Accepted = false
	}

	return nil
}

// send decide RPC function
func (px *Paxos) sendDecide(seq int, n int, v interface{}) {
	if seq < px.Min() {
		return
	}

	var args = &DecidedArgs{
		Sequence: seq,
		N:        n,
		Value:    v,
	}
	reply := &DecidedReply{}
	okCount := 0

	// easy access to this instance
	thisInstance := px.impl.instances[seq]

	//TODO: do we also have to make sure a majority of decides have been received in order to deduce decided in the proposer func?
	//YES

	for _, peer := range px.peers {
		//if index in peers is this server's index (px.me), then call the local decide function
		if peer == px.peers[px.me] {
			px.Decide(args, reply)
			if reply.Err == OK {
				okCount++
			}
		} else if common.Call(peer, "Paxos.Decide", args, reply) {
			if reply.Err == OK {
				okCount++
			}
		}
	}

	if okCount >= px.impl.majority {
		thisInstance.DecideOKCount = okCount
		thisInstance.Decided = true
	}
}

// acceptor's decide(n, v) RPC handler:
func (px *Paxos) Decide(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if args.Sequence < px.Min() {
		return nil
	}

	//first check if instance exists
	_, exists := px.impl.instances[args.Sequence]

	//for every new paxos instance encountered, we make a new local instance and add to instance map
	if !exists {
		px.impl.instances[args.Sequence] = &Instance{
			PrepareOKCount: 0,
			AcceptOKCount:  0,
			DecideOKCount:  0,
			PrepareOK:      false,
			AcceptOK:       false,
			Decided:        false,
			np:             0,
			na:             0,
			va:             nil,
			AcceptedValue:  nil,
		}
	}

	// easy access to this instance
	thisInstance := px.impl.instances[args.Sequence]

	// print the instances map
	//Printf("[%d]: instances map: %v\n", px.me, px.impl.instances)
	//Printf("[%d]: args.Sequence: %v\n", px.me, args.Sequence)

	thisInstance.Decided = true
	thisInstance.va = args.Value

	reply.Err = OK

	//Printf("[%d]: Decide called\n", px.me)
	//Printf("[%d]: Decided: %v\n", px.me, thisInstance.Decided)
	//Printf("[%d]: va: %v\n", px.me, thisInstance.va)
	return nil
}

// RPC handler to receive broadcasted done values
func (px *Paxos) BroadcastDone(args *BroadcastArgs, reply *BroadcastReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// update the MinDone value for that peer
	if args.Seq > px.impl.minDone[args.Replica] {
		px.impl.minDone[args.Replica] = args.Seq
	}

	// Use the Min() function to get the global minimum sequence number
	globalMinSeq := px.Min()

	// Remove instances that can be forgotten
	for s := range px.impl.instances {
		if s < globalMinSeq {
			delete(px.impl.instances, s)
		}
	}

	return nil
}

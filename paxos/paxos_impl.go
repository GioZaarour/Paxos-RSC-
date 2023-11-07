package paxos

import (
	"sync"
	"time"

	"usc.edu/csci499/proj3/common"
)

// additions to Paxos state.
type PaxosImpl struct {
	majority  int
	minDone   []int
	instances map[int]*Instance
	mu        sync.Mutex
}

type Instance struct {
	PrepareOKCount int
	AcceptOKCount  int
	DecideOKCount  int
	PrepareOK      bool
	AcceptOK       bool
	Decided        bool
	np             int
	na             int
	va             interface{}
	AcceptedValue  interface{}
}

// your px.impl.* initializations here.
func (px *Paxos) initImpl() {
	px.impl = PaxosImpl{
		majority: len(px.peers)/2 + 1,
		// initialize minDone to -1 for all peers
		minDone:   make([]int, len(px.peers)),
		instances: make(map[int]*Instance),
	}

	// initialize minDone to -1 for all peers
	for i := range px.impl.minDone {
		px.impl.minDone[i] = -1
	}

	//Printf("[%d]: initImpl finished\n", px.me)

}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.

// should return immediately so move most of functionality to a different function
// and put content into a go routine
func (px *Paxos) Start(seq int, v interface{}) {

	//small delay
	time.Sleep(100 * time.Millisecond)

	//first check if instance exists locally
	_, exists := px.impl.instances[seq]

	//for every new paxos instance encountered, we make a new local instance and add to instance map
	if !exists {
		px.impl.instances[seq] = &Instance{
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
	thisInstance := px.impl.instances[seq]

	//if this instance is already decided with a value, force the proposer to propose that value
	if thisInstance.va != nil {
		v = thisInstance.va
	}

	//if start is called with a sequence number less than min, this call should be ignored
	if !(seq < px.Min()) {
		//Printf("[%d]: sequence being proposed\n", px.me)
		go px.Propose(seq, v)
	}

}

// work flow for propose function
// for loop until this sequence is decided
/*
proposer(v):
  while not decided:
    choose n, unique and higher than any n seen so far
    send prepare(n) to all servers including self
    if prepare_ok(n, n_a, v_a) from majority:
      v' = v_a with highest n_a; choose own v otherwise
      send accept(n, v') to all
      if accept_ok(n) from majority:
        send decided(v') to all

infinite for loop that only breaks if sequence is decided
	1 everytime enter loop n = n+1
	2 need to call the prepare function which sends out prepare function to all the peers
	3 need to check if the majority of the peers responded OK
	4 if majority: accept phase
	5 v' = v_a with highest n_a; choose own v otherwise
	6 send out accept message to all the peers and check if majority accept (retry if this is not correct--continue)
	7 send the decide to all the peers
*/
func (px *Paxos) Propose(seq int, v interface{}) {

	//Printf("[%d]: sequence number: %d\n", px.me, seq)

	//first check if instance exists locally
	_, exists := px.impl.instances[seq]

	//for every new paxos instance encountered, we make a new local instance and add to instance map
	if !exists {
		px.impl.instances[seq] = &Instance{
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
	thisInstance := px.impl.instances[seq]

	if thisInstance.Decided {
		return
	}

	// choose n, unique and higher than any n seen so far
	n := int(common.Nrand())
	for n <= thisInstance.np {
		n = int(common.Nrand())
	}

	for !thisInstance.Decided {

		// PART 2

		px.sendPrepare(seq, n, v)

		// PART 3/4
		if thisInstance.PrepareOK {
			//Printf("[%d]: proposal ok\n", px.me)
			// PART 5
			//TODO double check v prime logic
			var vPrime interface{}

			//if this instance.va has a higher n_a than n, then vPrime = thisInstance.va
			//else vPrime = v
			if thisInstance.na > n {
				vPrime = thisInstance.va
			} else {
				vPrime = v
			}

			px.sendAccept(seq, n, vPrime)

			// PART 6
			if thisInstance.AcceptOK {
				thisInstance.va = vPrime

				// PART 7
				px.sendDecide(seq, n, vPrime)

				if thisInstance.DecideOKCount >= px.impl.majority {
					thisInstance.Decided = true
					thisInstance.va = vPrime
				}
			}
		} else {
			//Printf("[%d]: failed\n", px.me)
			// duration := 5 * time.Minute
			// time.Sleep(duration)
			continue
		}

		//increment n for every new proposal attempt
		n = thisInstance.np + 1

	} // END OF FOR LOOP

} // END OF PROPOSE FUNCTION

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	//Printf("[%d]: done called sequence %d\n", px.me, seq)

	args := &BroadcastArgs{
		Replica: px.me,
		Seq:     seq,
	}
	//broadcast this change to all other peers
	for _, peer := range px.peers {
		if peer != px.peers[px.me] {
			common.Call(peer, "Paxos.BroadcastDone", args, &BroadcastReply{})
		}
	}

	// update the MinDone value for this peer
	if seq > px.impl.minDone[px.me] {
		px.impl.minDone[px.me] = seq
	}

	// Use the Min() function to get the global minimum sequence number
	globalMinSeq := px.Min()

	// Remove instances that can be forgotten
	for s := range px.impl.instances {
		if s < globalMinSeq {
			delete(px.impl.instances, s)
		}
	}

}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {

	maxSeq := -1
	for seq := range px.impl.instances {
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s will not increase
// even if all reachable peers call Done(). The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
func (px *Paxos) Min() int {

	minSeq := px.impl.minDone[0]
	for _, doneSeq := range px.impl.minDone {
		if doneSeq < minSeq {
			minSeq = doneSeq
		}
	}
	return minSeq + 1

}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so, what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	if seq < px.Min() {
		return Forgotten, nil
	}
	instance, exists := px.impl.instances[seq]
	if !exists {
		return Pending, nil
	}
	if instance.Decided {
		return Decided, instance.va
	}
	return Pending, nil
}

// helper func for use in RSM library
func (px *Paxos) StatusString(fate Fate) string {
	if fate == Decided {
		return "Decided"
	} else if fate == Pending {
		return "Pending"
	} else {
		return "Forgotten"
	}
}

package paxosrsm

import (
	"fmt"
	"time"

	"usc.edu/csci499/proj3/paxos"
)

type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

// additions to PaxosRSM state
type PaxosRSMImpl struct {
	decided     bool
	usedSeqNums map[int]struct{}
}

// initialize rsm.impl.*
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl = PaxosRSMImpl{
		usedSeqNums: make(map[int]struct{}),
	}
}


func (rsm *PaxosRSM) Start(seq int, v interface{}) {
    rsm.px.Start(seq, v)
}

func (rsm *PaxosRSM) Status(seq int) (paxos.Fate, interface{}) {
    return rsm.px.Status(seq)
}

func (rsm *PaxosRSM) Done(seq int) {
    rsm.px.Done(seq)
}

func (rsm *PaxosRSM) Min() int {
    return rsm.px.Min()
}

func (rsm *PaxosRSM) Max() int {
    return rsm.px.Max()
}

//
// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
//

func (rsm *PaxosRSM) AddOp(v interface{}) interface{} {

	// create Paxos instance for this operation
	seq := rsm.Max() + 1

	// start a new instance of Paxos for this sequence number
	fmt.Println("seq: ", seq)
	fmt.Println("v: ", v)
	rsm.Start(seq, v)

	// trying paxos instance on seq num
	for {
		if _, exists := rsm.impl.usedSeqNums[seq]; !exists {
			break
		}
		seq++
	}

	// seq number is being used
	rsm.impl.usedSeqNums[seq] = struct{}{}

	return rsm.handleConflict(v, seq)
}

func (rsm *PaxosRSM) handleConflict(v interface{}, seq int) interface{} {
	// trying a new instance of Paxos with a new sequence number
	rsm.Start(seq, v)

	// wait to reach consensus
	to := 10 * time.Millisecond

<<<<<<< HEAD
	fate, decidedValue := rsm.px.Status(seq)
	if rsm.px.StatusString(fate) == "Decided" {
=======
	fate, decidedValue := rsm.Status(seq)
	if fate == paxos.Decided {
>>>>>>> 7f3342fe3b401929da129a4f8dc5bd5826624e8e
		rsm.applyOp(decidedValue)

		// The sequence number is no longer in use
		delete(rsm.impl.usedSeqNums, seq)

		return decidedValue
	} else {
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
		return rsm.handleConflict(v, seq+1)
	}

}

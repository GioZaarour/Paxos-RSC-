# Paxos-based RSM Key-Value Store

*This project is an excerpt from my Distributed Systems Course at USC. The implementation is my own work and is not perfect or devoid of bugs.* 

## Overview
Central to this project is the use of a group of replicas to process client requests in a consistent order, established via Paxos, even under unstable network conditions. The architecture includes clients and KVPaxos servers running as Paxos peers. Key features involve handling client RPCs, managing a replicated log, and concurrently implementing the Paxos protocol across multiple instances. This approach aims to effectively manage replication and order of operations, thereby improving system robustness.

## Diagram

![Paxos KV Store](https://github.com/giozaarour/Paxos-RSM/blob/master/common/DIAGRAM.png?raw=true)

## Usage

To test the script, navigate to the corresponding folder and set GOPATH before running

`go test`

To run a single test function in the test_test.go file, run

`go test -run FUNCTION_NAME`

To run a single test function multiple times, run

`go test -run FUNCTION_NAME -count NUM_TIMES`

To avoid warnings, run 

`go test | egrep -v "keyword1|keyword2|keyword3"`
# Paxos-based RSM Key-Value Store
# Paxos-based RSM Key-Value Store

*This project is an excerpt from my Distributed Systems Course at USC. The implementation is my own work and is not perfect or devoid of bugs.* 

## Overview

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
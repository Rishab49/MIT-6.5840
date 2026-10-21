package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type WorkerArgs struct {
	WorkerID string
}

type WorkType int

const (
	WorkMap WorkType = iota
	WorkReduce
	WorkNil
)

type WorkerReply struct {
	TaskID   string
	FileObj  File
	Nreduce  int
	Worktype WorkType
}

type CompletionArgs struct {
	Filename string
	ID       string
	Worktype WorkType
}

type CompletionReply struct {
	Status string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

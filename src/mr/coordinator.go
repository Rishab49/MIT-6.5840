package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	MapFiles    []File
	ReduceFiles []File
	isCompleted bool
	Nreduce     int
	mu          sync.Mutex
}

type File struct {
	Filename      string
	ID            string
	isFinished    bool
	TaskInitiated time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetWork(args *WorkerArgs, reply *WorkerReply) error {
	// fmt.Print("Reuest received\n")
	// fmt.Print(args, "\n")

	allMapDone := true
	allReducerDone := true
	for i := range c.MapFiles {
		if c.MapFiles[i].ID == "" || (c.MapFiles[i].isFinished == false && time.Since(c.MapFiles[i].TaskInitiated) > time.Duration(time.Second*5)) {
			c.mu.Lock()
			c.MapFiles[i].ID = args.WorkerID
			c.MapFiles[i].isFinished = false
			c.MapFiles[i].TaskInitiated = time.Now()
			reply.FileObj = c.MapFiles[i]
			reply.TaskID = strconv.Itoa(i)
			reply.Nreduce = c.Nreduce
			reply.Worktype = WorkMap
			c.mu.Unlock()
			return nil
		}
	}

	for _, f := range c.MapFiles {
		if !f.isFinished {
			allMapDone = false
			break
		}
	}

	if !allMapDone {
		// Map phase still in progress; tell worker to wait (do not assign reduce yet)
		reply.Worktype = WorkMap
		reply.FileObj = File{}
		return nil
	}

	if len(c.ReduceFiles) == 0 {

		files := []File{}

		for ind := range c.Nreduce {

			f := File{}
			f.ID = ""
			f.Filename = strconv.Itoa(ind)

			files = append(files, f)

		}

		c.ReduceFiles = files

	}

	for ind, file := range c.ReduceFiles {

		if file.ID == "" || (c.ReduceFiles[ind].isFinished == false && time.Since(c.ReduceFiles[ind].TaskInitiated) > time.Duration(time.Second*5)) {
			c.mu.Lock()
			c.ReduceFiles[ind].ID = args.WorkerID
			c.ReduceFiles[ind].TaskInitiated = time.Now()
			reply.FileObj = c.ReduceFiles[ind]
			reply.Nreduce = c.Nreduce
			reply.Worktype = WorkReduce
			reply.TaskID = strconv.Itoa(ind)
			c.mu.Unlock()
			return nil
		}
	}

	for _, f := range c.ReduceFiles {
		if !f.isFinished {
			allReducerDone = false
			break
		}
	}

	if !allReducerDone {
		// Map phase still in progress; tell worker to wait (do not assign reduce yet)
		reply.Worktype = WorkReduce
		reply.FileObj = File{}
		return nil
	}

	// for i := range c.ReduceFiles {
	// 	if c.ReduceFiles[i].ID == "" {
	// 		c.mu.Lock()
	// 		c.ReduceFiles[i].ID = args.WorkerID
	// 		reply.FileObj = c.ReduceFiles[i]
	// 		reply.Nreduce = c.Nreduce
	// 		reply.Worktype = WorkReduce
	// 		c.mu.Unlock()
	// 		return nil
	// 	}
	// }

	c.isCompleted = true
	reply.Worktype = WorkNil
	return nil

}

func (c *Coordinator) PostDone(args *CompletionArgs, reply *CompletionReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	// fmt.Print("POSTDONE CALLED")
	if args.Worktype == WorkMap {

		// fmt.Print("Current State of MAP", c.MapFiles, "\n")

		for index, file := range c.MapFiles {
			if file.Filename == args.Filename && file.ID == args.ID {

				c.MapFiles[index].isFinished = true
				return nil
			}
		}

		return nil
	} else {

		// fmt.Print("Current State of REDUCER", c.ReduceFiles, "\n")
		filename := args.Filename
		for ind, file := range c.ReduceFiles {
			if file.Filename == filename {
				c.ReduceFiles[ind].isFinished = true
				return nil
			}
		}

		return nil

	}

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	fmt.Print("starting the server")
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// for {
	// 	time.Sleep(time.Second * 1)
	// 	if c.isCompleted == true {
	// 		break
	// 	}
	// }

	// Your code here.

	return c.isCompleted
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Nreduce = nReduce

	globFiles := []File{}

	for _, file := range files {
		if len(file) > 4 {

			f := File{}
			f.ID = ""
			f.Filename = file

			globFiles = append(globFiles, f)
		}
	}

	filesArr, err := getFilesFromGlob(files)

	if err != nil {
		// fmt.Print(err)
	}
	c.MapFiles = filesArr
	c.ReduceFiles = []File{}
	// fmt.Print("files: ", c.MapFiles, globFiles, "\n")

	// Your code here.

	c.server()
	return &c
}

func getFilesFromGlob(fileGlob []string) ([]File, error) {

	files := []File{}

	for _, file := range fileGlob {
		// fmt.Print(len(file))
		if len(file) > 4 {

			f := File{}
			f.ID = ""
			f.Filename = file

			files = append(files, f)
		}
	}

	return files, nil

}

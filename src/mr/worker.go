package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/big"
	"net/rpc"
	"os"
	"path/filepath"
	"plugin"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var WORKERID string

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string, nReduce int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7fffffff) % nReduce
}

func generateSecure4DigitString() (string, error) {
	// Generate a random number between 0 and 9999
	n, err := rand.Int(rand.Reader, big.NewInt(10000))
	if err != nil {
		return "", err
	}
	// Use %04d to format with leading zeros (e.g., 7 becomes "0007")
	return fmt.Sprintf("%04d", n.Int64()), nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	WORKERID, _ = generateSecure4DigitString()

	// uncomment to send the Example RPC to the coordinator.
	for {

		reply := CallExample()

		if (reply.Worktype == WorkMap || reply.Worktype == WorkReduce) && reply.FileObj.Filename == "" {
			time.Sleep(time.Second * 1)
			continue
		}

		if reply.Worktype == WorkMap {
			// fmt.Print("IF STATEMENT ---------------------------\n", reply, "\n")

			intermediate := []KeyValue{}

			file, err := os.Open(reply.FileObj.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileObj.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileObj.Filename)
			}
			file.Close()
			kva := mapf(reply.FileObj.Filename, string(content))
			intermediate = append(intermediate, kva...)

			sort.Sort(ByKey(intermediate))

			buckets := make([][]KeyValue, reply.Nreduce)
			for _, kv := range intermediate {
				r := ihash(kv.Key, reply.Nreduce)
				buckets[r] = append(buckets[r], kv)
			}

			for r := 0; r < reply.Nreduce; r++ {
				tempFile, _ := os.CreateTemp(".", fmt.Sprintf("mr-tmp-%s-%d-*", reply.TaskID, r))
				enc := json.NewEncoder(tempFile)
				for _, kv := range buckets[r] {
					enc.Encode(&kv)
				}
				tempFile.Close()
				os.Rename(tempFile.Name(), fmt.Sprintf("mr-%s-%d", reply.TaskID, r))
			}

			// i := 0
			// for i+1 < len(intermediate) {

			// 	j := i + 1
			// 	hash := ihash(intermediate[j].Key, reply.Nreduce)
			// 	ofile, _ := os.OpenFile(fmt.Sprintf("mr-%s-%d", reply.WorkerID, hash), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			// 	enc := json.NewEncoder(ofile)

			// 	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {

			// 		kv := KeyValue{}
			// 		kv.Key = intermediate[i].Key
			// 		kv.Value = intermediate[j].Value
			// 		enc.Encode(&kv)
			// 		// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, intermediate[j].Value)
			// 		j++
			// 	}

			// 	i = j
			// }

			Completionargs := CompletionArgs{}
			Completionreply := CompletionReply{}

			Completionargs.Filename = reply.FileObj.Filename
			Completionargs.Worktype = WorkMap
			Completionargs.ID = WORKERID

			ok := call("Coordinator.PostDone", &Completionargs, &Completionreply)

			if !ok {

				// fmt.Printf("call failed!\n")

			}

		} else if reply.Worktype == WorkReduce {
			// fmt.Print("ELSE IF STATEMENT ---------------------------\n")
			files, err := filepath.Glob(fmt.Sprintf(`mr-*-%s`, reply.FileObj.Filename))
			// fmt.Print("files to read for reduce", files, reply, "\n")
			if err != nil {
				// fmt.Print(err)
			}

			// oname := fmt.Sprintf("mr-out-%s", reply.FileObj.Filename)
			// ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

			ofile, _ := os.CreateTemp(".", "mr-out-tmp-*")

			intermediate := []KeyValue{}
			i := 0
			for _, file := range files {
				f, _ := os.Open(file)

				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}

				f.Close()
			}

			sort.Sort(ByKey(intermediate))

			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()
			os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%s", reply.FileObj.Filename))

			Completionargs := CompletionArgs{}
			Completionreply := CompletionReply{}

			Completionargs.Filename = reply.FileObj.Filename
			Completionargs.Worktype = WorkReduce
			Completionargs.ID = WORKERID

			ok := call("Coordinator.PostDone", &Completionargs, &Completionreply)

			if !ok {

				// fmt.Printf("call failed!\n")

			}

		} else {

			// fmt.Print("ELSE STATEMENT ---------------------------\n")
			return
		}

	}

}

func CallDone(filename string, ID string) CompletionReply {

	args := CompletionArgs{}

	// fill in the argument(s).
	args.ID = ID
	args.Filename = filename

	// declare a reply structure.
	reply := CompletionReply{}

	ok := call("Coordinator.PostDone", &args, &reply)

	if ok {
		return reply
	} else {
		// fmt.Printf("call failed!\n")
		return CompletionReply{}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() WorkerReply {

	// declare an argument structure.
	args := WorkerArgs{}

	// fill in the argument(s).
	args.WorkerID = WORKERID

	// declare a reply structure.
	reply := WorkerReply{}
	// reply.WorkerID = WORKERID

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetWork", &args, &reply)
	if ok {
		return reply
	} else {
		// fmt.Printf("call failed!\n")
		return WorkerReply{}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

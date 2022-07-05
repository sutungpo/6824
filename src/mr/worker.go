package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetFlags(log.Lshortfile)
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := WorkArgs{}
		reply := WorkReply{}
		call("Coordinator.Work", &args, &reply)
		var ts = reply.Ts
		switch ts.Ctg {
		case "map":
			nbucket, ok := ts.Ext.(int)
			if !ok {
				return
			}
			num := ts.Seq
			filename := ts.Detail[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva))
			oldname := fmt.Sprintf("mr-%d*", num)
			files, err := filepath.Glob(oldname)
			if err != nil {
				log.Fatalf("incorrect pattern")
			}
			for _, f := range files {
				if err := os.Remove(f); err != nil {
					log.Fatalf("incorrect path")
				}
			}
			set := make(map[string]int)
			for _, kv := range kva {
				i := ihash(kv.Key) % nbucket
				oname := fmt.Sprintf("mr-%d-%d", num, i)
				ofile, err := os.OpenFile(oname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
				if err != nil {
					log.Fatalf("cannot create %v", oname)
				}
				set[oname] = i
				enc := json.NewEncoder(ofile)
				err1 := enc.Encode(&kv)
				if err1 != nil {
					log.Fatalf("json encode error!")
					return
				}
				ofile.Close()
			}
			CallDone("map", num, set)
		case "reduce":
			nmap, ok := ts.Ext.(int)
			if !ok {
				return
			}
			num := ts.Seq - nmap
			kva := []KeyValue{}
			for _, filename := range ts.Detail {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
			oname := fmt.Sprintf("mr-out-%v", num)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()
			CallDone("reduce", ts.Seq, nil)
		default:
			return
		}
	}

}

func CallDone(work string, seq int, m map[string]int) {
	args := WorkArgs{}
	args.S = work
	args.Seq = seq
	args.Ext = m
	reply := WorkReply{}
	call("Coordinator.WorkDone", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println(err)
	return false
}

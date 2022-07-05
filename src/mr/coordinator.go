package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// files name to parse
	files []string
	// files by map for reduce
	reduceFiles [][]string
	// task status, 0 not allocated, 1 allocated, 2 done, elements totally nReduce + nMap + 2, the second to last is all done task nums.
	// the last one is all allocated nums
	staTask []int
	// reduce task nums, specified by coordinator main func
	nReduce int
	// map task nums, equal to files nums
	nMap int
	cond *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Work(args *WorkArgs, reply *WorkReply) error {
	log.SetFlags(log.Lshortfile)
	ts := c.allocTask()
	if ts != nil {
		reply.Ts = *ts
		reply.Error = nil
		seq := ts.Seq
		go func() {
			time.Sleep(10 * time.Second)
			c.cond.L.Lock()
			defer c.cond.L.Unlock()
			if c.staTask[seq] != 2 {
				c.staTask[seq] = 0
				c.staTask[c.nMap+c.nReduce+1]--
				c.cond.Broadcast()
			}
		}()
	} else {
		reply.Ts = Task{Ctg: "exit"}
	}
	return nil
}

func (c *Coordinator) allocTask() *Task {
	log.SetFlags(log.Lshortfile)
	for {
		c.cond.L.Lock()
		if c.staTask[c.nMap+c.nReduce+1] < c.nMap {
			for i, b := range c.staTask[:c.nMap] {
				if b == 0 {
					c.staTask[i] = 1
					ts := new(Task)
					*ts = Task{Ctg: "map", Detail: []string{c.files[i]}, Seq: i, Ext: c.nReduce}
					c.staTask[c.nMap+c.nReduce+1]++
					c.cond.L.Unlock()
					return ts
				}
			}
		}
		for c.staTask[c.nMap+c.nReduce+1] == c.nMap && c.staTask[c.nMap+c.nReduce] < c.nMap {
			c.cond.Wait()
		}

		if c.staTask[c.nMap+c.nReduce] >= c.nMap {
			c.cond.L.Unlock()
			break
		}
		c.cond.L.Unlock()
	}

	for {
		c.cond.L.Lock()
		if c.staTask[c.nMap+c.nReduce+1] < c.nMap+c.nReduce {
			for i, b := range c.staTask[c.nMap : c.nMap+c.nReduce] {
				if b == 0 {
					c.staTask[c.nMap+i] = 1
					ts := new(Task)

					*ts = Task{Ctg: "reduce", Detail: c.reduceFiles[i], Seq: i + c.nMap, Ext: c.nMap}
					c.staTask[c.nMap+c.nReduce+1]++
					c.cond.L.Unlock()
					return ts
				}
			}
		}
		for c.staTask[c.nMap+c.nReduce+1] == c.nMap+c.nReduce && c.staTask[c.nMap+c.nReduce] < c.nMap+c.nReduce {
			c.cond.Wait()
		}

		if c.staTask[c.nMap+c.nReduce] == c.nMap+c.nReduce {
			c.cond.L.Unlock()
			break
		}
		c.cond.L.Unlock()
	}

	return nil
}

func (c *Coordinator) WorkDone(args *WorkArgs, reply *WorkReply) error {
	log.SetFlags(log.Lshortfile)
	num := args.Seq
	work := args.S
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if work == "map" {
		for filename, seq := range args.Ext {
			c.reduceFiles[seq] = append(c.reduceFiles[seq], filename)
		}
	}
	if c.staTask[num] == 1 {
		c.staTask[num] = 2
		c.staTask[c.nMap+c.nReduce]++
		c.cond.Broadcast()
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if c.staTask[c.nMap+c.nReduce] == c.nMap+c.nReduce {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	if len(files) == 0 || nReduce < 1 {
		return nil
	}
	c.files = files
	c.nReduce = nReduce
	c.nMap = len(files)
	c.cond = sync.NewCond(&sync.Mutex{})
	c.staTask = make([]int, c.nMap+c.nReduce+2)
	c.reduceFiles = make([][]string, c.nReduce)
	// Your code here.

	c.server()
	return &c
}

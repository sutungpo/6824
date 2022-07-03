package mr

import (
	"errors"
	"fmt"
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
	files   []string
	Task    []int
	nReduce int
	nMap    int
	numTask int
	cond    *sync.Cond
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
	ts := c.allocTask()
	if ts != nil {
		reply.Ts = *ts
		reply.Error = nil
		seq := ts.Seq
		go func() {
			time.Sleep(10 * time.Second)
			c.cond.L.Lock()
			defer c.cond.L.Unlock()
			if c.Task[seq] != 2 {
				c.Task[seq] = 0
			}
		}()
	} else {
		reply.Ts = Task{}
		reply.Error = errors.New("no more work")
	}
	return nil
}

func (c *Coordinator) allocTask() *Task {
	for {
		c.cond.L.Lock()
		if c.numTask < len(c.files) {
			for i, b := range c.Task[:c.nMap] {
				if b == 0 {
					c.Task[i] = 1
					ts := new(Task)
					*ts = Task{Ctg: "map", Detail: []string{c.files[i]}, Seq: i, Ext: c.nReduce}
					c.numTask++
					c.cond.L.Unlock()
					return ts
				}
			}
		}
		for c.numTask == c.nMap && c.Task[c.nMap+c.nReduce] < c.nMap {
			c.cond.Wait()
		}

		if c.Task[c.nMap+c.nReduce] >= c.nMap {
			c.cond.L.Unlock()
			break
		}
		c.cond.L.Unlock()
	}

	for {
		c.cond.L.Lock()
		if c.numTask < c.nMap+c.nReduce {
			for i, b := range c.Task[c.nMap : c.nMap+c.nReduce] {
				if b == 0 {
					c.Task[c.nMap+i] = 1
					ts := new(Task)
					pathStr := make([]string, c.nMap)
					for j, _ := range pathStr {
						pathStr[j] = fmt.Sprintf("mr-%v-%v", j, i)
					}

					*ts = Task{Ctg: "reduce", Detail: pathStr, Seq: i + c.nMap, Ext: c.nMap}
					c.numTask++
					c.cond.L.Unlock()
					return ts
				}
			}
		}
		for c.numTask == c.nMap+c.nReduce && c.Task[c.nMap+c.nReduce] < c.nMap+c.nReduce {
			c.cond.Wait()
		}

		if c.Task[c.nMap+c.nReduce] == c.nMap+c.nReduce {
			c.cond.L.Unlock()
			break
		}
	}

	return nil
}

func (c *Coordinator) WorkDone(args *WorkArgs, reply *WorkReply) error {
	num := args.Seq

	c.cond.L.Lock()
	if c.Task[num] == 0 || c.Task[num] == 2 {
		c.cond.L.Unlock()
	} else if c.Task[num] == 1 {
		c.Task[num] = 2
		c.Task[c.nMap+c.nReduce]++
		c.cond.Broadcast()
		c.cond.L.Unlock()
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
	if c.Task[c.nMap+c.nReduce] == c.nMap+c.nReduce {
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
	c.Task = make([]int, c.nMap+c.nReduce+1)
	// Your code here.

	c.server()
	return &c
}

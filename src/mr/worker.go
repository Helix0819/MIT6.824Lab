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
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	workerId := getWorkerId()

	for {
		req := TaskRequest{
			WorkerId: workerId,
			Type:     "map",
		}

		resp := TaskResponse{}

		ok := call("Coordinator.GetTask", &req, &resp)
		if ok {
			fmt.Printf("Task: %+v\n", resp.Task)
		} else {
			fmt.Println("No task available, wait for 1 second")
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.Task != nil && resp.Type == Map {
			handleMapTask(mapf, resp.Task)
		}

		if resp.Task != nil && resp.Task.Type == Reduce {
			handleReduceTask(reducef, resp.Task)
		}

		if resp.Task != nil && resp.Task.Type == Done {
			fmt.Println("All tasks are done")
			break
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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

	fmt.Println(err)
	return false
}

func getWorkerId() string {
	return strconv.Itoa(os.Getpid())
}

func handleMapTask(mapf func(string, string) []KeyValue, task *Task) {
	// read each input file
	intermediate := make([]KeyValue, 0)
	//fmt.Println("Start map task")
	file, err := os.Open(task.File)
	if err != nil {
		log.Fatalf("cannot open %v", task.File)
	} else {
		//fmt.Printf("MAP: Read file %v\n", task.File)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}
	file.Close()
	kva := mapf(task.File, string(content))
	intermediate = append(intermediate, kva...)

	// write intermidiate to file
	cutRes := make([][]KeyValue, task.NReduce)
	for i := range cutRes {
		cutRes[i] = make([]KeyValue, 0)
	}

	for _, kv := range intermediate {
		index := ihash(kv.Key) % task.NReduce
		cutRes[index] = append(cutRes[index], kv)
	}

	files := make([]string, 0)
	for i := range cutRes {
		// 打开或创建目标文件，使用 O_CREATE|O_WRONLY|O_APPEND 标志
		targetName := fmt.Sprintf("mr-%v-%v", getWorkerId(), i)
		targetPath := filepath.Join(mapfilepath, targetName)
		targetFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open or create file %v: %v", targetPath, err)
		}
		defer targetFile.Close()

		enc := json.NewEncoder(targetFile)
		for _, kv := range cutRes[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode kv: %v", err)
			}
		}

		//fmt.Printf("MAP: Write file %v\n", targetName)
		files = append(files, targetPath)
	}

	// send map task done
	mapTasksDone(files)

	return
}

func mapTasksDone(files []string) {
	req := MapTaskDoneRequest{
		WorkerId: getWorkerId(),
		Files:    files,
	}

	//fmt.Printf("worker %v done map tasks", req.WorkerId)
	resp := MapTaskDoneResponse{}
	ok := call("Coordinator.MapTasksDone", &req, &resp)
	if !ok {
		fmt.Println("Map tasks done failed")
	}
}

func reduceTasksDone(file string) {
	req := ReduceTaskDoneRequest{
		WorkerId: getWorkerId(),
		Files:    file,
	}

	resp := ReduceTaskDoneResponse{}
	ok := call("Coordinator.ReduceTasksDone", &req, &resp)
	if !ok {
		fmt.Println("Reduce tasks done failed")
	}
	return
}

func handleReduceTask(reducef func(string, []string) string, task *Task) {
	intermediate := make([]KeyValue, 0)
	files, err := filepath.Glob(task.File)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		} else {
			//fmt.Printf("Read file %v\n", file)
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	res := make([]KeyValue, 0)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		res = append(res, KeyValue{intermediate[i].Key, output})
		i = j
	}

	// write to file
	tempFile, err := ioutil.TempFile(reducefilepath, "mr-tmp")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	for _, kv := range res {
		fmt.Fprintf(tempFile, "%v %v\n", kv.Key, kv.Value)
	}

	//index := task.File[len(task.File)-1]
	lastChar := string(task.File[len(task.File)-1])
	tmpName := fmt.Sprintf("mr-out-%v", lastChar)
	err = os.Rename(tempFile.Name(), tmpName)
	if err != nil {
		log.Fatalf("cannot rename file")
	}

	fmt.Println("Reduce task done")

	// reduce tasks done
	reduceTasksDone(reducefilepath + tmpName)
}

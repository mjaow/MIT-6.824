package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup

	taskChan := createTaskChannel(jobName, mapFiles, phase, ntasks, n_other, &wg)

	go handleTaskService(taskChan, registerChan, &wg)

	//等待所有任务全部完成
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
func createTaskChannel(jobName string, mapFiles []string, phase jobPhase, ntasks int, n_other int, wg *sync.WaitGroup) chan DoTaskArgs {
	taskChan := make(chan DoTaskArgs, ntasks)
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		taskChan <- DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}
	}
	return taskChan
}

func handleTaskService(taskChan chan DoTaskArgs, registerChan chan string, wg *sync.WaitGroup) {
	for task := range taskChan {
		go dispacherTask(task, <-registerChan, taskChan, registerChan, wg)
	}
}

func dispacherTask(request DoTaskArgs, server string, taskChan chan DoTaskArgs, registerChan chan string, wg *sync.WaitGroup) {
	if success := call(server, "Worker.DoTask", request, nil); success {
		//注意先后顺序，执行完任务，先置为Done，避免wg.Wait()陷入永远等待
		wg.Done()
		registerChan <- server
	} else {
		//注意先后顺序，外部是taskChan等待registerChan，里面也必须保持一致的顺序，否则会出现死锁
		//并且优先将重试请求写入队列，可以保证在server:w还没写回registerChan之前让其他server进入优先服务
		taskChan <- request
		registerChan <- server
	}
}

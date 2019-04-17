package mapreduce

import (
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase or reducePhase).
// the mapFiles argument holds the names of the files that are the inputs to the map phase, one per map task.
// nReduce is the number of reduce tasks.
// the registerChan argument yields a stream of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all existing registered workers (if any) and new ones as they register.
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

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	// Your code here (Part III, Part IV).
	//
	/**
	代码缺点：
		1。TaskNumber 错误，不应该是总的数量，而是编号
		2。没有并发进行rpc调用
		3。没有进行错误处理，如果调用失败了怎么办？
	优点：
		想到了回送worker
	 */
	/*for _, file := range mapFiles{
		workerName := <-registerChan
		args := new(DoTaskArgs)
		args.JobName = jobName
		args.NumOtherPhase = n_other
		args.File = file
		args.TaskNumber = ntasks
		args.Phase = phase
		ok := call(workerName, "Worker.DoTask", args, nil)
		if ok == false {
			fmt.Printf("DoTask: RPC %s Dotask error\n", workerName)
		}
		fmt.Printf("Schedule: %v done\n", phase)
		registerChan <- workerName
	}*/
	/**
		1。多协程情况考虑使用waitgroup进行操控
		2。使用协程进行并发调用rpc，而非顺序执行
		3。fix the bug of using the Ntask as TaskNumber
		4。错误处理
		5。对task进行编号

		6.fix the bug, the channel's length is limit and it's not equal to N task, so it blocked
	 */
	 wg := &sync.WaitGroup{}
	 taskChan := make(chan int, ntasks)
	 for i := 0; i < ntasks; i++{
		taskChan <- i
		wg.Add(1)
	 }
	 go func() {
	 	wg.Wait()
	 	close(taskChan)
	 }()
	 var args DoTaskArgs
	 args.JobName = jobName
	 args.NumOtherPhase = n_other
	 args.Phase = phase

	 for i := range taskChan {
	 	args.TaskNumber = i
	 	// only map then will have input file
	 	if args.Phase == mapPhase{
			args.File = mapFiles[i]
		}
	 	workerName := <- registerChan
	 	go func(workerName string, args DoTaskArgs) {
			ok := call(workerName, "Worker.DoTask", args, nil)
			if ok == false {
				debug("DoTask: RPC %s Dotask error\n", workerName)
				taskChan <- args.TaskNumber
			}else{
				wg.Done()
				debug("Schedule: %v done\n", phase)
			}
			registerChan <- workerName
		}(workerName, args)
	 }
	debug("Schedule: %v phase done\n", phase)
}

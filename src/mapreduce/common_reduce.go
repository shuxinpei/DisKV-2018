package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// key []values
	kvmap := make(map[string]([]string))
	//read from file, then load it to kvmap
	for i := 0; i < nMap ; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		file_ptr, err:= os.Open(fileName)
		defer file_ptr.Close()
		if err != nil{
			fmt.Println("reduce open file failed")
		}
		decoder := json.NewDecoder(file_ptr)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err == io.EOF{
				break
			}else if err != nil {
				fmt.Println(err.Error())
			}
			kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
		}
	}
	keys := make([]string, 0)
	for key, _ := range kvmap{
		keys = append(keys, key)
	}
	sort.Strings(keys)
	outFile_ptr, err := os.Create(outFile)
	defer outFile_ptr.Close()
	if err != nil{
		fmt.Println("reduce create output file failed")
	}
	encoder := json.NewEncoder(outFile_ptr)
	for _, key := range keys{
		encoder.Encode(KeyValue{key, reduceF(key, kvmap[key])})
	}
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}

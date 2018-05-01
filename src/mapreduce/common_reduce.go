package mapreduce

import (
	"strings"
	"io/ioutil"
	"log"
	"encoding/json"
	"os"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string,       // write the output here
	nMap int,             // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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

	//读取该reduce的所有输出文件（多少map就有多少文件），将文件排序
	var reduceVals KeyValueList

	for i := 0; i < nMap; i++ {
		inputFile := reduceName(jobName, i, reduceTaskNumber)
		if dataInBytes, err := ioutil.ReadFile(inputFile); err != nil {
			log.Fatalf("error read output of map.Error msg:%v\n", err)
		} else {
			reduceVals = reduceVals.append(ToKeyValue(strings.Split(string(dataInBytes), "\n")))
		}
	}

	if fd, err := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644); err != nil {
		log.Fatalf("open file %s failed.%+v\n", outFile, err)
	} else {
		enc := json.NewEncoder(fd)
		for _, item := range reduceVals.groupByKey() {
			enc.Encode(KeyValue{
				Key:   item.Key,
				Value: reduceF(item.Key, item.ValueList),
			})

		}
		fd.Close()
	}

}

func ToKeyValue(content []string) []KeyValue {
	var rs []KeyValue
	for _, item := range content {
		if strings.Contains(item, ",") {
			kv := strings.Split(item, ",")
			rs = append(rs, KeyValue{
				Key:   kv[0],
				Value: kv[1],
			})
		}
	}
	return rs
}

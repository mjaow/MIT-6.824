package mapreduce

import (
	"fmt"
	"strconv"
	"sort"
)

// Debugging enabled?
const debugEnabled = false

// debug() will only print if debugEnabled is true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "Map"
	reducePhase          = "Reduce"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

type KeyValueList []KeyValue

func (data KeyValueList) Len() int {
	return len(data)
}

func (data KeyValueList) Less(i, j int) bool {
	return data[i].Key < data[j].Key
}

func (data KeyValueList) Swap(i, j int) {
	data[i], data[j] = data[j], data[i]
}

func (data KeyValueList) append(items []KeyValue) KeyValueList {
	for _, item := range items {
		data = append(data, item)
	}
	return data
}

type KeyValueMapping struct {
	Key       string
	ValueList []string
}

func (data KeyValueList) groupByKey() []KeyValueMapping {
	sort.Sort(data)
	var m []KeyValueMapping

	var result []string
	for i := 0; i < data.Len(); i++ {
		result = append(result, data[i].Value)
		if i == data.Len()-1 || data[i].Key != data[i+1].Key {
			m = append(m, KeyValueMapping{
				Key:       data[i].Key,
				ValueList: result,
			})
			result = make([]string, 0)
		}
	}
	return m
}

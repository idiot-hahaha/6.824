package shardctrler

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"testing"
)

func TestEncodeArray(t *testing.T) {
	arr := [10]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	config := Config{
		Num:    0,
		Shards: arr,
		Groups: nil,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(arr)
	e.Encode(config)
	var newConfig Config
	r := bytes.NewBuffer(w.Bytes())
	d := labgob.NewDecoder(r)
	var newArr [10]int
	d.Decode(&newArr)
	d.Decode(&newConfig)
	fmt.Println(newArr)
	fmt.Println(newConfig)
}

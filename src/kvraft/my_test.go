package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
	"fmt"
	"testing"
)

func TestString(t *testing.T) {
	s := "abc"
	s = string(append([]byte(s), []byte("efg")...))
}

func TestWaitGroup(t *testing.T) {

}

func TestEncodeStruct(t *testing.T) {
	set := map[int]struct{}{}
	KVMap := map[string]string{}
	set[1] = struct{}{}
	set[2] = struct{}{}
	KVMap["1"] = "a"
	KVMap["2"] = "b"
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(KVMap)
	e.Encode(set)

	wb := w.Bytes()
	r := bytes.NewBuffer(wb)
	var kVMapReader map[string]string
	var setReader map[int]struct{}
	d := labgob.NewDecoder(r)
	if d.Decode(&kVMapReader) != nil && d.Decode(&setReader) != nil {
		panic("decode err")
	}
	fmt.Printf("%v\n", kVMapReader)
	fmt.Printf("%v\n", setReader)
}

func TestEncodeStruct1(t *testing.T) {
	set := map[int]struct{}{}
	set[1] = struct{}{}
	set[2] = struct{}{}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(set)

	wb := w.Bytes()
	r := bytes.NewBuffer(wb)
	var setReader map[int]struct{}
	d := labgob.NewDecoder(r)
	if d.Decode(&setReader) != nil {
		panic("decode err")
	}
	fmt.Printf("%v\n", setReader)
}

func TestEncodeStruct2(t *testing.T) {
	set := map[int]struct{}{}
	KVMap := map[string]string{}
	set[1] = struct{}{}
	set[2] = struct{}{}
	KVMap["1"] = "a"
	KVMap["2"] = "b"
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(KVMap)
	e.Encode(set)

	wb := w.Bytes()
	r := bytes.NewBuffer(wb)
	var kVMapReader map[string]string
	var setReader map[int]struct{}
	d := labgob.NewDecoder(r)
	if d.Decode(&kVMapReader) != nil && d.Decode(&setReader) != nil {
		panic("decode err")
	}
	fmt.Printf("%v\n", kVMapReader)
	fmt.Printf("%v\n", setReader)
}

func TestEncodeStruct3(t *testing.T) {
	set := map[int]struct{}{}
	arr := []int{1, 2, 3}
	set[1] = struct{}{}
	set[2] = struct{}{}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(set)
	e.Encode(arr)

	r := bytes.NewBuffer(w.Bytes())
	var arrReader []int
	var setReader map[int]struct{}
	d := labgob.NewDecoder(r)
	if d.Decode(&setReader) != nil {
		panic("decode err")
	}
	if d.Decode(&arrReader) != nil {
		panic("decode err")
	}
	fmt.Printf("%v\n", arrReader)
	fmt.Printf("%v\n", setReader)
}

func TestPersist(t *testing.T) {
	p := raft.MakePersister()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	test := []int{1, 2, 3}
	e.Encode(test)
	test2 := []int{1, 2, 3, 4}
	e.Encode(test2)
	data := w.Bytes()
	p.SaveRaftState(data)
	p.SaveStateAndSnapshot(p.ReadRaftState(), data)

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(p.ReadRaftState())
	d := labgob.NewDecoder(r)
	var read []int

	if d.Decode(&read) != nil {
		fmt.Println("Decode err")
	} else {
		fmt.Println(read)
	}
	if d.Decode(&read) != nil {
		fmt.Println("Decode err")
	} else {
		fmt.Println(read)
	}
	if d.Decode(&read) != nil {
		fmt.Println("Decode err")
	} else {
		fmt.Println(read)
	}

	s := bytes.NewBuffer(p.ReadSnapshot())
	sd := labgob.NewDecoder(s)
	var snapshot1 []int
	var snapshot2 []int
	sd.Decode(&snapshot1)
	fmt.Println(snapshot1)
	sd.Decode(&snapshot2)
	fmt.Println(snapshot2)
}

package raft

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestInitialCrash(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): initial election")

	// is a leader elected?
	cfg.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()

	for i := 0; i < 3; i++ {
		if _, isLeader := cfg.rafts[i].GetState(); isLeader {
			cfg.crash1(i)
		}
	}

	time.Sleep(time.Second)

	cfg.end()
}

func TestPersist(t *testing.T) {
	p := MakePersister()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	test := 0
	e.Encode(test)
	test++
	e.Encode(test)
	data := w.Bytes()
	p.SaveRaftState(data)
	p.SaveStateAndSnapshot(p.ReadRaftState(), data)

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(p.ReadRaftState())
	d := labgob.NewDecoder(r)
	var read int

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
	var snapshot int
	sd.Decode(&snapshot)
	fmt.Println(snapshot)
	sd.Decode(&snapshot)
	fmt.Println(snapshot)
}

func TestAlertBuffer(t *testing.T) {
	p := MakePersister()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode([10]int{})
	p.SaveRaftState(w.Bytes())

	w1 := new(bytes.Buffer)
	e1 := labgob.NewEncoder(w1)
	e1.Encode([10]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	w1Bytes := w1.Bytes()

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	e2.Encode([3]int{})
	w2Bytes := w2.Bytes()

	ws := new(bytes.Buffer)
	es := labgob.NewEncoder(ws)
	es.Encode(make([]int, 10000))
	wsBytes := ws.Bytes()

	slice := make([]int, 10)
	fmt.Println(len(slice))

	array := [10]int{}
	raftState := p.ReadRaftState()
	//for i := 0; i < len(w1Bytes)/2; i++ {
	//	raftState[i] = w1Bytes[i]
	//}
	fmt.Printf("len(w1Bytes)=%d\n", len(w1Bytes))
	fmt.Printf("len(raftState)=%d\n", len(raftState))
	fmt.Printf("len(w2Bytes)=%d\n", len(w2Bytes))
	fmt.Printf("len(wsBytes)=%d\n", len(wsBytes))
	for i := len(w1Bytes) - 5; i < len(w1Bytes); i++ {
		raftState[i] = w1Bytes[i]
	}
	r := bytes.NewBuffer(raftState)
	d := labgob.NewDecoder(r)
	d.Decode(&array)
	fmt.Printf("%v", array)
}

func TestEncodeSlice(t *testing.T) {
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(make([]int, 10))
	//wb := w.Bytes()

	a := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	e2.Encode(a)
	wb := w2.Bytes()

	a = make([]int, 3)

	b := make([]int, 0)
	r := bytes.NewBuffer(wb)
	d := labgob.NewDecoder(r)
	d.Decode(&b)

	fmt.Println(b)
}

func TestCopySlice(t *testing.T) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	wb := w.Bytes()

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	e2.Encode(make([]int, 10))
	r := bytes.NewBuffer(w2.Bytes())
	rb := r.Bytes()
	fmt.Println(len(rb))
	for i := 0; i < 25; i++ {
		rb[i] = wb[i]
	}

	a := make([]int, 0)
	d := labgob.NewDecoder(bytes.NewBuffer(rb))
	d.Decode(&a)
	fmt.Println(a)
}

func TestAppend0(t *testing.T) {
	a := make([]int, 0)
	a = a[:0]
}

func TestSava(t *testing.T) {
	p := MakePersister()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(123)
	p.SaveStateAndSnapshot(nil, w.Bytes())
	p.ReadSnapshot()
	p.ReadSnapshot()
	r := bytes.NewBuffer(p.ReadSnapshot())
	d := labgob.NewDecoder(r)
	var data int
	if d.Decode(&data) != nil {
		fmt.Println("decode err")
	}
	fmt.Println(data)
	if d.Decode(&data) != nil {
		fmt.Println("decode err")
	}
	fmt.Println(data)
}

func TestDecode(t *testing.T) {
	//bs := []byte{3, 4, 0, 38, 12, 255, 143, 2, 1, 2, 255, 144, 0, 1, 16, 0, 0, 254, 1, 53, 255, 144, 0, 20, 0, 3, 105, 110, 116, 4, 10, 0, 248, 225, 77, 109, 15, 23, 184, 188, 228, 3, 105, 110, 116, 4, 10, 0, 248, 175, 244, 26, 183, 4, 85, 29, 114, 3, 105, 110, 116, 4, 10, 0, 248, 226, 237, 225, 39, 242, 242, 200, 228, 3, 105, 110, 116, 4, 10, 0, 248, 174, 86, 64, 67, 134, 170, 13, 248, 3, 105, 110, 116, 4, 10, 0, 248, 61, 190, 53, 188, 199, 164, 19, 122, 3, 105, 110, 116, 4, 10, 0, 248, 153, 63, 126, 36, 154, 251, 31, 20, 3, 105, 110, 116, 4, 10, 0, 248, 37, 220, 227, 114, 187, 186, 125, 58, 3, 105, 110, 116, 4, 10, 0, 248, 87, 167, 191, 197, 156, 23, 49, 74, 3, 105, 110, 116, 4, 10, 0, 248, 22, 57, 190, 2, 232, 17, 102, 140, 3, 105, 110, 116, 4, 10, 0, 248, 55, 129, 33, 234, 246, 43, 149, 120, 3, 105, 110, 116, 4, 10, 0, 248, 178, 223, 233, 55, 255, 62, 221, 94, 3, 105, 110, 116, 4, 10, 0, 248, 77, 70, 183, 207, 81, 237, 28, 30, 3, 105, 110, 116, 4, 10, 0, 248, 230, 102, 53, 245, 211, 241, 32, 224, 3, 105, 110, 116, 4, 10, 0, 248, 110, 129, 165, 201, 51, 41, 53, 172, 3, 105, 110, 116, 4, 10, 0, 248, 54, 40, 207, 136, 105, 133, 209, 14, 3, 105, 110, 116, 4, 10, 0, 248, 247, 137, 174, 23, 229, 52, 98, 60, 3, 105, 110, 116, 4, 10, 0, 248, 218, 238, 224, 33, 103, 220, 225, 56, 3, 105, 110, 116, 4, 10, 0, 248, 105, 140, 216, 237, 86, 100, 188, 32, 3, 105, 110, 116, 4, 10, 0, 248, 184, 190, 142, 216, 184, 122, 144, 52}
	bs := []byte{3, 4, 0, 58, 12, 255, 143, 2, 1, 2, 255, 144, 0, 1, 16, 0, 0, 254, 1, 213, 255, 144, 0, 30, 0, 3, 105, 110, 116, 4, 10, 0, 248, 19, 109, 125, 107, 51, 41, 56, 250, 3, 105, 110, 116, 4, 10, 0, 248, 244, 216, 76, 247, 168, 184, 9, 164, 3, 105, 110, 116, 4, 10, 0, 248, 38, 100, 217, 45, 251, 180, 230, 134, 3, 105, 110, 116, 4, 10, 0, 248, 60, 10, 93, 53, 211, 225, 211, 24, 3, 105, 110, 116, 4, 10, 0, 248, 255, 3, 126, 54, 166, 77, 178, 162, 3, 105, 110, 116, 4, 10, 0, 248, 231, 138, 132, 74, 132, 232, 205, 118, 3, 105, 110, 116, 4, 10, 0, 248, 243, 154, 27, 136, 239, 250, 236, 156, 3, 105, 110, 116, 4, 10, 0, 248, 52, 32, 102, 63, 137, 22, 179, 182, 3, 105, 110, 116, 4, 10, 0, 248, 79, 121, 31, 242, 160, 192, 252, 200, 3, 105, 110, 116, 4, 10, 0, 248, 140, 240, 224, 215, 37, 49, 140, 234, 3, 105, 110, 116, 4, 10, 0, 248, 47, 165, 98, 226, 65, 148, 210, 34, 3, 105, 110, 116, 4, 10, 0, 248, 9, 216, 131, 239, 52, 10, 161, 44, 3, 105, 110, 116, 4, 10, 0, 248, 185, 122, 192, 1, 177, 54, 128, 54, 3, 105, 110, 116, 4, 10, 0, 248, 144, 75, 252, 36, 3, 118, 223, 196, 3, 105, 110, 116, 4, 10, 0, 248, 213, 165, 72, 5, 61, 21, 52, 212, 3, 105, 110, 116, 4, 10, 0, 248, 26, 171, 12, 199, 89, 171, 60, 68, 3, 105, 110, 116, 4, 10, 0, 248, 210, 19, 177, 236, 71, 204, 92, 202, 3, 105, 110, 116, 4, 10, 0, 248, 171, 83, 1, 247, 158, 100, 202, 180, 3, 105, 110, 116, 4, 10, 0, 248, 88, 4, 252, 10, 111, 113, 168, 186, 3, 105, 110, 116, 4, 10, 0, 248, 78, 170, 241, 245, 222, 43, 22, 226, 3, 105, 110, 116, 4, 10, 0, 248, 192, 115, 176, 255, 196, 162, 8, 228, 3, 105, 110, 116, 4, 10, 0, 248, 23, 164, 241, 5, 120, 212, 12, 136, 3, 105, 110, 116, 4, 10, 0, 248, 165, 164, 79, 92, 221, 204, 164, 172, 3, 105, 110, 116, 4, 10, 0, 248, 117, 8, 5, 172, 192, 119, 196, 226, 3, 105, 110, 116, 4, 10, 0, 248, 100, 80, 114, 106, 244, 117, 129, 80, 3, 105, 110, 116, 4, 10, 0, 248, 139, 182, 110, 27, 155, 211, 94, 16, 3, 105, 110, 116, 4, 10, 0, 248, 238, 6, 159, 108, 162, 91, 246, 132, 3, 105, 110, 116, 4, 10, 0, 248, 175, 166, 169, 17, 175, 193, 227, 202, 3, 105, 110, 116, 4, 10, 0, 248, 68, 2, 169, 215, 17, 170, 145, 196}
	r := bytes.NewBuffer(bs)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var xlog []interface{}
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&xlog) != nil {
		fmt.Println("err")
	}
	fmt.Println(lastIncludedIndex)
	fmt.Println(xlog)
}

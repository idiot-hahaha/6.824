package shardkv

//
//import (
//	"6.824/labgob"
//	"bytes"
//	"fmt"
//	"sort"
//	"sync"
//	"testing"
//	"unsafe"
//)
//
//func TestRandString(t *testing.T) {
//	fmt.Println(randstring(20))
//}
//
//func TestMapCopy(t *testing.T) {
//	map1 := map[int]int{
//		1: 1,
//		2: 2,
//	}
//	map2 := map1
//	map2[1] = 2
//	fmt.Printf("%+v", map1)
//}
//
//func TestWaitGroup(t *testing.T) {
//	wg := sync.WaitGroup{}
//	wg.Wait()
//}
//
//func TestEncode(t *testing.T) {
//	m := map[int]int{1: 1}
//	w := new(bytes.Buffer)
//	e := labgob.NewEncoder(w)
//	e.Encode(m)
//	fmt.Println(m)
//	fmt.Println(len(w.Bytes()))
//}
//
//func TestNilMap(t *testing.T) {
//	var m map[int]int
//	fmt.Println(m[0])
//	m[0] = 1
//}
//
//func TestDefer(t *testing.T) {
//	a := 1
//	p := &a
//	defer fmt.Println(a)
//	defer fmt.Println(*p)
//	defer func(x int) { fmt.Println(x) }(a)
//	defer func(x *int) { fmt.Println(*x) }(p)
//	defer func() { fmt.Println(*p) }()
//	a = 2
//	defer fmt.Println(*p)
//}
//
//func TestSortSearch(t *testing.T) {
//	arr := []int{15, 19, 22, 24, 30}
//	i := sort.Search(5, func(i int) bool {
//		fmt.Println(arr[i])
//		return arr[i] > 20
//	})
//	fmt.Println(i)
//}
//func TestArray(t *testing.T) {
//	arr := [10]int{}
//	for _, v := range arr {
//		println(v)
//	}
//}
//func TestSwitch(t *testing.T) {
//	a := 2
//	switch a {
//	case 1:
//		fmt.Println(1)
//	default:
//		fmt.Println(a)
//	}
//}
//
//func TestShardKey(t *testing.T) {
//	fmt.Println(key2shard("0"))
//}
//
//func TestEncodeNilMap(t *testing.T) {
//	emptyDB := map[string]string{}
//	emptyClientCmd := map[int64]int{}
//	w := new(bytes.Buffer)
//	e := labgob.NewEncoder(w)
//	e.Encode(emptyDB)
//	e.Encode(emptyClientCmd)
//	DPrintf("%+v", w.Bytes())
//}
//
//func TestEncodeNilStringMap(t *testing.T) {
//	a := make([]map[string]string, 10)
//	fmt.Println(a[0] == nil)
//}
//
//func TestSizeof(t *testing.T) {
//	fmt.Println(int(unsafe.Sizeof(Op{})))
//	fmt.Println(unsafe.Sizeof(1))
//	fmt.Println(unsafe.Sizeof(int32(1)))
//}

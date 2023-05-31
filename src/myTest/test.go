package myTest

import (
	"fmt"
	"sync"
)

type test struct {
	data int
	mu   sync.Mutex
	cond *sync.Cond
}

func (t *test) broadcast() {
	for {
		t.mu.Lock()
		t.data--
		fmt.Println(t.data)
		t.cond.Broadcast()
		t.mu.Unlock()
	}
}

func (t *test) wait() {
	for {
		t.cond.Wait()
		t.data++
	}
}

func main() {
	t := test{
		data: 0,
	}
	t.cond = sync.NewCond(&t.mu)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		t.broadcast()
		wg.Done()
	}()
	go func() {
		t.wait()
		wg.Done()
	}()
	wg.Wait()
}

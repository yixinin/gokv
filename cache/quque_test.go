package cache

import (
	"fmt"
	"testing"
)

type TestX struct {
	Name string
}

func (x TestX) UniqueKey() string {
	return x.Name
}

func (x TestX) Valid() bool {
	return x.Name != ""
}

func TestQueue(t *testing.T) {
	var q = NewQueue[TestX]()
	q.Push(TestX{"x1"})
	q.Push(TestX{"x1"})
	q.Push(TestX{"x2"})
	q.Push(TestX{"x3"})
	q.Push(TestX{"x4"})
	for !q.Empty() {
		q.Push(TestX{"x1"})
		fmt.Println(q.Pop())
	}
}

package cache

import "sync"

type Key interface {
	UniqueKey() string
}

type Node[T Key] struct {
	Next *Node[T]
	Prev *Node[T]

	Val T
}

type Queue[T Key] struct {
	sync.Mutex
	Head *Node[T]
	Tail *Node[T]
	m    map[string]struct{}
}

func NewQueue[T Key]() *Queue[T] {
	return &Queue[T]{
		m: make(map[string]struct{}, 16),
	}
}

func (q *Queue[T]) Push(items ...T) bool {
	if len(items) == 0 {
		return false
	}
	q.Lock()
	defer q.Unlock()
	for _, item := range items {
		if _, ok := q.m[item.UniqueKey()]; ok {
			continue
		}
		head := q.Head
		q.Head = &Node[T]{
			Val:  item,
			Next: head,
		}
		if head != nil {
			head.Prev = q.Head
		} else {
			q.Tail = q.Head
		}
		q.m[item.UniqueKey()] = struct{}{}
	}
	return true
}

func (q *Queue[T]) Pop() (val T, ok bool) {
	if q.Tail == nil {
		return
	}
	q.Lock()
	defer q.Unlock()
	tail := q.Tail
	delete(q.m, tail.Val.UniqueKey())
	if tail.Prev != nil {
		q.Tail = tail.Prev
		q.Tail.Next = nil
		return tail.Val, true
	}

	q.Tail = nil
	q.Head = nil
	return tail.Val, true
}

func (q *Queue[T]) Empty() bool {
	q.Lock()
	defer q.Unlock()
	return q.Head == nil
}

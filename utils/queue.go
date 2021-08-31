package utils

import "container/list"

type Queue struct {
	l list.List
}

func (q *Queue) Push(obj interface{}) {
	q.l.PushFront(obj)
}

func (q *Queue) Pop() interface{} {
	if v := q.l.Back(); v != nil {
		q.l.Remove(v)
		return v.Value
	}

	return nil
}

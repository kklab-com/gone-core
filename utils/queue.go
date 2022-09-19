package utils

import "container/list"

type Queue struct {
	l list.List
}

func (q *Queue) Push(obj any) {
	q.l.PushFront(obj)
}

func (q *Queue) Pop() any {
	if v := q.l.Back(); v != nil {
		q.l.Remove(v)
		return v.Value
	}

	return nil
}

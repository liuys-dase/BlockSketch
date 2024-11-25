package csctree

import (
	"container/list"
)

// Deque 表示一个双向队列
type Deque struct {
	items *list.List
}

// NewDeque 创建一个新的 Deque
func NewDeque() *Deque {
	return &Deque{items: list.New()}
}

// PushFront 在队列前端添加一个元素
func (d *Deque) PushFront(value interface{}) {
	d.items.PushFront(value)
}

// PushBack 在队列后端添加一个元素
func (d *Deque) PushBack(value interface{}) {
	d.items.PushBack(value)
}

// RemoveFromFront 从队列前端移除并返回一个元素
func (d *Deque) RemoveFromFront() interface{} {
	if d.items.Len() > 0 {
		front := d.items.Front()
		return d.items.Remove(front)
	}
	return nil
}

// RemoveFromBack 从队列后端移除并返回一个元素
func (d *Deque) RemoveFromBack() interface{} {
	if d.items.Len() > 0 {
		back := d.items.Back()
		return d.items.Remove(back)
	}
	return nil
}

// Front 返回队列前端的元素但不移除
func (d *Deque) Front() interface{} {
	if d.items.Len() > 0 {
		return d.items.Front().Value
	}
	return nil
}

// Back 返回队列后端的元素但不移除
func (d *Deque) Back() interface{} {
	if d.items.Len() > 0 {
		return d.items.Back().Value
	}
	return nil
}

// KBack 返回队列后段的第 k 个元素但不移除
func (d *Deque) KBack(k int) interface{} {
	if k <= 0 || d.items.Len() < k {
		return nil
	} else {
		e := d.items.Back()
		for i := 0; i < k-1; i++ {
			e = e.Prev()
		}
		return e.Value
	}
}

// Size() 返回队列的长度
func (d *Deque) Size() int {
	return d.items.Len()
}

// Iterator 表示一个迭代器
type Iterator struct {
	current *list.Element
}

// NewIterator 创建一个新的迭代器
func (d *Deque) NewIterator() *Iterator {
	return &Iterator{current: d.items.Front()}
}

// HasNext 检查是否还有下一个元素
func (it *Iterator) HasNext() bool {
	return it.current != nil
}

// Next 返回下一个元素
func (it *Iterator) Next() interface{} {
	if it.current != nil {
		value := it.current.Value
		it.current = it.current.Next()
		return value
	}
	return nil
}
